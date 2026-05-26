//! Protocol message definitions.
//!
//! The QUIC rewrite removes everything that QUIC + TLS 1.3 already provides:
//! per-chunk CRC (TLS AEAD authenticates every byte), per-chunk ACKs and
//! retransmission (QUIC streams are reliable), and the windowed mode flag
//! (QUIC's stream multiplexing replaces the sliding window). Chunk data
//! travels on one unidirectional QUIC stream per chunk with the wire format
//!
//! ```text
//! [chunk_index : u64 little-endian | flags : u8 | payload bytes]
//! ```
//!
//! `flags` is a per-chunk bitfield (`transfer_file::FLAG_COMPRESSED = 0x01`
//! is the only bit defined today). The adaptive compressor decides per chunk
//! whether to compress, so even when `config.compression_enabled` is `true`
//! some chunks ride uncompressed (with `flags = 0`). When negotiation
//! disabled compression the sender never sets the bit. Chunk data never
//! goes through this control-plane [`Message`] enum.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Custom serialization for a fixed-size byte array as a hex string.
/// Used for SHA-256 file checksums and cert fingerprints so the wire form
/// is human-readable in `tcpdump`/logs.
mod checksum_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex_string = bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        serializer.serialize_str(&hex_string)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.len() != 64 {
            return Err(serde::de::Error::custom(format!(
                "Expected 64 hex characters, got {}",
                s.len()
            )));
        }
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|e| serde::de::Error::custom(format!("Invalid hex: {}", e)))?;
        }
        Ok(bytes)
    }
}

/// Top-level control-plane message enum. Travels over the bidirectional
/// QUIC control stream opened at connection setup. Chunk *data* is sent on
/// per-chunk unidirectional streams and is NOT a variant here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // Discovery
    DiscoveryBeacon(DiscoveryBeacon),

    // Handshake
    Hello(HelloMessage),
    HelloAck(HelloMessage),
    Config(ConfigMessage),
    ConfigAck,
    // Boxed: TransferInfo is the largest variant by far (file list + resume
    // bitmap) and we don't want every `Message` value on the recv path
    // bloated to the size of the manifest.
    TransferInfo(Box<TransferInfo>),
    Ready,
    Resume(ResumeRequest),

    // Control
    Pause,
    Cancel,
    Complete(CompleteMessage),
    FileChecksum(FileChecksumMessage),
    Error(ErrorMessage),

    // Keepalive (application-level, in addition to QUIC's own keepalive)
    Ping,
    Pong,
}

/// Discovery beacon broadcast message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryBeacon {
    /// Protocol version
    pub version: u8,
    /// Unique device identifier
    pub device_id: Uuid,
    /// Human-readable device name
    pub device_name: String,
    /// QUIC/UDP listening port for transfers
    pub port: u16,
    /// SHA-256 of the device's self-signed certificate. Required: discovered
    /// peers pin this fingerprint when initiating their first QUIC connection.
    #[serde(with = "checksum_hex")]
    pub cert_fingerprint: [u8; 32],
}

/// Handshake hello message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloMessage {
    /// Protocol version
    pub protocol_version: u8,
    /// Minimum supported version
    pub min_version: u8,
    /// Device identifier
    pub device_id: Uuid,
    /// SHA-256 of the sender's self-signed certificate. Cross-checked
    /// against the cert actually presented in the QUIC/TLS handshake.
    #[serde(with = "checksum_hex")]
    pub cert_fingerprint: [u8; 32],
}

/// Transfer configuration message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigMessage {
    /// Enable compression
    pub compression_enabled: bool,
    /// Zstd compression level (-7 to 22)
    pub compression_level: i32,
    /// Use adaptive compression (auto-disable if data is incompressible)
    pub adaptive_compression: bool,
    /// Chunk size in bytes
    pub chunk_size: u32,
    /// Bandwidth limit in bytes per second (0 = unlimited)
    pub bandwidth_limit: u64,
}

impl Default for ConfigMessage {
    fn default() -> Self {
        Self {
            compression_enabled: true,
            compression_level: 3,
            adaptive_compression: true,
            chunk_size: crate::DEFAULT_CHUNK_SIZE,
            bandwidth_limit: 0, // unlimited
        }
    }
}

/// Transfer information and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferInfo {
    /// Unique transfer identifier
    pub transfer_id: Uuid,
    /// List of files to transfer
    pub items: Vec<FileMetadata>,
    /// Resume point if applicable (covers the single in-progress file).
    pub resume_from: Option<ResumePoint>,
    /// File indices the sender already finished in a prior session and
    /// will skip entirely (no streams, no `FileChecksum`). The receiver
    /// must skip these or it will block in `accept_uni()` forever waiting
    /// for streams the sender never opens.
    #[serde(default)]
    pub completed_files: Vec<u32>,
}

/// File metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// Relative path
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified timestamp (Unix)
    pub modified: u64,
    /// SHA-256 checksum of entire file (zero-filled when computed during transfer)
    #[serde(with = "checksum_hex")]
    #[serde(default = "default_checksum")]
    pub checksum: [u8; 32],
}

fn default_checksum() -> [u8; 32] {
    [0u8; 32]
}

/// Resume point information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumePoint {
    /// Transfer ID to resume
    pub transfer_id: Uuid,
    /// File index within transfer
    pub file_index: u32,
    /// Indices of already-received chunks
    pub completed_chunks: Vec<u64>,
}

/// Resume request message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeRequest {
    /// Transfer ID to resume
    pub transfer_id: Uuid,
    /// Last successfully received chunk per file
    pub progress: Vec<FileProgress>,
}

/// Progress of a single file (for resume).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileProgress {
    /// File index
    pub file_index: u32,
    /// Total chunks in file
    pub total_chunks: u64,
    /// Indices of already-received chunks
    pub completed_chunks: Vec<u64>,
}

/// Transfer completion message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteMessage {
    /// Transfer identifier
    pub transfer_id: Uuid,
    /// Total bytes transferred
    pub total_bytes: u64,
    /// Transfer duration in milliseconds
    pub duration_ms: u64,
}

/// File checksum message (bidirectional — both sides compute and exchange).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChecksumMessage {
    /// Transfer identifier
    pub transfer_id: Uuid,
    /// File index
    pub file_index: u32,
    /// SHA-256 checksum of the complete file
    #[serde(with = "checksum_hex")]
    pub checksum: [u8; 32],
}

/// Error message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    /// Error code
    pub code: ErrorCode,
    /// Human-readable message
    pub message: String,
}

/// Error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    ProtocolError,
    VersionMismatch,
    UnsupportedCapability,
    FileSystemError,
    TransferCancelled,
    Other,
}
