//! Protocol message definitions

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Custom serialization for checksum as hex string
mod checksum_hex {
    use serde::{Deserializer, Serializer, Deserialize};

    pub fn serialize<S>(bytes: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex_string = bytes.iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        serializer.serialize_str(&hex_string)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        
        // Handle both hex string and array formats for backward compatibility
        if s.starts_with('[') {
            // Old format: JSON array - skip it
            return Err(serde::de::Error::custom("Array format is deprecated, please use hex string"));
        }
        
        if s.len() != 64 {
            return Err(serde::de::Error::custom(format!("Expected 64 hex characters, got {}", s.len())));
        }
        
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&s[i*2..i*2+2], 16)
                .map_err(|e| serde::de::Error::custom(format!("Invalid hex: {}", e)))?;
        }
        
        Ok(bytes)
    }
}

/// Top-level protocol message enum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // Discovery
    DiscoveryBeacon(DiscoveryBeacon),

    // Handshake
    Hello(HelloMessage),
    HelloAck(HelloMessage),
    Config(ConfigMessage),
    ConfigAck,
    TransferInfo(TransferInfo),
    Ready,
    Resume(ResumeRequest),

    // Transfer
    Chunk(ChunkMessage),
    ChunkAck(ChunkAck),

    // Control
    Pause,
    Cancel,
    Complete(CompleteMessage),
    Error(ErrorMessage),

    // Keepalive
    Ping,
    Pong,
}

/// Discovery beacon broadcast message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryBeacon {
    /// Protocol version
    pub version: u8,
    /// Unique device identifier
    pub device_id: Uuid,
    /// Human-readable device name
    pub device_name: String,
    /// TCP listening port for transfers
    pub port: u16,
    /// Supported capabilities
    pub capabilities: Capabilities,
}

/// Handshake hello message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloMessage {
    /// Protocol version
    pub protocol_version: u8,
    /// Minimum supported version
    pub min_version: u8,
    /// Device identifier
    pub device_id: Uuid,
    /// Supported capabilities
    pub capabilities: Capabilities,
}

/// Transfer configuration message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigMessage {
    /// Enable compression
    pub compression_enabled: bool,
    /// Zstd compression level (-7 to 22)
    pub compression_level: i32,
    /// Chunk size in bytes
    pub chunk_size: u32,
    /// Window size (1 = sequential, >1 = windowed/parallel chunks)
    pub window_size: usize,
}

/// Transfer information and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferInfo {
    /// Unique transfer identifier
    pub transfer_id: Uuid,
    /// List of files to transfer
    pub items: Vec<FileMetadata>,
    /// Resume point if applicable
    pub resume_from: Option<ResumePoint>,
}

/// File metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// Relative path
    pub path: String,
    /// File size in bytes
    pub size: u64,
    /// Last modified timestamp (Unix)
    pub modified: u64,
    /// SHA256 checksum of entire file
    #[serde(with = "checksum_hex")]
    pub checksum: [u8; 32],
}

/// Resume point information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumePoint {
    /// Transfer ID to resume
    pub transfer_id: Uuid,
    /// File index within transfer
    pub file_index: u32,
    /// Chunk index within file
    pub chunk_index: u64,
}

/// Resume request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeRequest {
    /// Transfer ID to resume
    pub transfer_id: Uuid,
    /// Last successfully received chunk per file
    pub progress: Vec<FileProgress>,
}

/// Progress of a single file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileProgress {
    /// File index
    pub file_index: u32,
    /// Total chunks in file
    pub total_chunks: u64,
    /// Bitmap of completed chunks (compressed)
    pub completed_chunks: Vec<u8>,
}

/// Data chunk message
#[derive(Clone, Serialize, Deserialize)]
pub struct ChunkMessage {
    /// Transfer identifier
    pub transfer_id: Uuid,
    /// File index within transfer
    pub file_index: u32,
    /// Chunk index within file
    pub chunk_index: u64,
    /// Total chunks in this file
    pub total_chunks: u64,
    /// Compressed data size
    pub compressed_size: u32,
    /// Uncompressed data size
    pub uncompressed_size: u32,
    /// CRC32 checksum of compressed data
    pub checksum: u32,
    /// Compressed chunk data
    pub data: Vec<u8>,
}

// Custom Debug implementation to avoid printing large data payloads
impl std::fmt::Debug for ChunkMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const MAX_DATA_DISPLAY: usize = 128;
        
        let data_display = if self.data.len() > MAX_DATA_DISPLAY {
            format!("[{} bytes: {:02x?}...]", self.data.len(), &self.data[..MAX_DATA_DISPLAY])
        } else {
            format!("[{} bytes: {:02x?}]", self.data.len(), &self.data)
        };
        
        f.debug_struct("ChunkMessage")
            .field("transfer_id", &self.transfer_id)
            .field("file_index", &self.file_index)
            .field("chunk_index", &self.chunk_index)
            .field("total_chunks", &self.total_chunks)
            .field("compressed_size", &self.compressed_size)
            .field("uncompressed_size", &self.uncompressed_size)
            .field("checksum", &self.checksum)
            .field("data", &data_display)
            .finish()
    }
}

/// Chunk acknowledgment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkAck {
    /// Transfer identifier
    pub transfer_id: Uuid,
    /// File index
    pub file_index: u32,
    /// Chunk index
    pub chunk_index: u64,
    /// Acknowledgment status
    pub status: AckStatus,
}

/// Transfer completion message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteMessage {
    /// Transfer identifier
    pub transfer_id: Uuid,
    /// Total bytes transferred
    pub total_bytes: u64,
    /// Transfer duration in milliseconds
    pub duration_ms: u64,
}

/// Error message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    /// Error code
    pub code: ErrorCode,
    /// Human-readable message
    pub message: String,
}

/// Acknowledgment status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckStatus {
    /// Chunk received successfully
    Success,
    /// Checksum verification failed
    ChecksumFailed,
    /// Decompression failed
    DecompressionFailed,
    /// Write to disk failed
    WriteFailed,
}

/// Device capabilities
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    bits: u32,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self::new()
    }
}

impl Capabilities {
    pub const COMPRESSION: u32 = 0b0000_0001;
    pub const RESUME: u32 = 0b0000_0010;
    pub const BATCH_TRANSFER: u32 = 0b0000_0100;
    pub const FOLDER_TRANSFER: u32 = 0b0000_1000;

    pub const fn new() -> Self {
        Self { bits: 0 }
    }

    pub const fn all() -> Self {
        Self {
            bits: Self::COMPRESSION
                | Self::RESUME
                | Self::BATCH_TRANSFER
                | Self::FOLDER_TRANSFER,
        }
    }

    pub const fn with_compression(mut self) -> Self {
        self.bits |= Self::COMPRESSION;
        self
    }

    pub const fn with_resume(mut self) -> Self {
        self.bits |= Self::RESUME;
        self
    }

    pub const fn with_batch_transfer(mut self) -> Self {
        self.bits |= Self::BATCH_TRANSFER;
        self
    }

    pub const fn with_folder_transfer(mut self) -> Self {
        self.bits |= Self::FOLDER_TRANSFER;
        self
    }

    pub const fn has_compression(&self) -> bool {
        (self.bits & Self::COMPRESSION) != 0
    }

    pub const fn has_resume(&self) -> bool {
        (self.bits & Self::RESUME) != 0
    }

    pub const fn has_batch_transfer(&self) -> bool {
        (self.bits & Self::BATCH_TRANSFER) != 0
    }

    pub const fn has_folder_transfer(&self) -> bool {
        (self.bits & Self::FOLDER_TRANSFER) != 0
    }

    pub const fn intersect(&self, other: &Self) -> Self {
        Self {
            bits: self.bits & other.bits,
        }
    }
}

/// Error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    ProtocolError,
    VersionMismatch,
    UnsupportedCapability,
    FileSystemError,
    TransferCancelled,
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities() {
        let caps = Capabilities::new()
            .with_compression()
            .with_resume();

        assert!(caps.has_compression());
        assert!(caps.has_resume());
        assert!(!caps.has_batch_transfer());

        let all = Capabilities::all();
        assert!(all.has_compression());
        assert!(all.has_resume());
        assert!(all.has_batch_transfer());
        assert!(all.has_folder_transfer());
    }

    #[test]
    fn test_capabilities_intersect() {
        let caps1 = Capabilities::new()
            .with_compression()
            .with_resume();
        let caps2 = Capabilities::new()
            .with_resume()
            .with_batch_transfer();

        let common = caps1.intersect(&caps2);
        assert!(!common.has_compression());
        assert!(common.has_resume());
        assert!(!common.has_batch_transfer());
    }
}
