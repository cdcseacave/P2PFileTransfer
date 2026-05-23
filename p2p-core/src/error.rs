//! Error types for P2P file transfer

use thiserror::Error;

/// Result type alias
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for P2P transfers
#[derive(Debug, Error)]
pub enum Error {
    /// Network I/O error
    #[error("Network error: {0}")]
    Network(#[from] std::io::Error),

    /// Protocol-level error
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Version mismatch during handshake
    #[error("Protocol version mismatch: peer version {peer}, our version {ours}")]
    VersionMismatch { peer: u8, ours: u8 },

    /// Compression error
    #[error("Compression error: {0}")]
    Compression(String),

    /// Decompression error
    #[error("Decompression error: {0}")]
    Decompression(String),

    /// Checksum verification failed
    #[error("Verification failed: {0}")]
    Verification(String),

    /// File system error
    #[error("File system error: {0}")]
    FileSystem(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),

    /// Peer disconnected
    #[error("Peer disconnected")]
    Disconnected,

    /// Connection timeout
    #[error("Connection timeout")]
    Timeout,

    /// Transfer cancelled by user
    #[error("Transfer cancelled by user")]
    Cancelled,

    /// Transfer not found (for resume)
    #[error("Transfer not found: {0}")]
    TransferNotFound(String),

    /// Invalid chunk
    #[error("Invalid chunk: {0}")]
    InvalidChunk(String),

    /// Capability not supported
    #[error("Capability not supported: {0}")]
    UnsupportedCapability(String),

    /// QUIC transport error (connection, stream, congestion control, ...)
    #[error("QUIC error: {0}")]
    Quic(String),

    /// TLS / identity / certificate error
    #[error("TLS error: {0}")]
    Tls(String),

    /// Rendezvous server protocol error
    #[error("Rendezvous error: {0}")]
    Rendezvous(String),

    /// UDP hole punching failed (e.g. peer behind symmetric NAT, relay required)
    #[error("Hole punch failed: {0}")]
    HolePunchFailed(String),

    /// Peer certificate fingerprint did not match the pinned value
    #[error("Peer fingerprint mismatch")]
    FingerprintMismatch,

    /// Generic error
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Check if this error is recoverable (transient — caller should reconnect)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Error::Network(_)
                | Error::Timeout
                | Error::Disconnected
                | Error::Quic(_)
                | Error::HolePunchFailed(_)
        )
    }

    /// Check if this error should trigger a retry of the same operation
    pub fn should_retry(&self) -> bool {
        matches!(
            self,
            Error::Network(_) | Error::Timeout | Error::InvalidChunk(_) | Error::Quic(_)
        )
    }
}
