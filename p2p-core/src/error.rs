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

    /// Generic error
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Check if this error is recoverable
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            Error::Network(_) | Error::Timeout | Error::Disconnected
        )
    }

    /// Check if this error should trigger a retry
    pub fn should_retry(&self) -> bool {
        matches!(
            self,
            Error::Network(_) | Error::Timeout | Error::InvalidChunk(_)
        )
    }
}
