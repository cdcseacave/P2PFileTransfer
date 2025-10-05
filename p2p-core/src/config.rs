//! Configuration management

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Application configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    pub network: NetworkConfig,
    pub transfer: TransferConfig,
    pub verification: VerificationConfig,
    pub ui: UiConfig,
    pub advanced: AdvancedConfig,
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// TCP listening port
    pub listen_port: u16,
    /// UDP discovery port
    pub discovery_port: u16,
    /// Discovery beacon interval (milliseconds)
    pub discovery_interval_ms: u64,
    /// Keepalive ping interval (milliseconds)
    pub keepalive_interval_ms: u64,
    /// Maximum reconnection attempts
    pub max_reconnect_attempts: u32,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_port: 7778,
            discovery_port: 7777,
            discovery_interval_ms: 2000,
            keepalive_interval_ms: 5000,
            max_reconnect_attempts: 10,
        }
    }
}

/// Transfer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferConfig {
    /// Chunk size in kilobytes
    pub chunk_size_kb: u32,
    /// Enable compression by default
    pub compression_enabled: bool,
    /// Zstd compression level (-7 to 22)
    pub compression_level: i32,
    /// Maximum chunks in flight (sliding window)
    pub max_chunks_in_flight: usize,
    /// Chunk acknowledgment timeout (milliseconds)
    pub chunk_timeout_ms: u64,
    /// Maximum chunk retry attempts
    pub max_chunk_retries: u32,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            chunk_size_kb: 64,
            compression_enabled: true,
            compression_level: 3,
            max_chunks_in_flight: 16,
            chunk_timeout_ms: 5000,
            max_chunk_retries: 3,
        }
    }
}

/// Verification configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationConfig {
    /// Use SHA256 for file verification
    pub use_sha256: bool,
    /// Verify checksums on transfer completion
    pub verify_on_complete: bool,
}

impl Default for VerificationConfig {
    fn default() -> Self {
        Self {
            use_sha256: true,
            verify_on_complete: true,
        }
    }
}

/// UI configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UiConfig {
    /// UI theme
    pub theme: String,
    /// Auto-accept incoming transfers
    pub auto_accept_transfers: bool,
    /// Default download path
    pub default_download_path: PathBuf,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            theme: "dark".to_string(),
            auto_accept_transfers: false,
            default_download_path: dirs::download_dir()
                .unwrap_or_else(|| PathBuf::from(".")),
        }
    }
}

/// Advanced configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedConfig {
    /// Enable TCP_NODELAY
    pub tcp_nodelay: bool,
    /// TCP buffer size in bytes
    pub tcp_buffer_size: usize,
}

impl Default for AdvancedConfig {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            tcp_buffer_size: 262144, // 256 KB
        }
    }
}

// Helper for dirs crate
mod dirs {
    use std::path::PathBuf;

    pub fn download_dir() -> Option<PathBuf> {
        #[cfg(target_os = "windows")]
        {
            Some(PathBuf::from(
                std::env::var("USERPROFILE").ok()? + "\\Downloads",
            ))
        }
        #[cfg(target_os = "macos")]
        {
            Some(PathBuf::from(
                std::env::var("HOME").ok()? + "/Downloads",
            ))
        }
        #[cfg(target_os = "linux")]
        {
            Some(PathBuf::from(
                std::env::var("HOME").ok()? + "/Downloads",
            ))
        }
        #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
        {
            None
        }
    }
}
