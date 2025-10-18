//! P2P Core Library
//!
//! This crate provides the core functionality for peer-to-peer file transfers
//! with compression and resume capabilities.

pub mod bandwidth; // Bandwidth throttling
pub mod compression;
pub mod config;
pub mod discovery;
pub mod error;
pub mod handshake;
pub mod history; // Transfer history tracking
pub mod nat; // NAT traversal and hole punching
pub mod network;
pub mod progress; // Unified progress tracking
pub mod protocol;
pub mod reconnect; // Auto-reconnect with exponential backoff
pub mod session; // High-level session management
pub mod state;
pub mod transfer;
pub mod transfer_file; // Single-file transfer
pub mod transfer_folder; // Folder transfer orchestration
pub mod verification;
pub mod window; // Sliding window protocol

pub use error::{Error, Result};
pub use protocol::Message;

// Re-export commonly used types
pub use uuid::Uuid;

/// Protocol version
pub const PROTOCOL_VERSION: u8 = 1;

/// Minimum supported protocol version
pub const MIN_PROTOCOL_VERSION: u8 = 1;

/// Default chunk size (64 KB)
pub const DEFAULT_CHUNK_SIZE: u32 = 65536;

/// Default discovery port
pub const DEFAULT_DISCOVERY_PORT: u16 = 14566;

/// Default transfer port
pub const DEFAULT_TRANSFER_PORT: u16 = 14567;

/// Magic bytes for protocol framing
pub const PROTOCOL_MAGIC: [u8; 4] = *b"P2PF";
