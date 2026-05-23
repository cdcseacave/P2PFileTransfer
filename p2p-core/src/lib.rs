//! P2P Core Library
//!
//! This crate provides the core functionality for peer-to-peer file transfers
//! with compression and resume capabilities.

pub mod bandwidth;
pub mod compression;
pub mod config;
pub mod discovery;
pub mod error;
pub mod handshake;
pub mod history;
pub mod identity; // Ed25519 device identity + self-signed cert
pub mod known_peers; // TOFU fingerprint trust store
pub mod network;
pub mod progress;
pub mod protocol;
pub mod reconnect;
pub mod session;
pub mod state;
pub mod tls; // rustls config + fingerprint-pinning verifier
pub mod transfer;
pub mod transfer_file;
pub mod transfer_folder;
pub mod traversal; // STUN + hole punch + rendezvous orchestration
pub mod verification;

pub use error::{Error, Result};
pub use protocol::Message;

// Re-export commonly used types
pub use uuid::Uuid;

/// Protocol version. Bumped to 2 for the QUIC + TLS 1.3 rewrite.
pub const PROTOCOL_VERSION: u8 = 2;

/// Minimum supported protocol version. Equal to PROTOCOL_VERSION — no v1 compat.
pub const MIN_PROTOCOL_VERSION: u8 = 2;

/// Default chunk size (64 KB)
pub const DEFAULT_CHUNK_SIZE: u32 = 65536;

/// Default discovery port (UDP LAN beacons)
pub const DEFAULT_DISCOVERY_PORT: u16 = 14566;

/// Default transfer port (QUIC/UDP)
pub const DEFAULT_TRANSFER_PORT: u16 = 14567;

/// Default rendezvous server port (TCP control channel)
pub const DEFAULT_RENDEZVOUS_PORT: u16 = 14570;

/// Magic bytes for protocol framing
pub const PROTOCOL_MAGIC: [u8; 4] = *b"P2PF";

/// ALPN protocol name negotiated over QUIC's TLS 1.3 handshake.
pub const ALPN_PROTOCOL: &[u8] = b"p2pf/2";
