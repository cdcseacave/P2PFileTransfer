//! Rendezvous wire protocol.
//!
//! Both peers connect to the same `rendezvousd` instance with a shared
//! short code. The first arrival waits; the second arrival triggers the
//! server to deliver a [`Message::Match`] containing the peer's public
//! endpoint, cert fingerprint, and device id to both sides, then close
//! the connection. Code expires after a server-chosen lifetime (default
//! 5 minutes) if unmatched.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// SHA-256 cert fingerprint as raw bytes (same encoding the rest of the
/// system uses; see `p2p_core::identity::Fingerprint`).
pub type Fingerprint = [u8; 32];

/// 128-bit device identifier (raw bytes form of `uuid::Uuid`).
pub type DeviceId = [u8; 16];

/// Wire protocol message. Travels as length-prefixed MessagePack frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Client → server. Asks to be paired with whoever else uses the same
    /// `code`. If no second peer arrives before the server's TTL the
    /// server replies with [`Message::Expired`] and closes.
    Register(RegisterRequest),

    /// Server → client. The other peer has arrived; here's how to reach it.
    Match {
        peer_endpoint: SocketAddr,
        peer_fingerprint: Fingerprint,
        peer_device_id: DeviceId,
    },

    /// Server → client. The other peer arrived but at least one side
    /// asked for relay mode (or detected symmetric NAT). Clients should
    /// connect their QUIC endpoint to `relay_endpoint` and prefix the
    /// first UDP datagram with a [`crate::relay::RelayHello`]
    /// carrying `relay_session_token` and their own cert fingerprint.
    RelayMatch {
        relay_endpoint: SocketAddr,
        relay_session_token: [u8; 16],
        peer_fingerprint: Fingerprint,
        peer_device_id: DeviceId,
    },

    /// Server → client. The code was used twice before this client had a
    /// chance to be matched, or the TTL fired. Clients should surface
    /// this as a user-visible "ask the peer for a fresh code" error.
    Expired,

    /// Server → client. The client's request was malformed
    /// (wrong protocol version, bad code, etc.).
    Rejected { reason: String },
}

/// Client-supplied registration. The server stores this until a second
/// `Register` with the same `code` arrives, then echoes the inverse to
/// both peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// Rendezvous protocol version. Equality-checked; bump together
    /// across server + client when the wire format changes.
    pub protocol_version: u8,
    /// Short shared code (Crockford-base32-ish, 6 chars by default).
    pub code: String,
    /// Public UDP endpoint as discovered via STUN on the same socket
    /// `quinn` will subsequently own.
    pub public_endpoint: SocketAddr,
    /// SHA-256 of this peer's self-signed TLS cert.
    pub cert_fingerprint: Fingerprint,
    /// Local device id (uuid bytes).
    pub device_id: DeviceId,
    /// Set when this peer detected symmetric NAT (or the user forced
    /// relay mode). If either peer of a pair sets this and the server
    /// has a relay configured, the response is a [`Message::RelayMatch`]
    /// instead of a direct [`Message::Match`]. Defaults to `false` for
    /// backward compatibility with the rendezvous v1 wire format.
    #[serde(default)]
    pub want_relay: bool,
}

/// Rendezvous protocol version. Bumped together on the server + client
/// any time the wire format changes; the server rejects mismatches with
/// [`Message::Rejected`].
pub const PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Error)]
pub enum RendezvousProtoError {
    #[error("rendezvous io: {0}")]
    Io(#[from] std::io::Error),
    #[error("rendezvous decode: {0}")]
    Decode(rmp_serde::decode::Error),
    #[error("rendezvous encode: {0}")]
    Encode(rmp_serde::encode::Error),
    #[error("rendezvous frame too large: {size} > {cap}")]
    FrameTooLarge { size: u32, cap: u32 },
}
