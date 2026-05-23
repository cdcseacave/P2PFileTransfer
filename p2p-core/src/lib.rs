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

/// Normalize a user-supplied `host[:port]` string to one that always carries
/// a port, suitable for `tokio::net::lookup_host`. Handles IPv4 / IPv6 /
/// hostname forms, including bracketed and bare IPv6 literals — `contains(':')`
/// alone is not enough to tell whether an IPv6 string already has a port.
pub fn with_default_port(host_port: &str, default_port: u16) -> String {
    use std::net::{IpAddr, SocketAddr};
    if host_port.parse::<SocketAddr>().is_ok() {
        return host_port.to_string();
    }
    if let Ok(ip) = host_port.parse::<IpAddr>() {
        return SocketAddr::new(ip, default_port).to_string();
    }
    // Bare bracketed IPv6 without port, e.g. "[2001:db8::1]".
    if let Some(inner) = host_port
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
    {
        if let Ok(ip) = inner.parse::<IpAddr>() {
            return SocketAddr::new(ip, default_port).to_string();
        }
    }
    // Hostname form: only treat `host:port` as already-ported when the host
    // part has no remaining colons (rules out unbracketed IPv6 literals).
    if let Some((host, port)) = host_port.rsplit_once(':') {
        if !host.is_empty() && !host.contains(':') && port.parse::<u16>().is_ok() {
            return host_port.to_string();
        }
    }
    format!("{host_port}:{default_port}")
}

#[cfg(test)]
mod with_default_port_tests {
    use super::with_default_port;

    #[test]
    fn ipv4_with_port_kept() {
        assert_eq!(with_default_port("1.2.3.4:80", 14570), "1.2.3.4:80");
    }

    #[test]
    fn ipv4_without_port_gets_default() {
        assert_eq!(with_default_port("1.2.3.4", 14570), "1.2.3.4:14570");
    }

    #[test]
    fn hostname_without_port_gets_default() {
        assert_eq!(with_default_port("example.com", 14570), "example.com:14570");
    }

    #[test]
    fn hostname_with_port_kept() {
        assert_eq!(
            with_default_port("example.com:9999", 14570),
            "example.com:9999"
        );
    }

    #[test]
    fn ipv6_bracketed_with_port_kept() {
        assert_eq!(
            with_default_port("[2001:db8::1]:9999", 14570),
            "[2001:db8::1]:9999"
        );
    }

    #[test]
    fn ipv6_bracketed_without_port_gets_default() {
        assert_eq!(
            with_default_port("[2001:db8::1]", 14570),
            "[2001:db8::1]:14570"
        );
    }

    #[test]
    fn ipv6_bare_gets_default() {
        assert_eq!(
            with_default_port("2001:db8::1", 14570),
            "[2001:db8::1]:14570"
        );
    }

    #[test]
    fn ipv6_loopback_bare_gets_default() {
        assert_eq!(with_default_port("::1", 14570), "[::1]:14570");
    }
}
