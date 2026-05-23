//! NAT traversal diagnostic.
//!
//! Runs the same STUN query the real traversal flow uses, on a real
//! `tokio::net::UdpSocket` (the same socket type quinn owns), and reports
//! the discovered public endpoint plus a coarse NAT classification by
//! cross-checking the mapped port against a second STUN server.

use anyhow::{anyhow, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::{lookup_host, UdpSocket};
use tracing::info;

use p2p_core::traversal::stun::{classify_nat, query, NatClass};

/// Default STUN servers used when the user does not pass `--stun-server`.
/// Two servers are required for symmetric/cone classification.
const DEFAULT_STUN_SERVERS: &[&str] = &[
    "stun.l.google.com:19302",
    "stun1.l.google.com:19302",
];

pub async fn handle_nat_test(stun_server: Option<String>) -> Result<()> {
    info!("Testing NAT traversal...");

    let servers = match stun_server.as_deref() {
        Some(custom) => {
            info!("  Custom STUN server: {custom}");
            vec![custom.to_string(), DEFAULT_STUN_SERVERS[1].to_string()]
        }
        None => {
            info!("  STUN servers: {} + {}", DEFAULT_STUN_SERVERS[0], DEFAULT_STUN_SERVERS[1]);
            DEFAULT_STUN_SERVERS.iter().map(|s| s.to_string()).collect()
        }
    };

    let a = resolve_first(&servers[0]).await?;
    let b = resolve_first(&servers[1]).await?;

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let socket = UdpSocket::bind(bind).await?;
    info!("  Local socket bound to {}", socket.local_addr()?);

    let public = query(&socket, a).await?;
    info!("  Public endpoint (server A): {public}");

    let classification = classify_nat(&socket, a, b).await?;
    match classification {
        NatClass::Cone { public } => {
            info!("Cone NAT detected — UDP hole punching should work.");
            info!("  Public endpoint: {public}");
            Ok(())
        }
        NatClass::Symmetric => {
            info!("Symmetric NAT detected — direct UDP hole punching will fail.");
            info!("  Peers behind symmetric NAT need the QUIC relay fallback.");
            Ok(())
        }
    }
}

async fn resolve_first(host_port: &str) -> Result<SocketAddr> {
    lookup_host(host_port)
        .await?
        .next()
        .ok_or_else(|| anyhow!("could not resolve STUN server: {host_port}"))
}
