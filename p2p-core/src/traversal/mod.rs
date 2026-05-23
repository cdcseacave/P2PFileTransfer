//! NAT traversal orchestrator (Phase 1).
//!
//! Owns the UDP socket lifecycle: bind → STUN probe (on the same socket
//! `quinn` will then own) → exchange endpoints + cert fingerprints via
//! the `p2p-rendezvous` server → race
//! [`QuicEndpoint::connect`] against [`QuicEndpoint::accept`] as the
//! hole-punch → hand back the established [`QuicConnection`].

pub mod punch;
pub mod stun;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use tokio::net::{lookup_host, UdpSocket};
use tracing::{debug, info};
use uuid::Uuid;

use p2p_rendezvous::client::register as rendezvous_register;
use p2p_rendezvous::protocol::{RegisterRequest, PROTOCOL_VERSION as RENDEZVOUS_PROTO_VERSION};

use crate::error::{Error, Result};
use crate::identity::Identity;
use crate::network::quic::{QuicConnection, QuicEndpoint};

use self::stun::{classify_nat, NatClass};

/// Default pair of STUN servers used when the caller does not supply
/// their own. Two are needed so [`stun::classify_nat`] can spot
/// symmetric-NAT mappings (different mapped port per destination).
pub const DEFAULT_STUN_SERVERS: [&str; 2] = [
    "stun.l.google.com:19302",
    "stun1.l.google.com:19302",
];

/// Result of a rendezvous-mediated session establishment.
pub struct EstablishedSession {
    pub endpoint: QuicEndpoint,
    pub connection: QuicConnection,
    pub peer_endpoint: SocketAddr,
    pub peer_fingerprint: crate::identity::Fingerprint,
    pub peer_device_id: Uuid,
}

/// Pairing parameters for [`establish_via_rendezvous`].
pub struct RendezvousParams {
    /// Address of the `rendezvousd` instance (host:port).
    pub rendezvous: SocketAddr,
    /// Shared short code (4–32 ASCII alphanumeric). Both peers use the
    /// same value; generate via [`generate_code`] or accept user input.
    pub code: String,
    /// This device's identity (keypair + cert).
    pub identity: Arc<Identity>,
    /// This device's UUID.
    pub device_id: Uuid,
    /// Pair of STUN servers to query for the public endpoint and to
    /// classify the local NAT. Pass [`DEFAULT_STUN_SERVERS`] when in
    /// doubt.
    pub stun_servers: [String; 2],
}

/// Establish a peer-to-peer QUIC session through a rendezvous server.
///
/// Steps:
/// 1. Bind a fresh UDP socket on `0.0.0.0:0`.
/// 2. Query STUN on that socket to learn our public endpoint and check
///    whether we're on a symmetric NAT (returns
///    [`Error::HolePunchFailed`] up front if so — Phase 2 will route
///    around this via the relay fallback).
/// 3. Register at the rendezvous and wait for the peer to do the same.
/// 4. Convert the socket to a `std::net::UdpSocket` and hand it to
///    [`QuicEndpoint::from_socket`].
/// 5. Race connect/accept as the actual punch.
pub async fn establish_via_rendezvous(params: RendezvousParams) -> Result<EstablishedSession> {
    let RendezvousParams {
        rendezvous,
        code,
        identity,
        device_id,
        stun_servers,
    } = params;

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let socket = UdpSocket::bind(bind).await.map_err(Error::Network)?;
    info!("traversal: bound UDP socket at {}", socket.local_addr().map_err(Error::Network)?);

    let stun_a = resolve_first(&stun_servers[0]).await?;
    let stun_b = resolve_first(&stun_servers[1]).await?;
    debug!("traversal: STUN servers resolved to {stun_a} and {stun_b}");

    let class = classify_nat(&socket, stun_a, stun_b).await?;
    let public_endpoint = match class {
        NatClass::Cone { public } => public,
        NatClass::Symmetric => {
            return Err(Error::HolePunchFailed(
                "symmetric NAT detected — UDP hole punching cannot succeed (enable relay fallback in Phase 2)".to_string(),
            ));
        }
    };
    info!("traversal: public endpoint {public_endpoint}");

    let our_fp = identity.fingerprint();
    let req = RegisterRequest {
        protocol_version: RENDEZVOUS_PROTO_VERSION,
        code,
        public_endpoint,
        cert_fingerprint: our_fp,
        device_id: *device_id.as_bytes(),
    };
    let peer = rendezvous_register(rendezvous, req)
        .await
        .map_err(|e| Error::Rendezvous(e.to_string()))?;
    info!(
        "traversal: paired with peer device {} at {}",
        Uuid::from_bytes(peer.device_id),
        peer.endpoint,
    );

    // Hand the (already-STUN-pinned) socket to quinn. From this point on
    // we can no longer raw-send_to — only quinn drives the socket.
    let std_socket = socket.into_std().map_err(Error::Network)?;
    let endpoint = QuicEndpoint::from_socket(std_socket, identity.clone())?;

    let connection = punch::race_connect_and_accept(&endpoint, peer.endpoint, peer.fingerprint).await?;

    Ok(EstablishedSession {
        endpoint,
        connection,
        peer_endpoint: peer.endpoint,
        peer_fingerprint: peer.fingerprint,
        peer_device_id: Uuid::from_bytes(peer.device_id),
    })
}

async fn resolve_first(host_port: &str) -> Result<SocketAddr> {
    lookup_host(host_port)
        .await
        .map_err(Error::Network)?
        .next()
        .ok_or_else(|| Error::Rendezvous(format!("could not resolve STUN server '{host_port}'")))
}

/// Generate a fresh 6-character base32 pairing code. Crockford-style:
/// no I/L/O/U to keep it human-typable.
pub fn generate_code() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTVWXYZ23456789";
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..6).map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_code_shape() {
        for _ in 0..50 {
            let c = generate_code();
            assert_eq!(c.len(), 6);
            assert!(c.chars().all(|c| c.is_ascii_alphanumeric()));
        }
    }
}
