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

use p2p_rendezvous::client::{register_full, MatchOutcome};
use p2p_rendezvous::protocol::{RegisterRequest, PROTOCOL_VERSION as RENDEZVOUS_PROTO_VERSION};
use p2p_rendezvous::relay::{RelayHello, FINGERPRINT_LEN, SESSION_TOKEN_LEN};

use crate::error::{Error, Result};
use crate::identity::Identity;
use crate::network::quic::{QuicConnection, QuicEndpoint};

use self::stun::{classify_nat, NatClass};

/// Default pair of STUN servers used when the caller does not supply
/// their own. Two are needed so [`stun::classify_nat`] can spot
/// symmetric-NAT mappings (different mapped port per destination).
pub const DEFAULT_STUN_SERVERS: [&str; 2] = ["stun.l.google.com:19302", "stun1.l.google.com:19302"];

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
    /// Force relay mode regardless of STUN classification. Useful for
    /// debugging; the more common case is "let symmetric-NAT detection
    /// decide" (`false`).
    pub force_relay: bool,
}

/// Establish a peer-to-peer QUIC session through a rendezvous server.
///
/// Steps:
/// 1. Bind a fresh UDP socket on `0.0.0.0:0`.
/// 2. Query STUN on that socket to learn our public endpoint and
///    classify the local NAT. On Cone NAT we register for direct
///    punching; on Symmetric NAT we set `want_relay = true` so the
///    rendezvous returns a relay endpoint instead of trying to punch.
///    A loopback rendezvous (`127.0.0.0/8` or `::1`, i.e. local-dev or
///    tests) is by definition not behind a discoverable NAT — skip
///    STUN there and use the bound socket address directly.
/// 3. Register at the rendezvous and wait for the peer to do the same.
/// 4. Convert the socket to a `std::net::UdpSocket` and hand it to
///    [`QuicEndpoint::from_socket`].
/// 5. Either race connect/accept as the actual punch (Direct outcome)
///    or send a [`RelayHello`] and run QUIC through the relay (Relay
///    outcome).
pub async fn establish_via_rendezvous(params: RendezvousParams) -> Result<EstablishedSession> {
    let RendezvousParams {
        rendezvous,
        code,
        identity,
        device_id,
        stun_servers,
        force_relay,
    } = params;

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let socket = UdpSocket::bind(bind).await.map_err(Error::Network)?;
    info!(
        "traversal: bound UDP socket at {}",
        socket.local_addr().map_err(Error::Network)?
    );

    // A loopback rendezvous (tests, local dev) is by definition not
    // behind any NAT we can discover with STUN. Worse, STUN against a
    // real server would return our public-NAT-mapped port, which has
    // no bearing on the loopback socket — the rendezvous then stamps
    // the request with `127.0.0.1` (TCP source) + that STUN-mapped
    // port, and the resulting punch target never reaches the local
    // socket. Skip STUN here and use the bound socket address directly.
    let (public_endpoint, want_relay) = if rendezvous.ip().is_loopback() {
        let local = socket.local_addr().map_err(Error::Network)?;
        info!("traversal: loopback rendezvous {rendezvous} — skipping STUN, using local {local}");
        (local, force_relay)
    } else {
        let stun_a = resolve_first(&stun_servers[0]).await?;
        let stun_b = resolve_first(&stun_servers[1]).await?;
        debug!("traversal: STUN servers resolved to {stun_a} and {stun_b}");

        let class = classify_nat(&socket, stun_a, stun_b).await?;
        match class {
            NatClass::Cone { public } => (public, force_relay),
            NatClass::Symmetric => {
                // Use the local socket address as a placeholder public endpoint
                // for the rendezvous request — the rendezvous won't use it
                // for relay mode (it gives back the relay's address), but
                // serde still expects a SocketAddr.
                let local = socket.local_addr().map_err(Error::Network)?;
                (local, true)
            }
        }
    };
    info!(
        "traversal: public endpoint {public_endpoint} ({})",
        if want_relay {
            "relay requested"
        } else {
            "direct punch"
        },
    );

    let our_fp = identity.fingerprint();
    let req = RegisterRequest {
        protocol_version: RENDEZVOUS_PROTO_VERSION,
        code,
        public_endpoint,
        cert_fingerprint: our_fp,
        device_id: *device_id.as_bytes(),
        want_relay,
    };
    let outcome = register_full(rendezvous, req)
        .await
        .map_err(|e| Error::Rendezvous(e.to_string()))?;

    match outcome {
        MatchOutcome::Direct(peer) => {
            let peer_id = Uuid::from_bytes(peer.device_id);
            info!(
                "traversal: direct match with peer device {peer_id} at {}",
                peer.endpoint,
            );
            let std_socket = socket.into_std().map_err(Error::Network)?;
            let endpoint = QuicEndpoint::from_socket(std_socket, identity.clone())?;
            let connection = punch::race_connect_and_accept(
                &endpoint,
                peer.endpoint,
                peer.fingerprint,
                device_id,
                peer_id,
            )
            .await?;
            Ok(EstablishedSession {
                endpoint,
                connection,
                peer_endpoint: peer.endpoint,
                peer_fingerprint: peer.fingerprint,
                peer_device_id: peer_id,
            })
        }
        MatchOutcome::Relay(relay) => {
            info!(
                "traversal: relay match via {} (peer device {})",
                relay.relay_endpoint,
                Uuid::from_bytes(relay.peer_device_id),
            );
            establish_via_relay(socket, identity.clone(), relay, our_fp, device_id).await
        }
    }
}

/// Take the STUN-pinned UDP socket, send a [`RelayHello`] to the
/// relay so it can record our source address against `session_token`,
/// then hand the socket to `quinn` and race connect/accept against the
/// **relay's** apparent address (since QUIC packets to the relay get
/// forwarded to the real peer).
async fn establish_via_relay(
    socket: UdpSocket,
    identity: Arc<Identity>,
    relay: p2p_rendezvous::RelayInfo,
    our_fp: [u8; FINGERPRINT_LEN],
    device_id: Uuid,
) -> Result<EstablishedSession> {
    let hello = RelayHello {
        token: relay.session_token,
        fingerprint: our_fp,
    }
    .encode();
    // Send the hello a couple of times to survive a single dropped UDP
    // packet during the join. The relay deduplicates by source address.
    for _ in 0..3 {
        socket
            .send_to(&hello, relay.relay_endpoint)
            .await
            .map_err(Error::Network)?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let std_socket = socket.into_std().map_err(Error::Network)?;
    let endpoint = QuicEndpoint::from_socket(std_socket, identity)?;

    let peer_id = Uuid::from_bytes(relay.peer_device_id);
    let conn = punch::race_connect_and_accept(
        &endpoint,
        relay.relay_endpoint,
        relay.peer_fingerprint,
        device_id,
        peer_id,
    )
    .await?;

    Ok(EstablishedSession {
        endpoint,
        connection: conn,
        peer_endpoint: relay.relay_endpoint,
        peer_fingerprint: relay.peer_fingerprint,
        peer_device_id: peer_id,
    })
}

// Tiny no-op suppression so the never-read SESSION_TOKEN_LEN re-export
// shows up in `cargo doc` examples without a `dead_code` lint when we
// build without the relay flow exercised.
#[allow(dead_code)]
const _SESSION_TOKEN_LEN_DOCREF: usize = SESSION_TOKEN_LEN;

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
    (0..6)
        .map(|_| ALPHABET[rng.gen_range(0..ALPHABET.len())] as char)
        .collect()
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
