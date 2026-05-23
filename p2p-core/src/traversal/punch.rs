//! UDP hole-punch on top of QUIC.
//!
//! Both peers exchanged public endpoints over the rendezvous. The
//! punch then drives [`QuicEndpoint::connect`] *and*
//! [`QuicEndpoint::accept`] in parallel on both sides — the connect
//! futures send the outbound QUIC `Initial` packets that open both
//! NAT mappings, and whichever direction's handshake completes first
//! is the connection we keep. The peer with the smaller `device_id`
//! starts its `connect` immediately; the other peer delays its
//! `connect` by [`SECONDARY_CONNECT_DELAY`] so the two `Initial`
//! flights don't perfectly collide on a strict NAT.
//!
//! After a connection arrives via `accept`, we verify the source
//! address matches `peer_addr` (the public endpoint the rendezvous
//! gave us). An unexpected source means a third party opened a QUIC
//! handshake to our socket; we drop it and keep listening.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::{sleep, timeout};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::identity::Fingerprint;
use crate::network::quic::{QuicConnection, QuicEndpoint};

/// How long we wait for the QUIC handshake to complete before giving up.
/// On the wire the typical first-Initial timeout in `quinn` is several
/// seconds; this is the application-level patience knob for a stuck
/// peer (down, blocked by a strict firewall, behind symmetric NAT, ...).
pub const PUNCH_TIMEOUT: Duration = Duration::from_secs(30);

/// The peer with the larger `device_id` waits this long before issuing
/// its outbound `connect` so the two `Initial` flights don't collide
/// on a strict-NAT mapping. Small enough that the human-perceptible
/// pairing latency is unaffected.
pub const SECONDARY_CONNECT_DELAY: Duration = Duration::from_millis(50);

/// Race both directions of the QUIC handshake. Each peer launches a
/// `connect(peer_addr)` *and* a loop on `accept_from(peer_addr)`. The
/// first successful handshake (in either direction) is returned and
/// the loser is dropped. Both `connect` calls fire to open the NAT
/// mappings symmetrically; staggering them by
/// [`SECONDARY_CONNECT_DELAY`] keeps Initial packets from colliding
/// in a way some NATs treat as out-of-order garbage.
pub async fn race_connect_and_accept(
    endpoint: &QuicEndpoint,
    peer_addr: SocketAddr,
    peer_fingerprint: Fingerprint,
    our_device_id: Uuid,
    peer_device_id: Uuid,
) -> Result<QuicConnection> {
    let we_go_first = our_device_id < peer_device_id;
    debug!(
        "QUIC punch to {peer_addr} starting (we_go_first={we_go_first}, our_id={our_device_id}, peer_id={peer_device_id})",
    );

    let connect_fut = async {
        if !we_go_first {
            sleep(SECONDARY_CONNECT_DELAY).await;
        }
        endpoint.connect(peer_addr, peer_fingerprint).await
    };
    let accept_fut = accept_from(endpoint, peer_addr);

    let outcome: Result<QuicConnection> = timeout(PUNCH_TIMEOUT, async {
        tokio::select! {
            res = connect_fut => res,
            res = accept_fut => res,
        }
    })
    .await
    .map_err(|_| Error::HolePunchFailed(format!(
        "no QUIC handshake completed with {peer_addr} within {:?} (peer down, strict firewall, or symmetric NAT)",
        PUNCH_TIMEOUT,
    )))?;

    match &outcome {
        Ok(conn) => debug!("QUIC handshake succeeded: {}", conn.peer_addr()),
        Err(e) => debug!("QUIC handshake failed: {e}"),
    }
    outcome
}

/// Run `endpoint.accept()` and verify the remote socket address
/// matches `expected`. A mismatch is a third party trying to ride
/// our open mapping; drop the connection and keep listening.
async fn accept_from(endpoint: &QuicEndpoint, expected: SocketAddr) -> Result<QuicConnection> {
    loop {
        let conn = endpoint.accept().await?;
        let peer = conn.peer_addr();
        if peer_matches(peer, expected) {
            return Ok(conn);
        }
        warn!("dropping unexpected inbound QUIC connection from {peer} (expected {expected})");
        // Don't propagate the error — the rightful peer might still
        // arrive on the next accept.
        drop(conn);
    }
}

/// Equal as a peer address. The expected address came back from
/// the rendezvous, so it should be the post-NAT public endpoint
/// the peer's kernel is sending from — exact equality is the right
/// check.
fn peer_matches(observed: SocketAddr, expected: SocketAddr) -> bool {
    observed == expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn peer_matches_exact() {
        let a: SocketAddr = "192.0.2.1:5555".parse().unwrap();
        let b: SocketAddr = "192.0.2.1:5555".parse().unwrap();
        assert!(peer_matches(a, b));
    }

    #[test]
    fn peer_matches_rejects_port_change() {
        let a: SocketAddr = "192.0.2.1:5555".parse().unwrap();
        let b: SocketAddr = "192.0.2.1:6666".parse().unwrap();
        assert!(!peer_matches(a, b));
    }

    #[test]
    fn peer_matches_rejects_ip_change() {
        let a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 5555);
        let b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)), 5555);
        assert!(!peer_matches(a, b));
    }
}
