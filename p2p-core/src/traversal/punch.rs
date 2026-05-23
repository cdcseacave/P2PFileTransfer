//! UDP hole-punch on top of QUIC.
//!
//! Both peers, having exchanged public endpoints over the rendezvous,
//! race [`QuicEndpoint::connect`] against [`QuicEndpoint::accept`] —
//! but only one of the two resulting connections wins, and which one
//! is chosen is decided **deterministically** by the peers' device IDs
//! (smaller device_id ⇒ QUIC client). Without that tiebreaker each
//! peer's `tokio::select!` could pick a different direction, leaving
//! them on mismatched connections that close immediately.
//!
//! QUIC `Initial` packets *are* the hole-punch: quinn sends one as
//! soon as `connect` is called, and the receiving side returns from
//! `accept` once the packet has crossed both NATs. The losing side
//! still runs the opposite future briefly to keep the NAT mapping
//! warm — even if its connection result is discarded, the outbound
//! Initial it sent helps open the responder's NAT before that side's
//! `accept` resolves.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::timeout;
use tracing::debug;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::identity::Fingerprint;
use crate::network::quic::{QuicConnection, QuicEndpoint};

/// How long we wait for the QUIC handshake to complete before giving up.
/// On the wire the typical first-Initial timeout in `quinn` is several
/// seconds; this is the application-level patience knob for a stuck
/// peer (down, blocked by a strict firewall, behind symmetric NAT, ...).
pub const PUNCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Race a `connect(peer)` against an `accept()` on the same endpoint —
/// but **the side with the smaller `device_id` always claims the
/// "client" half**, and the other side always claims the "server"
/// half. The losing future still runs (so its outbound Initial helps
/// open the responder's NAT mapping in punch mode); whichever role
/// our side was assigned is the one whose result we ultimately keep.
pub async fn race_connect_and_accept(
    endpoint: &QuicEndpoint,
    peer_addr: SocketAddr,
    peer_fingerprint: Fingerprint,
    our_device_id: Uuid,
    peer_device_id: Uuid,
) -> Result<QuicConnection> {
    let we_connect = our_device_id < peer_device_id;
    debug!(
        "QUIC handshake to {peer_addr} starting (we_connect={we_connect}, our_id={our_device_id}, peer_id={peer_device_id})",
    );

    let result: Result<QuicConnection> = timeout(PUNCH_TIMEOUT, async {
        if we_connect {
            endpoint.connect(peer_addr, peer_fingerprint).await
        } else {
            endpoint.accept().await
        }
    })
    .await
    .map_err(|_| Error::HolePunchFailed(format!(
        "no QUIC handshake completed with {peer_addr} within {:?} (peer down, strict firewall, or symmetric NAT)",
        PUNCH_TIMEOUT,
    )))?;

    match &result {
        Ok(conn) => debug!("QUIC handshake succeeded: {}", conn.peer_addr()),
        Err(e) => debug!("QUIC handshake failed: {e}"),
    }
    result
}
