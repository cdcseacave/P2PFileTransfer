//! UDP hole-punch on top of QUIC.
//!
//! Both peers, having exchanged public endpoints over the rendezvous,
//! simultaneously race [`QuicEndpoint::connect`] against
//! [`QuicEndpoint::accept`]. QUIC `Initial` packets *are* the
//! hole-punch — quinn sends one as soon as `connect` is called, and the
//! receiving side will return from `accept` as soon as the packet
//! traverses both NATs. Whichever direction wins the race becomes the
//! established [`QuicConnection`]; the losing future is dropped.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::timeout;
use tracing::debug;

use crate::error::{Error, Result};
use crate::identity::Fingerprint;
use crate::network::quic::{QuicConnection, QuicEndpoint};

/// How long we wait for either direction to complete before giving up.
/// On the wire the typical first-Initial timeout in `quinn` is several
/// seconds; this is the application-level patience knob for a stuck
/// peer (down, blocked by a strict firewall, behind symmetric NAT, ...).
pub const PUNCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Race a `connect(peer)` against an `accept()` on the same endpoint.
/// Returns the first one to succeed. Both peers run this concurrently.
pub async fn race_connect_and_accept(
    endpoint: &QuicEndpoint,
    peer_addr: SocketAddr,
    peer_fingerprint: Fingerprint,
) -> Result<QuicConnection> {
    debug!("starting hole-punch race to {peer_addr}");

    let result = timeout(PUNCH_TIMEOUT, async {
        tokio::select! {
            r = endpoint.connect(peer_addr, peer_fingerprint) => r,
            r = endpoint.accept() => r,
        }
    })
    .await
    .map_err(|_| Error::HolePunchFailed(format!(
        "no QUIC handshake completed with {peer_addr} within {:?} (peer down, strict firewall, or symmetric NAT)",
        PUNCH_TIMEOUT,
    )))?;

    match &result {
        Ok(conn) => debug!("hole-punch succeeded: {}", conn.peer_addr()),
        Err(e) => debug!("hole-punch race lost: {e}"),
    }
    result
}
