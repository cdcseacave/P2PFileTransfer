//! Rendezvous client.
//!
//! `register(server, req)` opens a TCP connection to the rendezvous,
//! sends one [`RegisterRequest`], and awaits the server's pairing
//! [`Message::Match`]. Returns the peer's endpoint / fingerprint /
//! device id (or an error if the server explicitly rejected, the code
//! expired, or the wire layer broke).

use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::framing;
use crate::protocol::{DeviceId, Fingerprint, Message, RegisterRequest, RendezvousProtoError};
use crate::relay::SESSION_TOKEN_LEN;

/// Hard ceiling on how long we wait between sending REGISTER and seeing
/// MATCH. Servers default to a 5-minute code TTL, so wait a touch longer
/// to receive a clean [`Message::Expired`] if no peer shows.
const REGISTER_WAIT_TIMEOUT: Duration = Duration::from_secs(310);

/// Peer information returned by a direct (hole-punched) rendezvous match.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub endpoint: SocketAddr,
    pub fingerprint: Fingerprint,
    pub device_id: DeviceId,
}

/// Relay-mediated match information. The client should send a
/// [`crate::relay::RelayHello`] (token + own fingerprint) on its UDP
/// socket to `relay_endpoint`, then run a normal QUIC handshake with
/// the **relay's** address as the apparent peer endpoint — the relay
/// forwards QUIC packets to the real peer.
#[derive(Debug, Clone)]
pub struct RelayInfo {
    pub relay_endpoint: SocketAddr,
    pub session_token: [u8; SESSION_TOKEN_LEN],
    pub peer_fingerprint: Fingerprint,
    pub peer_device_id: DeviceId,
}

/// What the rendezvous returned: a direct hole-punch match, a
/// relay-mediated match, or `None` if the code expired (peer never
/// arrived in time).
#[derive(Debug, Clone)]
pub enum MatchOutcome {
    Direct(PeerInfo),
    Relay(RelayInfo),
}

/// Register at `server` with `req` and await a peer match. Returns a
/// [`PeerInfo`] for a direct hole-punch; [`register_full`] returns the
/// full [`MatchOutcome`] (direct **or** relay) — use it when running
/// in Phase 2 / `--rendezvous --relay` mode.
pub async fn register(server: SocketAddr, req: RegisterRequest) -> Result<PeerInfo, ClientError> {
    match register_full(server, req).await? {
        MatchOutcome::Direct(p) => Ok(p),
        MatchOutcome::Relay(_) => Err(ClientError::UnexpectedFromServer(
            "rendezvous returned RelayMatch but caller used the direct-only register() helper"
                .to_string(),
        )),
    }
}

/// Register at `server` with `req` and await any kind of match (direct
/// or relay-mediated).
pub async fn register_full(
    server: SocketAddr,
    req: RegisterRequest,
) -> Result<MatchOutcome, ClientError> {
    let mut stream = TcpStream::connect(server)
        .await
        .map_err(ClientError::Connect)?;
    let _ = stream.set_nodelay(true);

    framing::write_message(&mut stream, &Message::Register(req))
        .await
        .map_err(ClientError::Wire)?;

    let response = timeout(REGISTER_WAIT_TIMEOUT, framing::read_message(&mut stream))
        .await
        .map_err(|_| ClientError::Timeout)?
        .map_err(ClientError::Wire)?;

    // Server closes after delivering the match; tear down our half.
    let _ = stream.shutdown().await;

    match response {
        Message::Match {
            peer_endpoint,
            peer_fingerprint,
            peer_device_id,
        } => Ok(MatchOutcome::Direct(PeerInfo {
            endpoint: peer_endpoint,
            fingerprint: peer_fingerprint,
            device_id: peer_device_id,
        })),
        Message::RelayMatch {
            relay_endpoint,
            relay_session_token,
            peer_fingerprint,
            peer_device_id,
        } => Ok(MatchOutcome::Relay(RelayInfo {
            relay_endpoint,
            session_token: relay_session_token,
            peer_fingerprint,
            peer_device_id,
        })),
        Message::Expired => Err(ClientError::Expired),
        Message::Rejected { reason } => Err(ClientError::Rejected(reason)),
        Message::Register(_) => Err(ClientError::UnexpectedFromServer(
            "Register frame from server".to_string(),
        )),
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("rendezvous connect failed: {0}")]
    Connect(std::io::Error),
    #[error("rendezvous wire: {0}")]
    Wire(RendezvousProtoError),
    #[error("rendezvous timed out waiting for peer")]
    Timeout,
    #[error("rendezvous code expired before peer arrived")]
    Expired,
    #[error("rendezvous rejected: {0}")]
    Rejected(String),
    #[error("unexpected message from rendezvous server: {0}")]
    UnexpectedFromServer(String),
}
