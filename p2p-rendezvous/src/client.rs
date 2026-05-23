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

/// Hard ceiling on how long we wait between sending REGISTER and seeing
/// MATCH. Servers default to a 5-minute code TTL, so wait a touch longer
/// to receive a clean [`Message::Expired`] if no peer shows.
const REGISTER_WAIT_TIMEOUT: Duration = Duration::from_secs(310);

/// Peer information returned by the rendezvous match.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub endpoint: SocketAddr,
    pub fingerprint: Fingerprint,
    pub device_id: DeviceId,
}

/// Register at `server` with `req` and await a peer match.
pub async fn register(server: SocketAddr, req: RegisterRequest) -> Result<PeerInfo, ClientError> {
    let mut stream = TcpStream::connect(server).await.map_err(ClientError::Connect)?;
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
        } => Ok(PeerInfo {
            endpoint: peer_endpoint,
            fingerprint: peer_fingerprint,
            device_id: peer_device_id,
        }),
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
