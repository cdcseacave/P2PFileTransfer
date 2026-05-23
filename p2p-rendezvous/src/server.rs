//! Rendezvous server.
//!
//! Listens on a TCP port (default [`crate::DEFAULT_PORT`]), reads one
//! [`Message::Register`] per inbound connection, pairs by `code`, and
//! delivers a [`Message::Match`] to both peers when the second one
//! arrives. The server never sees user data — once both peers are
//! matched the rendezvous channel is closed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};
use tracing::{debug, info, warn};

use crate::framing;
use crate::protocol::{Message, RegisterRequest, PROTOCOL_VERSION};

/// How long a code stays valid waiting for its second peer.
pub const DEFAULT_CODE_TTL: Duration = Duration::from_secs(300);

/// How long we wait for the first frame from a freshly connected peer
/// before assuming it's dead and closing the socket. Keeps slow-loris
/// style abuse from accumulating open sockets.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(15);

/// Listen state for a single rendezvous server instance.
pub struct Server {
    listener: TcpListener,
    state: Arc<State>,
}

struct State {
    /// Map from rendezvous code → waiting peer's registration + a oneshot
    /// channel back to the waiting connection task.
    waiting: Mutex<HashMap<String, Waiter>>,
    ttl: Duration,
}

struct Waiter {
    /// The first peer's registration data.
    first: RegisterRequest,
    /// Channel that fires when the second peer arrives, delivering its
    /// registration so the first peer's task can send the inverse Match.
    notify: oneshot::Sender<RegisterRequest>,
    /// Wall-clock instant the entry expires. After this point the second
    /// peer (if any) is rejected with [`Message::Expired`].
    expires_at: Instant,
}

impl Server {
    /// Bind a server at `addr` with the default 5-minute code TTL.
    pub async fn bind(addr: SocketAddr) -> Result<Self, ServerError> {
        Self::bind_with_ttl(addr, DEFAULT_CODE_TTL).await
    }

    /// Bind a server at `addr` with a custom code lifetime.
    pub async fn bind_with_ttl(addr: SocketAddr, ttl: Duration) -> Result<Self, ServerError> {
        let listener = TcpListener::bind(addr).await.map_err(ServerError::Bind)?;
        info!("rendezvous server listening on {}", listener.local_addr().map_err(ServerError::Bind)?);
        Ok(Self {
            listener,
            state: Arc::new(State {
                waiting: Mutex::new(HashMap::new()),
                ttl,
            }),
        })
    }

    /// Actual bound address (handy when `addr` was `:0`).
    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        self.listener.local_addr().map_err(ServerError::Bind)
    }

    /// Run the accept loop. Returns only when the listener errors.
    pub async fn run(self) -> Result<(), ServerError> {
        loop {
            let (stream, peer) = match self.listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    warn!("rendezvous accept error: {e}");
                    return Err(ServerError::Bind(e));
                }
            };
            let state = self.state.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(state, stream, peer).await {
                    debug!("rendezvous connection {peer} closed: {e}");
                }
            });
        }
    }
}

async fn handle_connection(
    state: Arc<State>,
    mut stream: TcpStream,
    peer: SocketAddr,
) -> Result<(), ServerError> {
    let (mut rd, mut wr) = stream.split();

    let req = match timeout(FIRST_FRAME_TIMEOUT, framing::read_message(&mut rd)).await {
        Ok(Ok(Message::Register(r))) => r,
        Ok(Ok(other)) => {
            warn!("rendezvous unexpected first frame from {peer}: {other:?}");
            send_rejected(&mut wr, "first frame must be Register").await;
            return Ok(());
        }
        Ok(Err(e)) => {
            debug!("rendezvous decode failure from {peer}: {e}");
            return Ok(());
        }
        Err(_) => {
            debug!("rendezvous first-frame timeout from {peer}");
            return Ok(());
        }
    };

    if req.protocol_version != PROTOCOL_VERSION {
        send_rejected(
            &mut wr,
            &format!(
                "unsupported rendezvous protocol version {} (server speaks {})",
                req.protocol_version, PROTOCOL_VERSION
            ),
        )
        .await;
        return Ok(());
    }

    if !is_valid_code(&req.code) {
        send_rejected(&mut wr, "code must be 4..32 ascii-alphanumeric chars").await;
        return Ok(());
    }

    // Match if a waiter is already present for this code.
    let waiter_for_pairing = {
        let mut waiting = state.waiting.lock().await;

        // Drop expired waiters lazily on each access.
        let now = Instant::now();
        waiting.retain(|_, w| w.expires_at > now);

        waiting.remove(&req.code)
    };

    if let Some(waiter) = waiter_for_pairing {
        // We're the second peer. Send the first peer's info to ourselves
        // and the second peer's info (us) to the first via the oneshot.
        let first = waiter.first.clone();
        let match_for_us = Message::Match {
            peer_endpoint: first.public_endpoint,
            peer_fingerprint: first.cert_fingerprint,
            peer_device_id: first.device_id,
        };
        framing::write_message(&mut wr, &match_for_us)
            .await
            .map_err(ServerError::Wire)?;
        let _ = wr.shutdown().await;

        // Notify the first peer. If it disconnected before we got here
        // the send fails harmlessly.
        let _ = waiter.notify.send(req);
        return Ok(());
    }

    // We're the first peer. Register ourselves and wait for the second.
    let (tx, rx) = oneshot::channel();
    {
        let mut waiting = state.waiting.lock().await;
        if waiting.contains_key(&req.code) {
            // Two peers raced both as "first". The second to grab the
            // lock loses and is rejected; user should retry.
            drop(waiting);
            send_rejected(&mut wr, "code already in use, ask for a fresh one").await;
            return Ok(());
        }
        waiting.insert(
            req.code.clone(),
            Waiter {
                first: req.clone(),
                notify: tx,
                expires_at: Instant::now() + state.ttl,
            },
        );
    }

    let code_for_cleanup = req.code.clone();
    let outcome = timeout(state.ttl, rx).await;

    // Cleanup the slot if we held it the whole time.
    {
        let mut waiting = state.waiting.lock().await;
        if let Some(w) = waiting.get(&code_for_cleanup) {
            // Same generation only — don't drop a fresher one a retry
            // installed under the same code.
            if w.first.device_id == req.device_id {
                waiting.remove(&code_for_cleanup);
            }
        }
    }

    match outcome {
        Ok(Ok(second)) => {
            let match_for_us = Message::Match {
                peer_endpoint: second.public_endpoint,
                peer_fingerprint: second.cert_fingerprint,
                peer_device_id: second.device_id,
            };
            framing::write_message(&mut wr, &match_for_us)
                .await
                .map_err(ServerError::Wire)?;
            let _ = wr.shutdown().await;
            Ok(())
        }
        Ok(Err(_)) | Err(_) => {
            // TTL expired or the oneshot got dropped. Tell the client.
            let _ = framing::write_message(&mut wr, &Message::Expired).await;
            let _ = wr.shutdown().await;
            Ok(())
        }
    }
}

async fn send_rejected<W>(w: &mut W, reason: &str)
where
    W: tokio::io::AsyncWriteExt + Unpin,
{
    let _ = framing::write_message(
        w,
        &Message::Rejected {
            reason: reason.to_string(),
        },
    )
    .await;
    let _ = w.shutdown().await;
}

fn is_valid_code(code: &str) -> bool {
    (4..=32).contains(&code.len()) && code.chars().all(|c| c.is_ascii_alphanumeric())
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("rendezvous bind error: {0}")]
    Bind(std::io::Error),
    #[error("rendezvous wire error: {0}")]
    Wire(crate::protocol::RendezvousProtoError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn matches_two_peers_with_same_code() {
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let server = Server::bind(bind).await.unwrap();
        let server_addr = server.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.run().await;
        });

        let a = RegisterRequest {
            protocol_version: PROTOCOL_VERSION,
            code: "ABC123".to_string(),
            public_endpoint: "1.2.3.4:5678".parse().unwrap(),
            cert_fingerprint: [0xAA; 32],
            device_id: [0x01; 16],
        };
        let b = RegisterRequest {
            protocol_version: PROTOCOL_VERSION,
            code: "ABC123".to_string(),
            public_endpoint: "5.6.7.8:9012".parse().unwrap(),
            cert_fingerprint: [0xBB; 32],
            device_id: [0x02; 16],
        };

        let a_task = tokio::spawn(crate::client::register(server_addr, a.clone()));
        // Slight delay to make A definitely the first peer.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let b_task = tokio::spawn(crate::client::register(server_addr, b.clone()));

        let a_match = a_task.await.unwrap().unwrap();
        let b_match = b_task.await.unwrap().unwrap();

        assert_eq!(a_match.endpoint, b.public_endpoint);
        assert_eq!(a_match.fingerprint, b.cert_fingerprint);
        assert_eq!(a_match.device_id, b.device_id);
        assert_eq!(b_match.endpoint, a.public_endpoint);
        assert_eq!(b_match.fingerprint, a.cert_fingerprint);
        assert_eq!(b_match.device_id, a.device_id);
    }

    #[tokio::test]
    async fn rejects_bad_code() {
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let server = Server::bind(bind).await.unwrap();
        let server_addr = server.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.run().await;
        });

        let bad = RegisterRequest {
            protocol_version: PROTOCOL_VERSION,
            code: "!".to_string(),
            public_endpoint: "1.2.3.4:5678".parse().unwrap(),
            cert_fingerprint: [0u8; 32],
            device_id: [0u8; 16],
        };
        let err = crate::client::register(server_addr, bad).await.unwrap_err();
        match err {
            crate::client::ClientError::Rejected(reason) => {
                assert!(reason.contains("code"));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }
}
