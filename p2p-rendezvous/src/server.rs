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
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{timeout, Instant};
use tracing::{debug, info, warn};

use crate::framing;
use crate::protocol::{Message, RegisterRequest, PROTOCOL_VERSION};
use crate::relay::{Relay, FINGERPRINT_LEN, SESSION_TOKEN_LEN};

/// How long a code stays valid waiting for its second peer.
pub const DEFAULT_CODE_TTL: Duration = Duration::from_secs(300);

/// How long we wait for the first frame from a freshly connected peer
/// before assuming it's dead and closing the socket. Keeps slow-loris
/// style abuse from accumulating open sockets.
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(15);

/// Default ceiling on concurrently-handled rendezvous connections. A
/// rendezvous session is `oneshot::Receiver`-backed and idle most of
/// the time, so this can be generous — but it must be finite so an
/// attacker can't fan out connections until the process runs out of
/// file descriptors.
pub const DEFAULT_MAX_CONCURRENT: usize = 1024;

/// Listen state for a single rendezvous server instance.
pub struct Server {
    listener: TcpListener,
    state: Arc<State>,
    concurrency: Arc<Semaphore>,
}

struct State {
    /// Map from rendezvous code → waiting peer's registration + a oneshot
    /// channel back to the waiting connection task.
    waiting: Mutex<HashMap<String, Waiter>>,
    ttl: Duration,
    /// Optional relay handle. If present, pairs where either side sets
    /// `want_relay` are returned as [`Message::RelayMatch`] with a
    /// session reserved on this relay; otherwise the server falls
    /// through to a direct [`Message::Match`] regardless.
    relay: Option<Relay>,
}

struct Waiter {
    /// The first peer's registration data.
    first: RegisterRequest,
    /// Channel that fires when the second peer arrives, delivering
    /// either the second peer's registration (direct) or a relay
    /// session reservation (relay).
    notify: oneshot::Sender<NotifyPayload>,
    /// Wall-clock instant the entry expires. After this point the second
    /// peer (if any) is rejected with [`Message::Expired`].
    expires_at: Instant,
}

/// What the second peer's task tells the waiting first peer's task.
enum NotifyPayload {
    /// Direct hole-punch match. Send the second peer's registration to
    /// the first peer as a [`Message::Match`].
    Direct(RegisterRequest),
    /// Relay-mediated match. The relay session is already reserved on
    /// this server's relay; the first peer's task just needs to send
    /// the RelayMatch frame.
    Relay {
        token: [u8; SESSION_TOKEN_LEN],
        relay_endpoint: SocketAddr,
        peer: PeerSummary,
    },
}

struct PeerSummary {
    fingerprint: [u8; FINGERPRINT_LEN],
    device_id: [u8; 16],
}

impl Server {
    /// Bind a server at `addr` with the default 5-minute code TTL,
    /// default concurrency cap, and no relay attached.
    pub async fn bind(addr: SocketAddr) -> Result<Self, ServerError> {
        Self::bind_with(addr, DEFAULT_CODE_TTL, DEFAULT_MAX_CONCURRENT).await
    }

    /// Bind a server at `addr` with a custom code lifetime and the
    /// default concurrency cap.
    pub async fn bind_with_ttl(addr: SocketAddr, ttl: Duration) -> Result<Self, ServerError> {
        Self::bind_with(addr, ttl, DEFAULT_MAX_CONCURRENT).await
    }

    /// Bind a server at `addr` with a custom code lifetime and a
    /// custom concurrency cap. Once the cap is reached, the accept
    /// loop applies backpressure on the listener until a slot frees;
    /// no more in-flight handlers will be spawned.
    pub async fn bind_with(
        addr: SocketAddr,
        ttl: Duration,
        max_concurrent: usize,
    ) -> Result<Self, ServerError> {
        let listener = TcpListener::bind(addr).await.map_err(ServerError::Bind)?;
        info!(
            "rendezvous server listening on {} (max_concurrent={max_concurrent})",
            listener.local_addr().map_err(ServerError::Bind)?
        );
        Ok(Self {
            listener,
            state: Arc::new(State {
                waiting: Mutex::new(HashMap::new()),
                ttl,
                relay: None,
            }),
            concurrency: Arc::new(Semaphore::new(max_concurrent)),
        })
    }

    /// Attach a running relay handle. Required for `RelayMatch`
    /// responses; without it, peers that set `want_relay` still get a
    /// direct `Match` (and will fail their hole-punch).
    pub fn attach_relay(&mut self, relay: Relay) {
        Arc::get_mut(&mut self.state)
            .expect("attach_relay must be called before run()")
            .relay = Some(relay);
    }

    /// Actual bound address (handy when `addr` was `:0`).
    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        self.listener.local_addr().map_err(ServerError::Bind)
    }

    /// Run the accept loop. Returns only when the listener errors.
    pub async fn run(self) -> Result<(), ServerError> {
        loop {
            // Acquire a concurrency permit *before* accept so we apply
            // backpressure on the listener — incoming connections sit
            // in the kernel queue (or get RST'd) instead of piling up
            // as detached spawned tasks once the cap is reached.
            let permit = self
                .concurrency
                .clone()
                .acquire_owned()
                .await
                .expect("rendezvous semaphore never closed");
            let (stream, peer) = match self.listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    warn!("rendezvous accept error: {e}");
                    return Err(ServerError::Bind(e));
                }
            };
            let state = self.state.clone();
            tokio::spawn(async move {
                let _permit = permit;
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

    // Stamp the registration with the TCP source IP so a peer can't
    // direct the punch at a third-party victim by lying about its
    // public address. The UDP port still has to come from the client
    // because the punch socket is on a different transport, but the
    // IP is forgeable for reflection and the TCP peer IP is the
    // source of truth.
    let mut req = req;
    req.public_endpoint = SocketAddr::new(peer.ip(), req.public_endpoint.port());

    if let Some(waiter) = waiter_for_pairing {
        // We're the second peer. Decide direct vs relay using:
        //   relay needed = either peer set want_relay,
        // **and** the server actually has a relay attached. Otherwise
        // we fall back to direct (which will fail the punch — but that
        // failure is the user's signal to enable relay mode).
        let first = waiter.first.clone();
        let needs_relay = req.want_relay || first.want_relay;
        if needs_relay {
            if let Some(relay) = state.relay.as_ref() {
                let token: [u8; SESSION_TOKEN_LEN] = rand::random();
                let peer_a_fp: [u8; FINGERPRINT_LEN] = first.cert_fingerprint;
                let peer_b_fp: [u8; FINGERPRINT_LEN] = req.cert_fingerprint;
                if let Err(e) = relay.reserve_session(token, peer_a_fp, peer_b_fp).await {
                    warn!("relay refused session for code {}: {e}", req.code);
                    send_rejected(&mut wr, "relay refused session").await;
                    return Ok(());
                }

                let relay_addr = relay.public_addr();
                let match_for_us = Message::RelayMatch {
                    relay_endpoint: relay_addr,
                    relay_session_token: token,
                    peer_fingerprint: first.cert_fingerprint,
                    peer_device_id: first.device_id,
                };
                framing::write_message(&mut wr, &match_for_us)
                    .await
                    .map_err(ServerError::Wire)?;
                let _ = wr.shutdown().await;

                let _ = waiter.notify.send(NotifyPayload::Relay {
                    token,
                    relay_endpoint: relay_addr,
                    peer: PeerSummary {
                        fingerprint: req.cert_fingerprint,
                        device_id: req.device_id,
                    },
                });
                return Ok(());
            }
            debug!("relay requested but server has no --relay-bind — falling back to direct match");
        }

        let match_for_us = Message::Match {
            peer_endpoint: first.public_endpoint,
            peer_fingerprint: first.cert_fingerprint,
            peer_device_id: first.device_id,
        };
        framing::write_message(&mut wr, &match_for_us)
            .await
            .map_err(ServerError::Wire)?;
        let _ = wr.shutdown().await;

        // Notify the first peer.
        let _ = waiter.notify.send(NotifyPayload::Direct(req));
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
        Ok(Ok(NotifyPayload::Direct(second))) => {
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
        Ok(Ok(NotifyPayload::Relay {
            token,
            relay_endpoint,
            peer,
        })) => {
            let match_for_us = Message::RelayMatch {
                relay_endpoint,
                relay_session_token: token,
                peer_fingerprint: peer.fingerprint,
                peer_device_id: peer.device_id,
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
            want_relay: false,
        };
        let b = RegisterRequest {
            protocol_version: PROTOCOL_VERSION,
            code: "ABC123".to_string(),
            public_endpoint: "5.6.7.8:9012".parse().unwrap(),
            cert_fingerprint: [0xBB; 32],
            device_id: [0x02; 16],
            want_relay: false,
        };

        let a_task = tokio::spawn(crate::client::register(server_addr, a.clone()));
        // Slight delay to make A definitely the first peer.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let b_task = tokio::spawn(crate::client::register(server_addr, b.clone()));

        let a_match = a_task.await.unwrap().unwrap();
        let b_match = b_task.await.unwrap().unwrap();

        // The server rewrites the IP portion of each peer's
        // public endpoint to its TCP source — preserving only the
        // user-supplied UDP port. See `rewrites_public_endpoint_ip_to_tcp_source`.
        assert!(a_match.endpoint.ip().is_loopback());
        assert_eq!(a_match.endpoint.port(), b.public_endpoint.port());
        assert_eq!(a_match.fingerprint, b.cert_fingerprint);
        assert_eq!(a_match.device_id, b.device_id);
        assert!(b_match.endpoint.ip().is_loopback());
        assert_eq!(b_match.endpoint.port(), a.public_endpoint.port());
        assert_eq!(b_match.fingerprint, a.cert_fingerprint);
        assert_eq!(b_match.device_id, a.device_id);
    }

    #[tokio::test]
    async fn rewrites_public_endpoint_ip_to_tcp_source() {
        // A peer claims its public IP is 99.99.99.99 but connects from
        // localhost. The server must rewrite the IP it gossips to the
        // second peer to the actual TCP source, keeping the port the
        // peer supplied. This blocks reflection attacks where a peer
        // names a third-party victim as its "public" address.
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let server = Server::bind(bind).await.unwrap();
        let server_addr = server.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.run().await;
        });

        let a = RegisterRequest {
            protocol_version: PROTOCOL_VERSION,
            code: "SPF000".to_string(),
            public_endpoint: "99.99.99.99:5555".parse().unwrap(),
            cert_fingerprint: [0xAA; 32],
            device_id: [0x01; 16],
            want_relay: false,
        };
        let b = RegisterRequest {
            protocol_version: PROTOCOL_VERSION,
            code: "SPF000".to_string(),
            public_endpoint: "88.88.88.88:6666".parse().unwrap(),
            cert_fingerprint: [0xBB; 32],
            device_id: [0x02; 16],
            want_relay: false,
        };

        let a_task = tokio::spawn(crate::client::register(server_addr, a));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let b_task = tokio::spawn(crate::client::register(server_addr, b));

        let a_match = a_task.await.unwrap().unwrap();
        let b_match = b_task.await.unwrap().unwrap();

        // The IP that A sees for B must be loopback (the TCP source),
        // not the spoofed 88.88.88.88. The port stays as 6666.
        assert!(a_match.endpoint.ip().is_loopback());
        assert_eq!(a_match.endpoint.port(), 6666);
        assert!(b_match.endpoint.ip().is_loopback());
        assert_eq!(b_match.endpoint.port(), 5555);
    }

    #[tokio::test]
    async fn caps_concurrent_sessions() {
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let server = Server::bind_with(bind, DEFAULT_CODE_TTL, 2).await.unwrap();
        let server_addr = server.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.run().await;
        });

        // Two slow clients that just open TCP connections and never
        // send a Register frame. They each consume one of the two
        // permits for FIRST_FRAME_TIMEOUT.
        let _slow_a = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let _slow_b = tokio::net::TcpStream::connect(server_addr).await.unwrap();

        // Give the accept loop a moment to claim both permits.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // A third connection beyond the cap. The TCP connect itself
        // still succeeds (kernel queue), but the server hasn't picked
        // it up yet — verify by racing a short timeout against the
        // server actually doing anything with us.
        let mut third = tokio::net::TcpStream::connect(server_addr).await.unwrap();
        let request = RegisterRequest {
            protocol_version: PROTOCOL_VERSION,
            code: "CAP000".to_string(),
            public_endpoint: "1.2.3.4:5678".parse().unwrap(),
            cert_fingerprint: [0u8; 32],
            device_id: [0u8; 16],
            want_relay: false,
        };
        framing::write_message(&mut third, &Message::Register(request))
            .await
            .unwrap();

        // The third client should not get a response within 250ms: the
        // first two permits are still held by the unresponsive peers.
        let recv = tokio::time::timeout(
            Duration::from_millis(250),
            framing::read_message(&mut third),
        )
        .await;
        assert!(
            recv.is_err(),
            "third client should be queued by the cap, not served"
        );
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
            want_relay: false,
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
