//! Phase 2 UDP packet relay.
//!
//! Two peers behind symmetric NAT (or with any other reason direct
//! hole-punching failed) can fall back to a relay. The relay is a plain
//! UDP packet forwarder: each peer's QUIC endpoint sends its packets to
//! the relay's UDP address, and the relay re-emits them with itself as
//! the source toward the matched peer. From quinn's perspective the
//! peer "is" the relay address; QUIC TLS still terminates end-to-end
//! between the two real peers, so the relay sees ciphertext only.
//!
//! Wire framing on the relay socket:
//!
//! * The first datagram from each peer is a [`RelayHello`] —
//!   `[MAGIC(4) | u8 version | u8 reserved | u8 session_token_len | u8 fingerprint_len | session_token | fingerprint]`.
//!   The relay parses it, records the peer's source address against
//!   the token, and (once both peers have arrived) starts forwarding.
//! * Every subsequent datagram is opaque to the relay and forwarded
//!   verbatim toward the other peer of the same session.
//!
//! The relay never inspects the QUIC bytes and never holds plaintext.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Magic bytes prefix every `RelayHello` so the forwarder can tell
/// hello packets from already-paired forwarded QUIC bytes (which never
/// start with this sequence because they're QUIC long-header packets
/// with their own format).
pub const RELAY_HELLO_MAGIC: [u8; 4] = *b"P2RZ";

/// Token size: 16 random bytes. The rendezvous generates a fresh
/// token per session and hands the same value to both peers.
pub const SESSION_TOKEN_LEN: usize = 16;

/// Cert-fingerprint size (SHA-256).
pub const FINGERPRINT_LEN: usize = 32;

/// Wall-clock idle timeout for a session — if neither peer sends a
/// packet for this long the relay forgets the pairing so a fresh code
/// can be issued.
pub const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// Maximum UDP datagram the relay reads in one go. Sized to the UDP
/// payload ceiling (65 507 bytes — `u16::MAX − IPv4 header − UDP
/// header`) plus a little slack so neither jumbo frames nor IPv6
/// fragmented datagrams get truncated.
const RECV_BUF_BYTES: usize = 65 * 1024;

/// How often the background task scans for idle sessions to evict.
/// Moving the scan off the per-packet hot path keeps the lock
/// hold-time per packet O(1) instead of O(sessions).
const IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Hello packet sent by each peer when joining a relay session.
#[derive(Debug, Clone)]
pub struct RelayHello {
    pub token: [u8; SESSION_TOKEN_LEN],
    pub fingerprint: [u8; FINGERPRINT_LEN],
}

impl RelayHello {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + 4 + SESSION_TOKEN_LEN + FINGERPRINT_LEN);
        out.extend_from_slice(&RELAY_HELLO_MAGIC);
        out.push(1); // version
        out.push(0); // reserved
        out.push(SESSION_TOKEN_LEN as u8);
        out.push(FINGERPRINT_LEN as u8);
        out.extend_from_slice(&self.token);
        out.extend_from_slice(&self.fingerprint);
        out
    }

    pub fn try_decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 || data[0..4] != RELAY_HELLO_MAGIC {
            return None;
        }
        let version = data[4];
        if version != 1 {
            return None;
        }
        let token_len = data[6] as usize;
        let fp_len = data[7] as usize;
        if token_len != SESSION_TOKEN_LEN || fp_len != FINGERPRINT_LEN {
            return None;
        }
        let want = 8 + token_len + fp_len;
        if data.len() < want {
            return None;
        }
        let mut token = [0u8; SESSION_TOKEN_LEN];
        token.copy_from_slice(&data[8..8 + token_len]);
        let mut fingerprint = [0u8; FINGERPRINT_LEN];
        fingerprint.copy_from_slice(&data[8 + token_len..8 + token_len + fp_len]);
        Some(Self { token, fingerprint })
    }
}

/// Runtime state of one relay session.
struct Session {
    /// First peer's UDP source address (recorded on its hello).
    peer_a: Option<PeerState>,
    /// Second peer's UDP source address (recorded on its hello).
    peer_b: Option<PeerState>,
    /// When the session was created — used to expire reserved-but-empty
    /// slots (peer A registered, peer B never showed).
    created_at: Instant,
    /// Most recent packet timestamp on either side. After
    /// [`SESSION_IDLE_TIMEOUT`] of inactivity the session is dropped.
    last_active: Instant,
    /// Peer A's expected fingerprint (registered by the rendezvous).
    peer_a_expected_fp: [u8; FINGERPRINT_LEN],
    /// Peer B's expected fingerprint.
    peer_b_expected_fp: [u8; FINGERPRINT_LEN],
}

#[derive(Debug, Clone, Copy)]
struct PeerState {
    addr: SocketAddr,
    /// Kept for diagnostics — the relay only routes by address but
    /// having the fingerprint on hand makes log lines unambiguous when
    /// the same NAT remaps several sessions to one source address.
    #[allow(dead_code)]
    fingerprint: [u8; FINGERPRINT_LEN],
}

/// Mutable relay state. `Mutex` is fine because the per-packet work is
/// trivial; the relay isn't CPU-bound at the lock granularity.
#[derive(Default)]
struct RelayState {
    /// Session token → session.
    sessions: HashMap<[u8; SESSION_TOKEN_LEN], Session>,
    /// Reverse index: source address → token (so packet forwarding is O(1)).
    addr_to_token: HashMap<SocketAddr, [u8; SESSION_TOKEN_LEN]>,
    /// Total bytes forwarded since startup — exposed for a future metric.
    bytes_forwarded: u64,
}

impl RelayState {
    fn reserve(
        &mut self,
        token: [u8; SESSION_TOKEN_LEN],
        peer_a_fp: [u8; FINGERPRINT_LEN],
        peer_b_fp: [u8; FINGERPRINT_LEN],
    ) -> Result<(), RelayError> {
        if peer_a_fp == peer_b_fp {
            return Err(RelayError::DuplicateFingerprint);
        }
        let now = Instant::now();
        self.sessions.insert(
            token,
            Session {
                peer_a: None,
                peer_b: None,
                created_at: now,
                last_active: now,
                peer_a_expected_fp: peer_a_fp,
                peer_b_expected_fp: peer_b_fp,
            },
        );
        Ok(())
    }

    fn forget(&mut self, token: &[u8; SESSION_TOKEN_LEN]) {
        if let Some(s) = self.sessions.remove(token) {
            if let Some(a) = s.peer_a {
                self.addr_to_token.remove(&a.addr);
            }
            if let Some(b) = s.peer_b {
                self.addr_to_token.remove(&b.addr);
            }
        }
    }

    fn evict_idle(&mut self, now: Instant) {
        let stale: Vec<[u8; SESSION_TOKEN_LEN]> = self
            .sessions
            .iter()
            .filter(|(_, s)| {
                let half_open = s.peer_a.is_none() || s.peer_b.is_none();
                if half_open {
                    now.duration_since(s.created_at) > SESSION_IDLE_TIMEOUT
                } else {
                    now.duration_since(s.last_active) > SESSION_IDLE_TIMEOUT
                }
            })
            .map(|(k, _)| *k)
            .collect();
        for token in stale {
            debug!("relay: evicting idle session");
            self.forget(&token);
        }
    }
}

/// Public relay handle. The rendezvous server holds one of these and
/// calls [`Relay::reserve_session`] each time it pairs peers in
/// "want_relay" mode; the relay's own task drives the UDP loop.
#[derive(Clone)]
pub struct Relay {
    state: Arc<Mutex<RelayState>>,
    /// Local socket address the relay listens on (for handing back to
    /// the rendezvous → client over the control channel).
    public_addr: SocketAddr,
}

impl Relay {
    /// Bind a UDP socket and spawn the forwarding loop. Returns the
    /// handle the rendezvous uses to reserve sessions.
    pub async fn bind(addr: SocketAddr, bandwidth_cap_bps: u64) -> Result<Self, RelayError> {
        let socket = UdpSocket::bind(addr).await.map_err(RelayError::Bind)?;
        let public_addr = socket.local_addr().map_err(RelayError::Bind)?;
        info!("relay: listening on {public_addr} (cap={bandwidth_cap_bps} B/s)");

        let state = Arc::new(Mutex::new(RelayState::default()));
        let handle = Self {
            state: state.clone(),
            public_addr,
        };

        tokio::spawn(forward_loop(socket, state.clone(), bandwidth_cap_bps));
        tokio::spawn(idle_sweep_loop(state));
        Ok(handle)
    }

    /// The address peers should send their relay traffic to.
    pub fn public_addr(&self) -> SocketAddr {
        self.public_addr
    }

    /// Reserve a session for two peers identified by `token`. Both
    /// fingerprints are recorded so the relay can reject impostors
    /// that know only the token but not the matching cert. Returns an
    /// error when both peers would share a fingerprint — that means
    /// either peer can occupy either slot and there's no impostor
    /// barrier left.
    pub async fn reserve_session(
        &self,
        token: [u8; SESSION_TOKEN_LEN],
        peer_a_fp: [u8; FINGERPRINT_LEN],
        peer_b_fp: [u8; FINGERPRINT_LEN],
    ) -> Result<(), RelayError> {
        let mut state = self.state.lock().await;
        state.reserve(token, peer_a_fp, peer_b_fp)
    }

    /// Visible bytes-forwarded counter, for diagnostics.
    pub async fn bytes_forwarded(&self) -> u64 {
        self.state.lock().await.bytes_forwarded
    }
}

async fn forward_loop(socket: UdpSocket, state: Arc<Mutex<RelayState>>, bandwidth_cap_bps: u64) {
    let mut buf = vec![0u8; RECV_BUF_BYTES];
    let mut bucket_tokens: f64 = bandwidth_cap_bps as f64;
    let mut bucket_last = Instant::now();
    loop {
        let (len, src) = match socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                warn!("relay: recv_from failed: {e}");
                continue;
            }
        };
        if len == RECV_BUF_BYTES {
            warn!("relay: received {len}-byte datagram filling the entire buffer — possible truncation");
        }

        // Top up the token bucket only when a cap is set. Burst = 0.5s of cap.
        if bandwidth_cap_bps > 0 {
            let now = Instant::now();
            let elapsed = now.duration_since(bucket_last).as_secs_f64();
            bucket_last = now;
            bucket_tokens = (bucket_tokens + elapsed * bandwidth_cap_bps as f64)
                .min(bandwidth_cap_bps as f64 * 0.5);
            if (len as f64) > bucket_tokens {
                debug!("relay: rate-capped (dropping {len} byte packet from {src})");
                continue;
            }
            bucket_tokens -= len as f64;
        }

        let packet = &buf[..len];
        let mut state_guard = state.lock().await;
        let now = Instant::now();

        if let Some(token) = state_guard.addr_to_token.get(&src).copied() {
            // Already paired. Forward to the partner.
            let Some(session) = state_guard.sessions.get_mut(&token) else {
                continue;
            };
            session.last_active = now;
            let dest = match (session.peer_a, session.peer_b) {
                (Some(a), Some(b)) if src == a.addr => Some(b.addr),
                (Some(a), Some(b)) if src == b.addr => Some(a.addr),
                _ => None,
            };
            if let Some(dest) = dest {
                state_guard.bytes_forwarded += len as u64;
                drop(state_guard);
                if let Err(e) = socket.send_to(packet, dest).await {
                    debug!("relay: send_to {dest} failed: {e}");
                }
            }
            continue;
        }

        // Not paired yet — must be a hello.
        let Some(hello) = RelayHello::try_decode(packet) else {
            debug!("relay: dropping unsolicited {len} bytes from {src}");
            continue;
        };

        // Take the session out of the map for a scoped mutation, then
        // re-insert. Avoids two simultaneous mutable borrows of `state_guard`.
        let Some(mut session) = state_guard.sessions.remove(&hello.token) else {
            debug!("relay: hello with unknown token from {src}");
            continue;
        };

        // Pre-bound slot lookup. `reserve_session` rejected identical
        // fingerprints upfront, so each fingerprint maps to exactly
        // one slot here.
        let assigned_slot = if hello.fingerprint == session.peer_a_expected_fp {
            if session.peer_a.is_some() {
                debug!("relay: duplicate hello for slot A from {src}");
                state_guard.sessions.insert(hello.token, session);
                continue;
            }
            session.peer_a = Some(PeerState {
                addr: src,
                fingerprint: hello.fingerprint,
            });
            "A"
        } else if hello.fingerprint == session.peer_b_expected_fp {
            if session.peer_b.is_some() {
                debug!("relay: duplicate hello for slot B from {src}");
                state_guard.sessions.insert(hello.token, session);
                continue;
            }
            session.peer_b = Some(PeerState {
                addr: src,
                fingerprint: hello.fingerprint,
            });
            "B"
        } else {
            debug!("relay: hello with unknown fingerprint from {src}");
            state_guard.sessions.insert(hello.token, session);
            continue;
        };
        session.last_active = now;
        let ready = session.peer_a.is_some() as u8 + session.peer_b.is_some() as u8;
        state_guard.sessions.insert(hello.token, session);
        state_guard.addr_to_token.insert(src, hello.token);
        info!("relay: peer joined session (slot {assigned_slot}, {ready} of 2 ready)",);
    }
}

#[derive(Debug, Error)]
pub enum RelayError {
    #[error("relay bind: {0}")]
    Bind(std::io::Error),
    #[error("relay refused session: both peers share the same fingerprint")]
    DuplicateFingerprint,
}

/// Background task: periodically scan for idle sessions and evict
/// them. Keeps the per-packet forward path off the linear scan.
async fn idle_sweep_loop(state: Arc<Mutex<RelayState>>) {
    loop {
        tokio::time::sleep(IDLE_SWEEP_INTERVAL).await;
        let mut guard = state.lock().await;
        guard.evict_idle(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_roundtrip() {
        let h = RelayHello {
            token: [0x42; SESSION_TOKEN_LEN],
            fingerprint: [0xCD; FINGERPRINT_LEN],
        };
        let enc = h.encode();
        let dec = RelayHello::try_decode(&enc).unwrap();
        assert_eq!(dec.token, h.token);
        assert_eq!(dec.fingerprint, h.fingerprint);
    }

    #[test]
    fn hello_rejects_bad_magic() {
        let mut enc = RelayHello {
            token: [0; SESSION_TOKEN_LEN],
            fingerprint: [0; FINGERPRINT_LEN],
        }
        .encode();
        enc[0] = b'X';
        assert!(RelayHello::try_decode(&enc).is_none());
    }

    #[test]
    fn hello_rejects_short() {
        let bytes = b"P2RZ";
        assert!(RelayHello::try_decode(bytes).is_none());
    }
}
