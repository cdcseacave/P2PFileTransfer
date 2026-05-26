//! QUIC transport — the only transport in this codebase.
//!
//! The shape of a peer interaction:
//!
//! * One [`QuicEndpoint`] per local UDP socket. Configured up-front to act
//!   as both a server (accepting inbound) and a client (initiating outbound)
//!   on the same socket. This is what makes hole punching work: both peers
//!   construct an endpoint and race [`connect`](QuicEndpoint::connect) /
//!   [`accept`](QuicEndpoint::accept) — whichever direction wins is fine.
//! * One [`QuicConnection`] per peer. It holds the [`quinn::Connection`]
//!   plus an open *bidirectional* control stream that carries
//!   length-prefixed [`Message`] frames (the existing
//!   [`crate::network::framing`] format runs unchanged over QUIC streams).
//! * File chunks travel on per-chunk *unidirectional* streams. Each chunk
//!   stream is prefixed with `u64` (little-endian) chunk index, then the
//!   raw (optionally compressed) payload bytes; the sender finishes the
//!   stream when the chunk is done. The receiver loops on
//!   [`QuicConnection::accept_uni`], reads the index, and writes the
//!   payload to the destination file at the matching offset. QUIC's
//!   per-stream flow control and packet-level retransmission replace the
//!   sliding window + ACK + CRC machinery the old TCP transport needed.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{
    ClientConfig, Endpoint, EndpointConfig, RecvStream, SendStream, ServerConfig, TokioRuntime,
    TransportConfig, VarInt,
};
use tracing::debug;

use crate::error::{Error, Result};
use crate::identity::{Fingerprint, Identity};
use crate::network::framing;
use crate::protocol::Message;
use crate::tls;

/// Application-layer keepalive: keep punched NAT mappings alive even if
/// the higher-level protocol is momentarily idle.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Maximum idle before quinn tears down a connection.
const MAX_IDLE_TIMEOUT_SECS: u64 = 60;

/// Per-stream receive window. Sized to comfortably hold one in-flight
/// chunk at the new 1 MiB default with room for the next one to start
/// streaming before the previous one drains.
const STREAM_RECEIVE_WINDOW: u32 = 8 * 1024 * 1024;

/// Connection-level receive window. Sized for high-BDP links (gigabit
/// at ~30 ms RTT is ~3.75 MB; 64 MiB leaves ample headroom and is
/// well below the 2^62 VarInt limit).
const RECEIVE_WINDOW: u32 = 64 * 1024 * 1024;

/// A QUIC endpoint bound to one UDP socket. Acts as both client and server.
///
/// Constructed in one of two ways:
///
/// * [`QuicEndpoint::bind`] — convenience, binds a fresh UDP socket at the
///   given address. Used for direct LAN/`--peer` connections.
/// * [`QuicEndpoint::from_socket`] — takes a pre-bound `std::net::UdpSocket`.
///   The traversal flow needs this because it must run STUN on the socket
///   first so the discovered public mapping refers to the socket QUIC will
///   then own.
pub struct QuicEndpoint {
    endpoint: Endpoint,
    identity: Arc<Identity>,
}

impl QuicEndpoint {
    /// Bind a fresh UDP socket at `bind_addr` and construct an endpoint
    /// configured as both server and (latent) client.
    pub fn bind(bind_addr: SocketAddr, identity: Arc<Identity>) -> Result<Self> {
        let socket = std::net::UdpSocket::bind(bind_addr).map_err(Error::Network)?;
        Self::from_socket(socket, identity)
    }

    /// Construct an endpoint from a pre-bound socket. The socket must be
    /// idle (no in-flight reads) when passed in — quinn takes ownership.
    pub fn from_socket(socket: std::net::UdpSocket, identity: Arc<Identity>) -> Result<Self> {
        tls::install_default_crypto_provider();
        socket.set_nonblocking(true).map_err(Error::Network)?;

        let server_crypto = tls::server_config(&identity)?;
        let quic_server_crypto = QuicServerConfig::try_from(server_crypto.as_ref().clone())
            .map_err(|e| Error::Tls(format!("QuicServerConfig: {e}")))?;
        let mut server_cfg = ServerConfig::with_crypto(Arc::new(quic_server_crypto));
        server_cfg.transport_config(Arc::new(transport_config()));

        let endpoint = Endpoint::new(
            EndpointConfig::default(),
            Some(server_cfg),
            socket,
            Arc::new(TokioRuntime),
        )
        .map_err(|e| Error::Quic(format!("endpoint construct: {e}")))?;

        Ok(Self { endpoint, identity })
    }

    /// Local socket address the endpoint is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint.local_addr().map_err(Error::Network)
    }

    /// Initiate a connection to `peer_addr`, pinning the peer's cert
    /// fingerprint. `server_name` is required by rustls but ignored by our
    /// pinning verifier; pass `"p2p-transfer"`.
    pub async fn connect(
        &self,
        peer_addr: SocketAddr,
        peer_fingerprint: Fingerprint,
    ) -> Result<QuicConnection> {
        let client_crypto = tls::client_config_pinning(peer_fingerprint, &self.identity)?;
        let quic_client_crypto = QuicClientConfig::try_from(client_crypto.as_ref().clone())
            .map_err(|e| Error::Tls(format!("QuicClientConfig: {e}")))?;
        let mut client_cfg = ClientConfig::new(Arc::new(quic_client_crypto));
        client_cfg.transport_config(Arc::new(transport_config()));

        let connecting = self
            .endpoint
            .connect_with(client_cfg, peer_addr, "p2p-transfer")
            .map_err(|e| Error::Quic(format!("connect_with: {e}")))?;
        let connection = connecting
            .await
            .map_err(|e| Error::Quic(format!("handshake: {e}")))?;
        debug!(remote = %connection.remote_address(), "QUIC outbound connected");
        QuicConnection::open_control_initiator(connection).await
    }

    /// Accept the next inbound connection. The peer's cert is **not** pinned
    /// here — the application-level HELLO message carries the claimed
    /// fingerprint and the caller is responsible for cross-checking it
    /// against the actual presented cert via
    /// [`QuicConnection::peer_fingerprint`].
    pub async fn accept(&self) -> Result<QuicConnection> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| Error::Quic("endpoint closed".to_string()))?;
        let connection = incoming
            .await
            .map_err(|e| Error::Quic(format!("inbound handshake: {e}")))?;
        debug!(remote = %connection.remote_address(), "QUIC inbound accepted");
        QuicConnection::open_control_responder(connection).await
    }

    /// Initiate a graceful close; flushes pending streams up to `timeout`.
    pub async fn close(&self) {
        self.endpoint.close(0u32.into(), b"shutdown");
        self.endpoint.wait_idle().await;
    }
}

/// A live QUIC connection to one peer. Owns the bidirectional control
/// stream; chunk streams are opened/accepted on demand via [`open_uni`] /
/// [`accept_uni`].
pub struct QuicConnection {
    connection: quinn::Connection,
    control_send: SendStream,
    control_recv: RecvStream,
}

impl QuicConnection {
    /// Initiator side: open the control stream and prime it with the
    /// `PROTOCOL_MAGIC` so the peer's `accept_bi` unblocks immediately.
    /// quinn's `open_bi` itself is a local operation — the responder's
    /// `accept_bi` only resolves once the initiator writes *something*
    /// to the stream, so the magic doubles as the wake-up.
    async fn open_control_initiator(connection: quinn::Connection) -> Result<Self> {
        let (mut control_send, control_recv) = connection
            .open_bi()
            .await
            .map_err(|e| Error::Quic(format!("open_bi: {e}")))?;
        // quinn::SendStream has an inherent write_all (not via AsyncWriteExt).
        control_send
            .write_all(&crate::PROTOCOL_MAGIC)
            .await
            .map_err(|e| Error::Quic(format!("control stream prime: {e}")))?;
        Ok(Self {
            connection,
            control_send,
            control_recv,
        })
    }

    /// Responder side: accept the control stream the initiator opened
    /// and consume the priming magic.
    async fn open_control_responder(connection: quinn::Connection) -> Result<Self> {
        let (control_send, mut control_recv) = connection
            .accept_bi()
            .await
            .map_err(|e| Error::Quic(format!("accept_bi: {e}")))?;
        let mut magic = [0u8; 4];
        // quinn::RecvStream has an inherent read_exact that returns
        // `Result<(), ReadExactError>`.
        control_recv
            .read_exact(&mut magic)
            .await
            .map_err(|e| Error::Quic(format!("control stream prime read: {e}")))?;
        if magic != crate::PROTOCOL_MAGIC {
            return Err(Error::Protocol(format!(
                "control stream priming magic mismatch: got {magic:?}",
            )));
        }
        Ok(Self {
            connection,
            control_send,
            control_recv,
        })
    }

    /// Remote socket address (post-NAT, as observed by the local kernel).
    pub fn peer_addr(&self) -> SocketAddr {
        self.connection.remote_address()
    }

    /// SHA-256 fingerprint of the peer's certificate as presented during the
    /// TLS handshake. Used to cross-check the fingerprint claimed in the
    /// application HELLO message.
    pub fn peer_fingerprint(&self) -> Option<Fingerprint> {
        let identity = self.connection.peer_identity()?;
        let certs = identity
            .downcast::<Vec<rustls_pki_types::CertificateDer<'static>>>()
            .ok()?;
        let first = certs.first()?;
        Some(crate::identity::fingerprint_of(first))
    }

    /// Write a control-plane message on the bidirectional control stream.
    pub async fn send_message(&mut self, msg: &Message) -> Result<()> {
        framing::write_message(&mut self.control_send, msg).await
    }

    /// Read the next control-plane message from the bidirectional control stream.
    pub async fn recv_message(&mut self) -> Result<Message> {
        framing::read_message(&mut self.control_recv).await
    }

    /// Open a new unidirectional stream for a single chunk payload.
    pub async fn open_uni(&self) -> Result<SendStream> {
        self.connection
            .open_uni()
            .await
            .map_err(|e| Error::Quic(format!("open_uni: {e}")))
    }

    /// Accept the next unidirectional stream the peer opened.
    pub async fn accept_uni(&self) -> Result<RecvStream> {
        self.connection
            .accept_uni()
            .await
            .map_err(|e| Error::Quic(format!("accept_uni: {e}")))
    }

    /// Close the connection with a normal-shutdown error code.
    pub async fn close(&mut self) -> Result<()> {
        // Flush the control stream so any in-flight messages get acked
        // before the connection tears down.
        let _ = self.control_send.finish();
        self.connection.close(0u32.into(), b"bye");
        Ok(())
    }
}

fn transport_config() -> TransportConfig {
    let mut t = TransportConfig::default();
    t.keep_alive_interval(Some(KEEPALIVE_INTERVAL));
    t.max_idle_timeout(Some(
        Duration::from_secs(MAX_IDLE_TIMEOUT_SECS)
            .try_into()
            .expect("idle timeout fits"),
    ));
    t.stream_receive_window(VarInt::from_u32(STREAM_RECEIVE_WINDOW));
    t.receive_window(VarInt::from_u32(RECEIVE_WINDOW));
    t.send_window(RECEIVE_WINDOW as u64);
    t
}

/// Convenience: bind a wildcard IPv4 endpoint on `port` (0 = ephemeral).
pub fn bind_wildcard(port: u16, identity: Arc<Identity>) -> Result<QuicEndpoint> {
    QuicEndpoint::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
        identity,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::HelloMessage;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn loopback_send_and_receive_control_message() {
        let identity = Arc::new(Identity::generate().unwrap());
        let server = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            identity.clone(),
        )
        .unwrap();
        let server_addr = server.local_addr().unwrap();
        let expected_fp = identity.fingerprint();

        let server_task = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let msg = conn.recv_message().await.unwrap();
            // Echo it back.
            conn.send_message(&msg).await.unwrap();
            // Hold the connection until client closes.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let client_identity = Arc::new(Identity::generate().unwrap());
        let client = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            client_identity,
        )
        .unwrap();
        let mut conn = client.connect(server_addr, expected_fp).await.unwrap();

        let msg = Message::Hello(HelloMessage {
            protocol_version: crate::PROTOCOL_VERSION,
            min_version: crate::MIN_PROTOCOL_VERSION,
            device_id: Uuid::new_v4(),
            cert_fingerprint: [0u8; 32],
        });
        conn.send_message(&msg).await.unwrap();
        let echoed = conn.recv_message().await.unwrap();
        match (msg, echoed) {
            (Message::Hello(a), Message::Hello(b)) => assert_eq!(a.device_id, b.device_id),
            _ => panic!("unexpected message types"),
        }

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn unidirectional_stream_carries_chunk_data() {
        let identity = Arc::new(Identity::generate().unwrap());
        let server = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            identity.clone(),
        )
        .unwrap();
        let server_addr = server.local_addr().unwrap();
        let fp = identity.fingerprint();

        let payload = vec![0xAB; 4096];
        let payload_clone = payload.clone();

        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let server_task = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            // Real usage always writes a control message right after connect;
            // mirror that here so accept_bi unblocks before accept_uni runs.
            let _ = conn.recv_message().await.unwrap();
            let mut stream = conn.accept_uni().await.unwrap();
            let buf = stream.read_to_end(64 * 1024).await.unwrap_or_default();
            // Hold the connection until the test signals it's done; otherwise
            // dropping `conn` here closes the connection before the client
            // has finished reading any in-flight ACKs.
            let _ = done_rx.await;
            buf
        });

        let client = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            Arc::new(Identity::generate().unwrap()),
        )
        .unwrap();
        let mut conn = client.connect(server_addr, fp).await.unwrap();
        conn.send_message(&Message::Ping).await.unwrap();
        let mut stream = conn.open_uni().await.unwrap();
        stream.write_all(&payload_clone).await.unwrap();
        stream.finish().ok();
        // Wait for the stream to drain before signalling the server to close.
        let _ = stream.stopped().await;
        done_tx.send(()).ok();
        let received = server_task.await.unwrap();
        assert_eq!(received, payload_clone);
    }
}
