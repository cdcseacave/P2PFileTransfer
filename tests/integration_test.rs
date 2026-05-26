//! Workspace-level integration smoke test.
//!
//! Spins up a `P2PSession` on each side of a QUIC loopback connection and
//! verifies the handshake completes and the cert fingerprint pin holds.
//! Per-module unit tests cover the detailed protocol behavior; this file
//! exists so one failing workspace-level test surfaces "the whole pipeline
//! doesn't even spin up."

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use p2p_core::{
    identity::Identity, network::quic::QuicEndpoint, protocol::ConfigMessage, session::P2PSession,
    Uuid,
};
use tokio::time::timeout;

#[tokio::test]
async fn full_session_handshake_over_quic() {
    let server_identity = Arc::new(Identity::generate().unwrap());
    let server_fp = server_identity.fingerprint();

    // Bind explicitly so we can publish the ephemeral port to the client.
    let endpoint = QuicEndpoint::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        server_identity.clone(),
    )
    .unwrap();
    let server_addr = endpoint.local_addr().unwrap();
    drop(endpoint);

    // Server task: bind a fresh endpoint on a known port and run accept().
    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let server_id_for_task = server_identity.clone();
    let server_task = tokio::spawn(async move {
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let ep = QuicEndpoint::bind(bind, server_id_for_task.clone()).unwrap();
        addr_tx.send(ep.local_addr().unwrap()).ok();
        // P2PSession::accept re-binds; emulate it inline using ep so we
        // don't race the port number.
        let mut conn = ep.accept().await.unwrap();
        let handshake =
            p2p_core::handshake::HandshakeServer::new(Uuid::new_v4(), &server_id_for_task);
        let result = handshake.perform_handshake(&mut conn).await.unwrap();
        // Hold the connection until the test signals the client is done
        // reading the last handshake message; real P2PSession::accept holds
        // it for the session's lifetime.
        let _ = done_rx.await;
        result
    });

    let real_addr = addr_rx.await.unwrap();
    let _ = server_addr; // earlier ephemeral; unused beyond proving bind works

    let client_identity = Arc::new(Identity::generate().unwrap());
    let session = timeout(
        Duration::from_secs(5),
        P2PSession::connect(
            real_addr,
            server_fp,
            client_identity,
            Uuid::new_v4(),
            ConfigMessage::default(),
        ),
    )
    .await
    .expect("connect timed out")
    .expect("connect failed");

    done_tx.send(()).ok();
    let _server_handshake = server_task.await.expect("server task panicked");

    assert_eq!(session.peer_fingerprint(), server_fp);
}
