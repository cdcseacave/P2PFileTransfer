//! Phase 2 loopback test for the QUIC relay fallback.
//!
//! Stands up a rendezvous + relay on localhost, has two peers register
//! with `want_relay = true`, validates they each receive a
//! `RelayMatch`, sends their hellos to the relay, then races
//! `QuicEndpoint::connect`/`accept` with the **relay's** address as
//! the apparent peer endpoint. Because both peers' QUIC packets are
//! relayed verbatim, the QUIC TLS handshake terminates end-to-end
//! between the two peers — the relay only forwards bytes.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;

use p2p_core::{
    identity::Identity,
    network::quic::QuicEndpoint,
    traversal::punch::race_connect_and_accept,
    Uuid,
};
use p2p_rendezvous::{
    client::{register_full, MatchOutcome},
    protocol::{RegisterRequest, PROTOCOL_VERSION as RZV_PROTO},
    relay::RelayHello,
    Relay, Server,
};

#[tokio::test]
async fn loopback_pair_via_relay() {
    // Stand up rendezvous + relay on localhost.
    let rzv_bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let relay_bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let relay = Relay::bind(relay_bind, 0).await.expect("relay bind");
    let mut server = Server::bind(rzv_bind).await.expect("rendezvous bind");
    server.attach_relay(relay.clone());
    let rzv_addr = server.local_addr().expect("rzv addr");
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Identities + UDP sockets for each peer. These sockets are what
    // `quinn` will own; we send the RelayHello on them first.
    let id_a = Arc::new(Identity::generate().unwrap());
    let id_b = Arc::new(Identity::generate().unwrap());
    let fp_a = id_a.fingerprint();
    let fp_b = id_b.fingerprint();

    let sock_a = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let sock_b = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();

    let local_a = sock_a.local_addr().unwrap();
    let local_b = sock_b.local_addr().unwrap();

    // Both peers register with want_relay=true so the rendezvous
    // reserves a relay session for them.
    let code = "RLPAIR".to_string();
    let req_a = RegisterRequest {
        protocol_version: RZV_PROTO,
        code: code.clone(),
        public_endpoint: local_a,
        cert_fingerprint: fp_a,
        device_id: [0xA1; 16],
        want_relay: true,
    };
    let req_b = RegisterRequest {
        protocol_version: RZV_PROTO,
        code: code.clone(),
        public_endpoint: local_b,
        cert_fingerprint: fp_b,
        device_id: [0xB2; 16],
        want_relay: true,
    };

    let a_task = tokio::spawn(register_full(rzv_addr, req_a));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let b_task = tokio::spawn(register_full(rzv_addr, req_b));

    let out_a = a_task.await.unwrap().expect("A got match");
    let out_b = b_task.await.unwrap().expect("B got match");

    let relay_for_a = match out_a {
        MatchOutcome::Relay(r) => r,
        MatchOutcome::Direct(_) => panic!("expected RelayMatch for A"),
    };
    let relay_for_b = match out_b {
        MatchOutcome::Relay(r) => r,
        MatchOutcome::Direct(_) => panic!("expected RelayMatch for B"),
    };
    assert_eq!(relay_for_a.session_token, relay_for_b.session_token);
    assert_eq!(relay_for_a.relay_endpoint, relay_for_b.relay_endpoint);
    assert_eq!(relay_for_a.peer_fingerprint, fp_b);
    assert_eq!(relay_for_b.peer_fingerprint, fp_a);

    // Each peer sends its hello to the relay so the relay records the
    // peer's source address for forwarding.
    for _ in 0..3 {
        sock_a
            .send_to(
                &RelayHello {
                    token: relay_for_a.session_token,
                    fingerprint: fp_a,
                }
                .encode(),
                relay_for_a.relay_endpoint,
            )
            .await
            .unwrap();
        sock_b
            .send_to(
                &RelayHello {
                    token: relay_for_b.session_token,
                    fingerprint: fp_b,
                }
                .encode(),
                relay_for_b.relay_endpoint,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // Hand the sockets to quinn and race connect/accept against the
    // relay's address. The relay forwards QUIC packets verbatim.
    let std_a = sock_a.into_std().unwrap();
    let std_b = sock_b.into_std().unwrap();
    let ep_a = QuicEndpoint::from_socket(std_a, id_a).unwrap();
    let ep_b = QuicEndpoint::from_socket(std_b, id_b).unwrap();

    let our_id_a = Uuid::from_bytes([0xA1; 16]);
    let our_id_b = Uuid::from_bytes([0xB2; 16]);
    let fut_a = race_connect_and_accept(&ep_a, relay_for_a.relay_endpoint, relay_for_a.peer_fingerprint, our_id_a, our_id_b);
    let fut_b = race_connect_and_accept(&ep_b, relay_for_b.relay_endpoint, relay_for_b.peer_fingerprint, our_id_b, our_id_a);

    let (conn_a, conn_b) = timeout(Duration::from_secs(20), async {
        tokio::try_join!(fut_a, fut_b)
    })
    .await
    .expect("relay handshake timed out")
    .expect("connect/accept on both sides");

    assert_eq!(conn_a.peer_addr(), relay_for_a.relay_endpoint);
    assert_eq!(conn_b.peer_addr(), relay_for_b.relay_endpoint);
    // Only the QUIC client side sees the server's cert directly via
    // `peer_identity()` (the server config uses `with_no_client_auth`).
    // A.device_id ([0xA1; 16]) < B.device_id ([0xB2; 16]) so A is the
    // client and observes B's cert; B is the server and observes None.
    // The application-layer HELLO message carries fingerprints both
    // ways for cross-checking — see handshake.rs.
    assert_eq!(conn_a.peer_fingerprint(), Some(fp_b));
    assert_eq!(conn_b.peer_fingerprint(), None);

    let bytes = relay.bytes_forwarded().await;
    assert!(bytes > 0, "relay should have forwarded the QUIC handshake bytes");
}
