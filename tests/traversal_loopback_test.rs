//! Loopback traversal smoke test.
//!
//! Drives the **rendezvous** + **race_connect_and_accept** primitives
//! directly (bypassing real STUN, which would require an external
//! server). Two peers register with the same code at a locally-bound
//! `p2p-rendezvous::Server`, exchange their local QUIC endpoints as
//! "public endpoints", and then race connect/accept. This proves the
//! plumbing works end-to-end against localhost.
//!
//! Cross-NAT validation requires the `tests/traversal/` netns harness
//! and real-world laptop pairing, which run separately on Linux.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;

use p2p_core::{
    identity::Identity,
    network::quic::QuicEndpoint,
    traversal::punch::race_connect_and_accept,
};
use p2p_rendezvous::{
    client::register as rendezvous_register,
    protocol::{RegisterRequest, PROTOCOL_VERSION as RZV_PROTO},
    Server,
};

#[tokio::test]
async fn loopback_pair_via_rendezvous_and_punch() {
    // 1. Stand up the rendezvous server.
    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = Server::bind(bind).await.expect("rendezvous bind");
    let rendezvous_addr = server.local_addr().expect("rendezvous addr");
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // 2. Each peer constructs its own QUIC endpoint up-front and uses
    //    the local address as its "public" endpoint for the test. In
    //    production STUN would discover the post-NAT address; here we
    //    skip STUN because there's no NAT to discover.
    let id_a = Arc::new(Identity::generate().unwrap());
    let id_b = Arc::new(Identity::generate().unwrap());

    let ep_a = QuicEndpoint::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        id_a.clone(),
    )
    .expect("endpoint A");
    let ep_b = QuicEndpoint::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        id_b.clone(),
    )
    .expect("endpoint B");

    let addr_a = ep_a.local_addr().unwrap();
    let addr_b = ep_b.local_addr().unwrap();
    let fp_a = id_a.fingerprint();
    let fp_b = id_b.fingerprint();

    // 3. Both peers register at the rendezvous with the same code,
    //    handing in each side's QUIC endpoint as the public address.
    let code = "LBPAIR".to_string();
    let req_a = RegisterRequest {
        protocol_version: RZV_PROTO,
        code: code.clone(),
        public_endpoint: addr_a,
        cert_fingerprint: fp_a,
        device_id: [0xA1; 16],
        want_relay: false,
    };
    let req_b = RegisterRequest {
        protocol_version: RZV_PROTO,
        code: code.clone(),
        public_endpoint: addr_b,
        cert_fingerprint: fp_b,
        device_id: [0xB2; 16],
        want_relay: false,
    };

    let a_task = tokio::spawn(rendezvous_register(rendezvous_addr, req_a));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let b_task = tokio::spawn(rendezvous_register(rendezvous_addr, req_b));

    let peer_for_a = a_task.await.unwrap().expect("A got match");
    let peer_for_b = b_task.await.unwrap().expect("B got match");
    assert_eq!(peer_for_a.endpoint, addr_b);
    assert_eq!(peer_for_b.endpoint, addr_a);

    // 4. Race connect/accept on each side.
    let conn_a_fut = race_connect_and_accept(&ep_a, peer_for_a.endpoint, peer_for_a.fingerprint);
    let conn_b_fut = race_connect_and_accept(&ep_b, peer_for_b.endpoint, peer_for_b.fingerprint);

    let (conn_a, conn_b) = timeout(Duration::from_secs(15), async {
        tokio::try_join!(conn_a_fut, conn_b_fut)
    })
    .await
    .expect("race did not complete within timeout")
    .expect("connect/accept on both sides");

    assert_eq!(conn_a.peer_addr(), addr_b);
    assert_eq!(conn_b.peer_addr(), addr_a);
}
