//! NAT traversal diagnostic.
//!
//! Two modes:
//!
//! * **STUN-only (default):** queries two STUN servers on the same UDP
//!   socket and reports `Cone` (UDP hole-punching will work) vs
//!   `Symmetric` (relay required).
//! * **Self-loop (`--rendezvous URL`):** stands up two local peers,
//!   registers both at the given rendezvous server with a fresh code,
//!   then races a QUIC handshake between them through the punched path
//!   (or via the relay, if the server offers one). Reports
//!   `direct` / `relay` / `failed` plus latency.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use tokio::net::{lookup_host, UdpSocket};
use tokio::time::timeout;
use tracing::info;

use p2p_core::identity::Identity;
use p2p_core::network::quic::QuicEndpoint;
use p2p_core::traversal::stun::{classify_nat, query, NatClass};
use p2p_core::traversal::{generate_code, punch::race_connect_and_accept};
use p2p_rendezvous::client::{register_full, MatchOutcome};
use p2p_rendezvous::protocol::{RegisterRequest, PROTOCOL_VERSION as RZV_PROTO};
use p2p_rendezvous::relay::RelayHello;

/// Default STUN servers used when the user does not pass `--stun-server`.
const DEFAULT_STUN_SERVERS: &[&str] = &["stun.l.google.com:19302", "stun1.l.google.com:19302"];

pub async fn handle_nat_test(
    stun_server: Option<String>,
    rendezvous: Option<String>,
) -> Result<()> {
    if let Some(rendezvous) = rendezvous {
        run_self_loop_punch(&rendezvous).await
    } else {
        run_stun_only(stun_server).await
    }
}

async fn run_stun_only(stun_server: Option<String>) -> Result<()> {
    info!("Testing NAT traversal (STUN diagnostic)...");

    let servers = match stun_server.as_deref() {
        Some(custom) => {
            info!("  Custom STUN server: {custom}");
            vec![custom.to_string(), DEFAULT_STUN_SERVERS[1].to_string()]
        }
        None => {
            info!(
                "  STUN servers: {} + {}",
                DEFAULT_STUN_SERVERS[0], DEFAULT_STUN_SERVERS[1]
            );
            DEFAULT_STUN_SERVERS.iter().map(|s| s.to_string()).collect()
        }
    };

    let a = resolve_first(&servers[0]).await?;
    let b = resolve_first(&servers[1]).await?;

    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let socket = UdpSocket::bind(bind).await?;
    info!("  Local socket bound to {}", socket.local_addr()?);

    let public = query(&socket, a).await?;
    info!("  Public endpoint (server A): {public}");

    match classify_nat(&socket, a, b).await? {
        NatClass::Cone { public } => {
            info!("Cone NAT detected — UDP hole punching should work.");
            info!("  Public endpoint: {public}");
        }
        NatClass::Symmetric => {
            info!("Symmetric NAT detected — direct UDP hole punching will fail.");
            info!("  Peers behind symmetric NAT need the QUIC relay fallback.");
        }
    }
    Ok(())
}

async fn run_self_loop_punch(rendezvous_host: &str) -> Result<()> {
    info!("Self-loop punch test through rendezvous '{rendezvous_host}'...");

    let with_port = p2p_core::with_default_port(rendezvous_host, p2p_core::DEFAULT_RENDEZVOUS_PORT);
    let rendezvous_addr = resolve_first(&with_port)
        .await
        .with_context(|| format!("resolving rendezvous '{with_port}'"))?;
    info!("  Rendezvous: {rendezvous_addr}");

    // Generate a code; both halves of the self-loop use it.
    let code = generate_code();
    info!("  Pairing code: {code}");

    let id_a = Arc::new(Identity::generate()?);
    let id_b = Arc::new(Identity::generate()?);
    let fp_a = id_a.fingerprint();
    let fp_b = id_b.fingerprint();

    // Bind to LOCALHOST so that local_addr() returns a real connectable
    // destination (binding to 0.0.0.0 leaves it as `0.0.0.0:port`, which
    // is not a valid `connect_with` target on the peer side of the self-loop).
    let sock_a = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let sock_b = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let local_a = sock_a.local_addr()?;
    let local_b = sock_b.local_addr()?;

    let req_a = RegisterRequest {
        protocol_version: RZV_PROTO,
        code: code.clone(),
        public_endpoint: local_a,
        cert_fingerprint: fp_a,
        device_id: [0xA1; 16],
        want_relay: false,
    };
    let req_b = RegisterRequest {
        protocol_version: RZV_PROTO,
        code: code.clone(),
        public_endpoint: local_b,
        cert_fingerprint: fp_b,
        device_id: [0xB2; 16],
        want_relay: false,
    };

    let started = Instant::now();
    let a_task = tokio::spawn(register_full(rendezvous_addr, req_a));
    // Tiny stagger so the rendezvous treats A as the first peer.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let b_task = tokio::spawn(register_full(rendezvous_addr, req_b));

    let out_a = a_task
        .await
        .map_err(|e| anyhow!("A register task: {e}"))?
        .map_err(|e| anyhow!("A register: {e}"))?;
    let out_b = b_task
        .await
        .map_err(|e| anyhow!("B register task: {e}"))?
        .map_err(|e| anyhow!("B register: {e}"))?;

    let (direct_or_relay, peer_a, peer_b) = match (out_a, out_b) {
        (MatchOutcome::Direct(a), MatchOutcome::Direct(b)) => ("direct", a.endpoint, b.endpoint),
        (MatchOutcome::Relay(a), MatchOutcome::Relay(b)) => {
            // Send the hellos so the relay records source addresses.
            let hello_a = RelayHello {
                token: a.session_token,
                fingerprint: fp_a,
            }
            .encode();
            let hello_b = RelayHello {
                token: b.session_token,
                fingerprint: fp_b,
            }
            .encode();
            for _ in 0..3 {
                sock_a.send_to(&hello_a, a.relay_endpoint).await?;
                sock_b.send_to(&hello_b, b.relay_endpoint).await?;
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            ("relay", a.relay_endpoint, b.relay_endpoint)
        }
        _ => {
            return Err(anyhow!(
                "rendezvous returned mixed Direct/Relay outcomes (unsupported)"
            ))
        }
    };

    let std_a = sock_a.into_std()?;
    let std_b = sock_b.into_std()?;
    let ep_a = QuicEndpoint::from_socket(std_a, id_a)?;
    let ep_b = QuicEndpoint::from_socket(std_b, id_b)?;

    let our_a = p2p_core::Uuid::from_bytes([0xA1; 16]);
    let our_b = p2p_core::Uuid::from_bytes([0xB2; 16]);
    let fut_a = race_connect_and_accept(&ep_a, peer_a, fp_b, our_a, our_b);
    let fut_b = race_connect_and_accept(&ep_b, peer_b, fp_a, our_b, our_a);

    let outcome = timeout(Duration::from_secs(30), async {
        tokio::try_join!(fut_a, fut_b)
    })
    .await;

    let elapsed = started.elapsed();
    match outcome {
        Err(_) => {
            info!("Self-loop punch FAILED: timed out after {:?}", elapsed);
            Err(anyhow!("punch timed out"))
        }
        Ok(Err(e)) => {
            info!("Self-loop punch FAILED in {:?}: {e}", elapsed);
            Err(anyhow!("punch failed: {e}"))
        }
        Ok(Ok(_)) => {
            info!(
                "Self-loop punch succeeded ({}) in {:?}",
                direct_or_relay, elapsed,
            );
            Ok(())
        }
    }
}

async fn resolve_first(host_port: &str) -> Result<SocketAddr> {
    lookup_host(host_port)
        .await?
        .next()
        .ok_or_else(|| anyhow!("could not resolve '{host_port}'"))
}
