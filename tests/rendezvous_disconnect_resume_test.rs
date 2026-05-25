//! End-to-end test: rendezvous pairing → first sender closes → receiver
//! re-pairs through the same rendezvous → second sender uses
//! `handle_resume` with `--rendezvous` → destination matches source.
//!
//! This test exists to prevent two structural regressions that landed
//! before any test caught them:
//!
//! 1. **Receiver re-pair under rendezvous.** Post-rendezvous, the QUIC
//!    role (initiator vs responder) is decided by a UUID compare; the
//!    receiver wins only ~half the time, so `session.reaccept()` is
//!    structurally wrong half the time. The fix re-pairs through the
//!    rendezvous on disconnect. Here we drive the same receiver instance
//!    through TWO consecutive pairings — if `reaccept()` were still on
//!    the disconnect path, the second pairing would fail with
//!    "reaccept() is only valid for responder sessions" half the time.
//!
//! 2. **Resume over rendezvous.** The original `resume` CLI only accepted
//!    `--to <ip:port>`, making cross-NAT resume impossible. The fix
//!    flattens `SessionParams` into the `resume` command; phase 2 of
//!    this test calls `handle_resume` with `--rendezvous` + `--code` so
//!    a regression would surface as a CLI-parse failure or a
//!    session-establish failure.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tokio::time::{sleep, timeout};

use p2p_cli::cli::{SessionParams, TransferParams};
use p2p_core::{
    protocol::{ConfigMessage, FileMetadata},
    transfer_folder::FolderTransferState,
    Uuid,
};
use p2p_rendezvous::Server;

const PAIRING_CODE: &str = "RZRTEST";
const PAYLOAD_SIZE: usize = 1_048_576; // 1 MiB
const PHASE_DEADLINE: Duration = Duration::from_secs(45);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receiver_re_pairs_after_sender_disconnect_and_resume_uses_rendezvous() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let dirs = Dirs::lay_out(tmp.path()).await;
    let payloads = Payloads::create(&dirs).await;

    let rzv_addr = start_local_rendezvous().await;

    // The receiver instance must survive the first sender disconnecting
    // and accept a second sender that arrives through resume. Both
    // pairings are through the same rendezvous + code.
    let receiver = spawn_receiver(rzv_addr, &dirs);

    // PHASE 1 — fresh send via rendezvous. Completes naturally. When the
    // sender exits, the receiver's QUIC connection closes; this is the
    // point at which the receive loop must successfully re-pair through
    // the rendezvous (and not call `reaccept()`).
    phase1_send_file(rzv_addr, &dirs, &payloads.a).await;
    wait_until_file_at(&dirs.dst, &payloads.a.name, PAYLOAD_SIZE).await;
    assert_file_matches(&payloads.a, &dirs.dst.join(&payloads.a.name)).await;

    // PHASE 2 — resume via rendezvous. We synthesise a fresh state file
    // describing a not-yet-started transfer of file B, then drive
    // `handle_resume` with `--rendezvous` + `--code`. Pre-fix this would
    // fail at CLI signature or session establish; post-fix it pairs
    // through the rendezvous (the receiver is now in re-pair after
    // phase 1) and transfers file B.
    let resume_id = synthesize_state_for_resume(&dirs, &payloads.b).await;
    phase2_resume_file(rzv_addr, &dirs, &payloads.b, resume_id).await;
    wait_until_file_at(&dirs.dst, &payloads.b.name, PAYLOAD_SIZE).await;
    assert_file_matches(&payloads.b, &dirs.dst.join(&payloads.b.name)).await;

    receiver.abort();
}

// ---- harness ----------------------------------------------------------------

struct Dirs {
    src: PathBuf,
    dst: PathBuf,
    state: PathBuf,
    receiver_identity: PathBuf,
    sender_identity: PathBuf,
}

impl Dirs {
    async fn lay_out(root: &Path) -> Self {
        let dirs = Self {
            src: root.join("src"),
            dst: root.join("dst"),
            state: root.join("state"),
            receiver_identity: root.join("ident-receiver"),
            sender_identity: root.join("ident-sender"),
        };
        for p in [
            &dirs.src,
            &dirs.dst,
            &dirs.state,
            &dirs.receiver_identity,
            &dirs.sender_identity,
        ] {
            tokio::fs::create_dir_all(p).await.expect("mkdir");
        }
        dirs
    }
}

/// A single source file's name, on-disk path, and SHA-256 — small bundle
/// so the test body doesn't juggle three parallel variables per payload.
struct Payload {
    name: String,
    path: PathBuf,
    sha: [u8; 32],
}

struct Payloads {
    a: Payload,
    b: Payload,
}

impl Payloads {
    async fn create(dirs: &Dirs) -> Self {
        Self {
            a: write_random_payload(&dirs.src, "file_a.bin", 0xA1A1A1A1).await,
            b: write_random_payload(&dirs.src, "file_b.bin", 0xB2B2B2B2).await,
        }
    }
}

async fn start_local_rendezvous() -> SocketAddr {
    let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let server = Server::bind(bind).await.expect("rendezvous bind");
    let addr = server.local_addr().expect("rendezvous local addr");
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    addr
}

fn rendezvous_session_params(rzv_addr: SocketAddr) -> SessionParams {
    SessionParams {
        role: None,
        peer: None,
        peer_fingerprint: None,
        port: 0,
        discover: false,
        rendezvous: Some(rzv_addr.to_string()),
        code: Some(PAIRING_CODE.into()),
        force_relay: false,
    }
}

fn transfer_params_no_compression() -> TransferParams {
    TransferParams {
        compress: false, // random payload is incompressible; skip the work
        compress_level: 3,
        adaptive: true,
        chunk_size: 1024,          // KB → 1 MiB chunks → one chunk per file
        max_speed: 0,              // unlimited; localhost is fast
        max_reconnect_attempts: 0, // don't auto-reconnect across the loop
    }
}

fn spawn_receiver(rzv_addr: SocketAddr, dirs: &Dirs) -> tokio::task::JoinHandle<()> {
    let params = rendezvous_session_params(rzv_addr);
    let output = dirs.dst.clone();
    let identity_dir = dirs.receiver_identity.clone();
    tokio::spawn(async move {
        // Auto-accept so the y/N prompt doesn't block the test.
        // `handle_receive` runs an infinite loop; it returns only on a
        // fatal (non-disconnect) error or when the task is aborted.
        let _ = p2p_cli::receive::handle_receive(output, true, params, Some(identity_dir)).await;
    })
}

async fn phase1_send_file(rzv_addr: SocketAddr, dirs: &Dirs, payload: &Payload) {
    let params = rendezvous_session_params(rzv_addr);
    let transfer = transfer_params_no_compression();
    let identity_dir = dirs.sender_identity.clone();
    let result = timeout(
        PHASE_DEADLINE,
        p2p_cli::send::handle_send(
            payload.path.clone(),
            Some(dirs.state.clone()),
            params,
            transfer,
            Some(identity_dir),
        ),
    )
    .await
    .expect("phase 1 send timed out");
    result.expect("phase 1 send failed");
}

async fn phase2_resume_file(
    rzv_addr: SocketAddr,
    dirs: &Dirs,
    payload: &Payload,
    transfer_id: Uuid,
) {
    let params = rendezvous_session_params(rzv_addr);
    let identity_dir = dirs.sender_identity.clone();
    let result = timeout(
        PHASE_DEADLINE,
        p2p_cli::resume::handle_resume(
            transfer_id.to_string(),
            payload.path.clone(),
            Some(dirs.state.clone()),
            0,
            params,
            Some(identity_dir),
        ),
    )
    .await
    .expect("phase 2 resume timed out");
    result.expect("phase 2 resume failed");
}

/// Build a `FolderTransferState` describing a not-yet-started transfer of
/// `payload`, save it as `transfer_<uuid>.json` in `dirs.state`, and
/// return the transfer id. `handle_resume` will load this file, see
/// `completed_files` is empty + `file_chunks` is empty, and stream the
/// whole payload — exactly the same wire path a real "resume from
/// scratch" would take.
async fn synthesize_state_for_resume(dirs: &Dirs, payload: &Payload) -> Uuid {
    let transfer_id = Uuid::new_v4();
    let state = FolderTransferState::new(
        transfer_id,
        "src".to_string(),
        vec![FileMetadata {
            path: payload.name.clone(),
            size: PAYLOAD_SIZE as u64,
            modified: 0,
            checksum: [0u8; 32],
        }],
        &ConfigMessage::default(),
    );
    let state_path = dirs.state.join(format!("transfer_{transfer_id}.json"));
    state
        .save_to_file(&state_path)
        .await
        .expect("save synthetic state");
    transfer_id
}

// ---- payload generation + verification --------------------------------------

async fn write_random_payload(dir: &Path, name: &str, seed: u64) -> Payload {
    let mut buf = vec![0u8; PAYLOAD_SIZE];
    fill_pseudo_random(&mut buf, seed);
    let path = dir.join(name);
    tokio::fs::write(&path, &buf).await.expect("write payload");
    let sha = Sha256::digest(&buf).into();
    Payload {
        name: name.to_string(),
        path,
        sha,
    }
}

/// LCG fill — not cryptographic, but produces incompressible-enough bytes
/// that no compression path can short-circuit the transfer.
fn fill_pseudo_random(buf: &mut [u8], seed: u64) {
    let mut x = seed.wrapping_mul(0x9E3779B97F4A7C15);
    for byte in buf.iter_mut() {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *byte = (x >> 56) as u8;
    }
}

async fn wait_until_file_at(dir: &Path, name: &str, expected_size: usize) {
    let target = dir.join(name);
    let deadline = Instant::now() + PHASE_DEADLINE;
    loop {
        match tokio::fs::metadata(&target).await {
            Ok(m) if m.len() as usize == expected_size => return,
            _ => {}
        }
        if Instant::now() >= deadline {
            panic!(
                "{} never reached {} bytes within {:?}",
                target.display(),
                expected_size,
                PHASE_DEADLINE
            );
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn assert_file_matches(expected: &Payload, actual: &Path) {
    let bytes = tokio::fs::read(actual).await.expect("read destination");
    let got: [u8; 32] = Sha256::digest(&bytes).into();
    assert_eq!(
        got,
        expected.sha,
        "destination {} did not match source {} (SHA-256)",
        actual.display(),
        expected.path.display()
    );
}
