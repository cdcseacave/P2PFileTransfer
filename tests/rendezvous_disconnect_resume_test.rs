//! End-to-end test: rendezvous pairing → receiver re-pairs after the first
//! sender disconnects → a second `send` of the *same source* auto-resumes a
//! prior incomplete transfer → destination matches source.
//!
//! This guards two structural behaviours:
//!
//! 1. **Receiver re-pair under rendezvous.** Post-rendezvous, the QUIC role
//!    (initiator vs responder) is decided by a UUID compare; the receiver
//!    wins only ~half the time, so `session.reaccept()` is structurally
//!    wrong half the time. The fix re-pairs through the rendezvous on
//!    disconnect. A single long-lived receiver is driven through TWO
//!    consecutive pairings — if `reaccept()` were on the disconnect path it
//!    would fail half the time on the second pair.
//!
//! 2. **Resume folded into `send`.** There is no longer a `resume`
//!    subcommand. Re-running the identical `send` of an interrupted
//!    transfer must auto-detect the prior state (by peer fingerprint + file
//!    list) and continue it. Phase 2 calls `handle_send` again — a
//!    regression would either restart from zero (no "Resuming transfer"
//!    log, prior state file left orphaned) or fail to pair / skip the
//!    already-completed file.
//!
//! The mid-transfer checkpoint is *reconstructed* rather than produced by
//! killing a live transfer: in rendezvous mode the QUIC initiator/responder
//! — and therefore which side's `ConfigMessage` (and bandwidth limit) wins
//! — is decided by a UUID race, so a live transfer's duration is not stable
//! enough to interrupt it deterministically. Seeding the checkpoint keeps
//! the test exercising the real resume path (find_resumable_state →
//! handle_send → receiver skips completed files → rendezvous re-pair)
//! without a timing race.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::time::{sleep, timeout};

use p2p_cli::cli::{SessionParams, TransferParams};
use p2p_core::identity::Identity;
use p2p_core::protocol::ConfigMessage;
use p2p_core::transfer_folder::{enumerate_files, FolderTransferState};
use p2p_core::Uuid;
use p2p_rendezvous::Server;

const PAIRING_CODE: &str = "RZRTEST";
const PAYLOAD_SIZE: usize = 256 * 1024; // 256 KiB per file
const PHASE_DEADLINE: Duration = Duration::from_secs(45);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_send_auto_resumes_on_rerun_over_rendezvous() {
    let logs = install_log_capture();

    let tmp = tempfile::tempdir().expect("tmpdir");
    let dirs = Dirs::lay_out(tmp.path()).await;
    let payloads = Payloads::create(&dirs).await;
    let dst_root = dirs.dst.join("src"); // folder name is preserved on the wire

    // Pre-generate the receiver identity so we can compute the fingerprint
    // the sender will see as its peer — used to stamp the seeded checkpoint.
    // The receiver task loads this same persisted identity on startup.
    let peer_fp = Identity::load_or_generate(Some(&dirs.receiver_identity))
        .expect("receiver identity")
        .fingerprint();

    let rzv_addr = start_local_rendezvous().await;

    // One receiver instance must survive the first sender disconnecting and
    // accept the resuming sender. Both pairings go through the same
    // rendezvous + code.
    let receiver = spawn_receiver(rzv_addr, &dirs);
    sleep(Duration::from_millis(200)).await; // let the receiver register first

    // PHASE 1 — a real folder send that completes. The sender then exits,
    // closing the QUIC connection; the receiver must recover by re-pairing
    // through the rendezvous (not reaccept()).
    timeout(PHASE_DEADLINE, run_send(rzv_addr, &dirs))
        .await
        .expect("phase 1 send timed out")
        .expect("phase 1 send failed");
    assert_file_matches(&payloads.a, &dst_root.join(&payloads.a.name)).await;
    assert_file_matches(&payloads.b, &dst_root.join(&payloads.b.name)).await;

    // Reconstruct a mid-transfer checkpoint: file A done, file B still
    // pending, stamped with the real peer fingerprint. (See the module doc
    // for why this is seeded rather than produced by a live interruption.)
    clear_state_dir(&dirs.state).await; // drop phase 1's own (now-complete) state
    let seed = seed_partial_state(&dirs, peer_fp, &payloads.b.name).await;
    // Drop B from the destination so a successful resume must re-deliver it.
    tokio::fs::remove_file(dst_root.join(&payloads.b.name))
        .await
        .expect("remove dst B");

    // PHASE 2 — re-run the identical `send`. It must locate the checkpoint
    // (same peer + file list), pair again through the rendezvous, skip the
    // completed file A, and re-deliver B.
    sleep(Duration::from_millis(500)).await; // let the receiver re-register
    timeout(PHASE_DEADLINE, run_send(rzv_addr, &dirs))
        .await
        .expect("phase 2 send timed out")
        .expect("phase 2 send failed");

    assert_file_matches(&payloads.a, &dst_root.join(&payloads.a.name)).await;
    assert_file_matches(&payloads.b, &dst_root.join(&payloads.b.name)).await;

    // Resume, not restart: phase 2 must have consumed the checkpoint (a
    // fresh transfer would have minted a new UUID and left it orphaned), and
    // the "Resuming transfer" line must have been logged.
    assert!(
        !seed.exists(),
        "resume should consume the checkpoint, not orphan it: {}",
        seed.display()
    );
    let captured = String::from_utf8_lossy(&logs.lock().unwrap()).to_string();
    assert!(
        captured.contains("Resuming transfer"),
        "phase 2 should have resumed (no 'Resuming transfer' in logs)"
    );

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

/// A single source file's name, on-disk path, and SHA-256.
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

fn transfer_params() -> TransferParams {
    TransferParams {
        compress: false, // random payload is incompressible; skip the work
        compress_level: 3,
        adaptive: true,
        chunk_size: 1024,          // KB → 1 MiB chunks → one chunk per file
        max_speed: 0,              // unlimited; localhost is fast
        max_reconnect_attempts: 1, // single attempt; the test re-pairs explicitly
    }
}

fn spawn_receiver(rzv_addr: SocketAddr, dirs: &Dirs) -> tokio::task::JoinHandle<()> {
    let params = rendezvous_session_params(rzv_addr);
    let output = dirs.dst.clone();
    let identity_dir = dirs.receiver_identity.clone();
    tokio::spawn(async move {
        // Auto-accept so the y/N prompt doesn't block the test.
        // `handle_receive` loops; it returns only on a fatal (non-disconnect)
        // error or when the task is aborted.
        let _ = p2p_cli::receive::handle_receive(output, true, params, Some(identity_dir)).await;
    })
}

/// Drive `handle_send` of the whole `src` folder to completion (resume
/// enabled).
async fn run_send(rzv_addr: SocketAddr, dirs: &Dirs) -> anyhow::Result<()> {
    p2p_cli::send::handle_send(
        dirs.src.clone(),
        Some(dirs.state.clone()),
        false, // allow resume
        rendezvous_session_params(rzv_addr),
        transfer_params(),
        Some(dirs.sender_identity.clone()),
    )
    .await
}

/// Persist a checkpoint describing the `src` folder with every file marked
/// complete *except* `pending_name`, stamped with `peer_fp`. Returns the
/// state file path.
async fn seed_partial_state(dirs: &Dirs, peer_fp: [u8; 32], pending_name: &str) -> PathBuf {
    let files = enumerate_files(&dirs.src).await.expect("enumerate source");
    let mut state = FolderTransferState::new(
        Uuid::new_v4(),
        "src".into(),
        files.clone(),
        &ConfigMessage::default(),
    );
    state.peer_fingerprint = peer_fp;
    for (i, f) in files.iter().enumerate() {
        if !f.path.ends_with(pending_name) {
            state.mark_file_complete(i);
        }
    }
    let path = dirs
        .state
        .join(format!("transfer_{}.json", state.transfer_id));
    state.save_to_file(&path).await.expect("save checkpoint");
    path
}

/// Remove any `transfer_*.json` left in the state dir so the only resumable
/// state the next `send` can find is the one the test seeds.
async fn clear_state_dir(state_dir: &Path) {
    let mut entries = match tokio::fs::read_dir(state_dir).await {
        Ok(entries) => entries,
        Err(_) => return,
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let _ = tokio::fs::remove_file(entry.path()).await;
    }
}

// ---- log capture ------------------------------------------------------------

#[derive(Clone)]
struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedBuf {
    type Writer = SharedBuf;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Install a process-global subscriber that mirrors INFO logs into a buffer
/// the test can inspect. This binary has a single test, so setting the
/// global default once is safe.
fn install_log_capture() -> Arc<Mutex<Vec<u8>>> {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(SharedBuf(buf.clone()))
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
    buf
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

async fn assert_file_matches(expected: &Payload, actual: &Path) {
    let bytes = tokio::fs::read(actual)
        .await
        .unwrap_or_else(|e| panic!("read destination {}: {e}", actual.display()));
    let got: [u8; 32] = Sha256::digest(&bytes).into();
    assert_eq!(
        got,
        expected.sha,
        "destination {} did not match source {} (SHA-256)",
        actual.display(),
        expected.path.display()
    );
}
