//! Receive operations.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tracing::{info, warn};

use p2p_core::{
    error::Error,
    history::{record_transfer, TransferDirection, TransferRecord},
    identity::Identity,
    progress::ProgressState,
    protocol::{ConfigMessage, TransferInfo},
    session::P2PSession,
    transfer_folder::AcceptDecision,
    Uuid,
};

use crate::cli::SessionParams;
use crate::rendezvous::{establish_session, is_rendezvous_mode};

pub async fn handle_receive(
    output: PathBuf,
    auto_accept: bool,
    session_params: SessionParams,
    identity_dir: Option<PathBuf>,
) -> Result<()> {
    info!("Starting receive mode");
    info!("  Output directory: {}", output.display());

    let role = session_params.get_role("server");
    info!("  Session role: {}", role);
    info!(
        "  Mode: {}",
        if auto_accept {
            "Auto-accept (no prompts)"
        } else {
            "Interactive (prompt y/N per transfer)"
        }
    );

    std::fs::create_dir_all(&output)?;

    let identity = Arc::new(Identity::load_or_generate(identity_dir.as_deref())?);
    info!("  Identity fingerprint: {}", identity.fingerprint_hex());

    let mut session = pair_or_listen(&session_params, &identity).await?;
    log_session(&session);

    info!("Session ready - waiting for incoming transfers... (Ctrl+C to exit)");
    receive_loop(
        &mut session,
        &output,
        auto_accept,
        &session_params,
        &identity,
    )
    .await
}

/// Initial session pairing. Identical to a post-disconnect re-pair — both
/// go through [`establish_session`] so the rendezvous role-randomness
/// problem is invisible to the receive loop.
async fn pair_or_listen(
    session_params: &SessionParams,
    identity: &Arc<Identity>,
) -> Result<P2PSession> {
    establish_session(
        session_params,
        "server",
        identity.clone(),
        Uuid::new_v4(),
        Some(ConfigMessage::default()),
    )
    .await
}

fn log_session(session: &P2PSession) {
    info!("Session established");
    info!("    Peer: {}", session.peer_device_id());
    info!(
        "    Peer fingerprint: {}",
        hex::encode(session.peer_fingerprint())
    );
    info!("    Compression: {}", session.config().compression_enabled);
}

/// Body of the receive loop: handle one inbound transfer at a time,
/// recover from peer disconnects, exit on unrecoverable errors.
async fn receive_loop(
    session: &mut P2PSession,
    output: &Path,
    auto_accept: bool,
    session_params: &SessionParams,
    identity: &Arc<Identity>,
) -> Result<()> {
    let mut peer_addr = session.peer_addr().to_string();
    loop {
        match receive_one(session, output, auto_accept, peer_addr.clone()).await {
            ReceiveOutcome::Completed => {}
            // Clean end-of-stream from the peer (whether via the framing
            // layer's between-frames close detection or via Quinn surfacing
            // an application close): recover by accepting the next inbound
            // session. The recovery mechanism depends on the original
            // pairing mode — see [`recover_after_disconnect`].
            ReceiveOutcome::PeerDisconnected => {
                recover_after_disconnect(session, session_params, identity).await?;
                peer_addr = session.peer_addr().to_string();
                log_new_peer(session);
            }
            ReceiveOutcome::Fatal(e) => return Err(e),
        }
    }
}

enum ReceiveOutcome {
    Completed,
    PeerDisconnected,
    Fatal(anyhow::Error),
}

async fn receive_one(
    session: &mut P2PSession,
    output: &Path,
    auto_accept: bool,
    peer_addr: String,
) -> ReceiveOutcome {
    let mut progress = ProgressState::new(0);
    let mut record = TransferRecord::new(Uuid::new_v4(), TransferDirection::Receive, peer_addr);

    let accept_cb = |info: &TransferInfo| accept_or_prompt(auto_accept, info);
    match session
        .receive_to(output, None, accept_cb, Some(&mut progress))
        .await
    {
        Ok(summary) => {
            if summary.files.is_empty() {
                info!("Transfer rejected; awaiting next");
                record.interrupt(vec![], 0);
            } else {
                record.complete(summary.files, summary.bytes);
            }
            if let Err(e) = record_transfer(record, None).await {
                warn!("Failed to record transfer history: {}", e);
            }
            ReceiveOutcome::Completed
        }
        // Treat true peer disconnects (whether the framing layer mapped a
        // Quinn close to Error::Disconnected or Quinn surfaced its own
        // Quic variant) as a graceful end-of-stream, not a failure.
        // Disk I/O errors land in Error::Network and DO bubble up.
        Err(e) if matches!(&e, Error::Disconnected | Error::Quic(_)) => {
            info!("Peer disconnected; awaiting next inbound session");
            ReceiveOutcome::PeerDisconnected
        }
        Err(e) => {
            record.fail(e.to_string());
            let _ = record_transfer(record, None).await;
            ReceiveOutcome::Fatal(e.into())
        }
    }
}

/// Bring the session back up after a peer disconnect.
///
/// In direct mode (`--port`-based listener) the QUIC endpoint is still
/// bound and we can `reaccept()` on it — keeping the same `--port`
/// stable across sessions.
///
/// In rendezvous mode, the QUIC endpoint was created during the
/// hole-punch and its role (initiator vs responder) was decided by a
/// UUID compare against the peer; the receiver wins that compare only
/// 50% of the time, so `reaccept()` is structurally wrong half the
/// time. Re-pairing through the rendezvous with the same code works
/// regardless of which side becomes the QUIC initiator on the next
/// pair, and is symmetric with how the first session was established.
async fn recover_after_disconnect(
    session: &mut P2PSession,
    session_params: &SessionParams,
    identity: &Arc<Identity>,
) -> Result<()> {
    if is_rendezvous_mode(session_params) {
        info!(
            "Re-pairing through rendezvous '{}' with same code...",
            session_params.rendezvous.as_deref().unwrap_or("?"),
        );
        *session = establish_session(
            session_params,
            "server",
            identity.clone(),
            Uuid::new_v4(),
            Some(ConfigMessage::default()),
        )
        .await?;
        Ok(())
    } else {
        match session.reaccept().await {
            Ok(()) => Ok(()),
            Err(reaccept_err) => {
                warn!("Failed to re-accept: {}", reaccept_err);
                Err(reaccept_err.into())
            }
        }
    }
}

fn log_new_peer(session: &P2PSession) {
    info!("New peer connected: {}", session.peer_device_id());
}

/// Prompt the user on stderr (y/N) when not in auto-accept mode.
/// Synchronous stdin read inside the async loop is fine here — this only
/// runs at most once per inbound transfer, after which the loop blocks
/// on the network anyway.
fn accept_or_prompt(auto_accept: bool, info: &TransferInfo) -> AcceptDecision {
    if auto_accept {
        return AcceptDecision::Accept;
    }
    let total: u64 = info.items.iter().map(|f| f.size).sum();
    let first = info.items.first().map(|f| f.path.as_str()).unwrap_or("?");
    eprint!(
        "Incoming transfer: {} files starting with {:?} ({} bytes total). Accept? [y/N]: ",
        info.items.len(),
        first,
        total
    );
    let _ = std::io::stderr().flush();
    let mut line = String::new();
    match std::io::stdin().read_line(&mut line) {
        Ok(_) => {
            if line.trim().eq_ignore_ascii_case("y") || line.trim().eq_ignore_ascii_case("yes") {
                AcceptDecision::Accept
            } else {
                AcceptDecision::Reject
            }
        }
        Err(_) => AcceptDecision::Reject,
    }
}
