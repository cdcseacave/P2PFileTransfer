//! Send operations.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tokio::signal;
use tracing::{info, warn};

use p2p_core::{
    history::{record_transfer, TransferDirection, TransferRecord},
    identity::Identity,
    protocol::ConfigMessage,
    session::P2PSession,
    Uuid,
};

use crate::cli::{SessionParams, TransferParams};
use crate::rendezvous::establish_session;
use crate::util::{default_state_dir, derive_base_name, find_resumable_state, resolve_state_file};

pub async fn handle_send(
    path: PathBuf,
    state_dir: Option<PathBuf>,
    no_resume: bool,
    session_params: SessionParams,
    transfer_params: TransferParams,
    identity_dir: Option<PathBuf>,
) -> Result<()> {
    info!("Starting send operation");
    info!("  Path: {}", path.display());

    let role = session_params.get_role("client");
    info!("  Session role: {}", role);

    if transfer_params.max_speed > 0 {
        info!(
            "  Speed limit: {}",
            p2p_core::bandwidth::format_bandwidth(transfer_params.max_speed)
        );
    }

    if !path.exists() {
        anyhow::bail!("Path does not exist: {}", path.display());
    }

    let config = ConfigMessage {
        compression_enabled: transfer_params.compress,
        compression_level: transfer_params.compress_level,
        adaptive_compression: transfer_params.adaptive,
        chunk_size: transfer_params.chunk_size * 1024,
        bandwidth_limit: transfer_params.max_speed,
    };

    let identity = Arc::new(Identity::load_or_generate(identity_dir.as_deref())?);
    info!("  Identity fingerprint: {}", identity.fingerprint_hex());

    let device_id = Uuid::new_v4();

    let mut session = establish_session(
        &session_params,
        "client",
        identity,
        device_id,
        Some(config.clone()),
    )
    .await?;

    info!("Session established");
    info!("    Peer: {}", session.peer_device_id());
    info!(
        "    Peer fingerprint: {}",
        hex::encode(session.peer_fingerprint())
    );

    let peer_fp = session.peer_fingerprint();
    let peer_addr = session.peer_addr().to_string();

    // Resume bridge: with no `--state-dir`, prior state lives in a stable
    // per-user location so a re-run from any working directory finds it.
    let state_dir = state_dir.unwrap_or_else(default_state_dir);

    // Auto-detect a prior incomplete transfer of this exact source to this
    // exact peer and pick up where it left off. `--no-resume` forces a
    // fresh transfer (e.g. when the source content changed in a way the
    // size+mtime check can't see).
    let (transfer_id, state_file, resume_from_bytes) = match resolve_resume(
        &state_dir,
        &path,
        peer_fp,
        session.config().chunk_size,
        no_resume,
    )
    .await?
    {
        Some((id, file, bytes)) => (id, file, bytes),
        None => {
            let id = Uuid::new_v4();
            (
                id,
                resolve_state_file(Some(&state_dir), &id.to_string())?,
                0,
            )
        }
    };

    tokio::select! {
        result = send(
            &mut session,
            &path,
            &state_file,
            transfer_id,
            resume_from_bytes,
            transfer_params.max_reconnect_attempts,
            &peer_addr,
        ) => result,
        _ = signal::ctrl_c() => Err(anyhow::anyhow!("Transfer interrupted by user (Ctrl+C)")),
    }
}

/// Returns `Some((transfer_id, state_file, already_transferred_bytes))`
/// when a prior incomplete transfer should be resumed, or `None` for a
/// fresh transfer (also `None` when `--no-resume` is set).
async fn resolve_resume(
    state_dir: &Path,
    path: &Path,
    peer_fp: [u8; 32],
    chunk_size: u32,
    no_resume: bool,
) -> Result<Option<(Uuid, PathBuf, u64)>> {
    if no_resume {
        return Ok(None);
    }
    let Some((existing_path, existing_state)) =
        find_resumable_state(state_dir, path, peer_fp, chunk_size).await?
    else {
        return Ok(None);
    };
    let pct = 100 * existing_state.transferred_bytes / existing_state.total_bytes.max(1);
    info!(
        "Resuming transfer {} ({}/{} bytes, {}% done)",
        existing_state.transfer_id,
        existing_state.transferred_bytes,
        existing_state.total_bytes,
        pct,
    );
    Ok(Some((
        existing_state.transfer_id,
        existing_path,
        existing_state.transferred_bytes,
    )))
}

#[allow(clippy::too_many_arguments)]
async fn send(
    session: &mut P2PSession,
    path: &Path,
    state_file: &Path,
    transfer_id: Uuid,
    resume_from_bytes: u64,
    max_reconnect_attempts: u32,
    peer_addr: &str,
) -> Result<()> {
    let base_name = derive_base_name(path)?;
    if path.is_file() {
        info!("Sending file: {}", base_name);
    } else {
        info!("Sending folder: {}", base_name);
    }

    let mut progress = p2p_core::progress::ProgressState::new(0);
    // Pre-account bytes a prior session already moved so the progress bar
    // (and the recorded total) start at the resumed percentage rather than 0.
    if resume_from_bytes > 0 {
        progress.add_bytes(resume_from_bytes);
    }
    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: max_reconnect_attempts,
        ..Default::default()
    };

    let mut record = TransferRecord::new(transfer_id, TransferDirection::Send, peer_addr.into());

    let result = session
        .send_path(
            path,
            &reconnect_config,
            Some(state_file),
            Some(&mut progress),
        )
        .await;

    match result {
        Ok(summary) => {
            if state_file.exists() {
                let _ = tokio::fs::remove_file(state_file).await;
            }
            // Prefer the per-file list from the summary so folder
            // transfers record every file rather than just the folder
            // name (finding 3.2). Fall back to base_name when the summary
            // is empty (e.g. a single-file transfer with no inner list).
            let files = if summary.files.is_empty() {
                vec![base_name]
            } else {
                summary.files
            };
            record.complete(files, progress.transferred_bytes());
            if let Err(e) = record_transfer(record, None).await {
                warn!("Failed to record transfer history: {}", e);
            }
            info!("Transfer complete!");
            Ok(())
        }
        Err(e) => {
            if state_file.exists() {
                warn!(
                    "Transfer interrupted; state saved to {}",
                    state_file.display()
                );
                warn!(
                    "Re-run the same `send` command to resume from here \
                     (or pass --no-resume to start over)."
                );
            }
            record.fail(e.to_string());
            if let Err(rec_err) = record_transfer(record, None).await {
                warn!("Failed to record transfer history: {}", rec_err);
            }
            Err(e.into())
        }
    }
}
