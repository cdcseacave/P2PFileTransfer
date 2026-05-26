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
use crate::util::{derive_base_name, resolve_state_file};

pub async fn handle_send(
    path: PathBuf,
    state_dir: Option<PathBuf>,
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

    let peer_addr = session.peer_addr().to_string();

    tokio::select! {
        result = send(&mut session, &path, state_dir.as_deref(), transfer_params.max_reconnect_attempts, &peer_addr) => result,
        _ = signal::ctrl_c() => Err(anyhow::anyhow!("Transfer interrupted by user (Ctrl+C)")),
    }
}

async fn send(
    session: &mut P2PSession,
    path: &Path,
    state_dir: Option<&Path>,
    max_reconnect_attempts: u32,
    peer_addr: &str,
) -> Result<()> {
    let base_name = derive_base_name(path)?;
    if path.is_file() {
        info!("Sending file: {}", base_name);
    } else {
        info!("Sending folder: {}", base_name);
    }

    let transfer_id = Uuid::new_v4();
    let state_file = resolve_state_file(state_dir, &transfer_id.to_string())?;
    let mut progress = p2p_core::progress::ProgressState::new(0);
    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: max_reconnect_attempts,
        ..Default::default()
    };

    let mut record = TransferRecord::new(transfer_id, TransferDirection::Send, peer_addr.into());

    let result = session
        .send_path(
            path,
            &reconnect_config,
            Some(&state_file),
            Some(&mut progress),
        )
        .await;

    match result {
        Ok(summary) => {
            if state_file.exists() {
                let _ = tokio::fs::remove_file(&state_file).await;
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
                warn!("Transfer interrupted");
                warn!("State saved to: {}", state_file.display());
                warn!(
                    "Resume with: p2p-transfer resume {} --path <orig-path> \
                     (then your original pairing flags: --peer + --peer-fingerprint, \
                     or --rendezvous + --code)",
                    transfer_id
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
