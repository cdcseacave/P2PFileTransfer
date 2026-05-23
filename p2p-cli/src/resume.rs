//! Resume operations.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio::signal;
use tracing::{debug, info, warn};

use p2p_core::{
    identity::Identity,
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    transfer_folder::FolderTransferState,
    Uuid,
};

pub async fn handle_resume(
    transfer_id: String,
    to: String,
    peer_fingerprint_hex: String,
    path: PathBuf,
) -> Result<()> {
    info!("Resuming transfer");
    info!("  Transfer ID: {}", transfer_id);
    info!("  Folder path: {}", path.display());
    info!("  Peer address: {}", to);

    if !path.exists() || !path.is_dir() {
        anyhow::bail!(
            "Folder path does not exist or is not a directory: {}",
            path.display()
        );
    }

    let state_path = PathBuf::from(format!("transfer_{}.json", transfer_id));
    if !state_path.exists() {
        anyhow::bail!(
            "State file not found: {}. Transfer may have already completed.",
            state_path.display()
        );
    }

    info!("Loading transfer state...");
    let state = FolderTransferState::load_from_file(&state_path).await?;
    debug!(
        "Progress: {}/{} files ({:.1}%)",
        state.completed_files.len(),
        state.files.len(),
        state.progress_percentage()
    );

    let peer_addr = to.parse::<SocketAddr>()?;

    if peer_fingerprint_hex.len() != 64 {
        anyhow::bail!(
            "--peer-fingerprint must be 64 hex chars, got {}",
            peer_fingerprint_hex.len()
        );
    }
    let mut peer_fp = [0u8; 32];
    peer_fp.copy_from_slice(&hex::decode(&peer_fingerprint_hex)?);

    let identity = Arc::new(Identity::load_or_generate()?);
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();
    let config = ConfigMessage::default();

    info!("Reconnecting to peer...");
    let mut session = P2PSession::connect(
        peer_addr,
        peer_fp,
        identity,
        device_id,
        capabilities,
        config,
    )
    .await?;
    info!("Session established");

    let mut progress = p2p_core::progress::ProgressState::new(state.total_bytes);
    progress.add_bytes(state.transferred_bytes);

    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: 1,
        ..Default::default()
    };

    info!("Resuming folder transfer...");
    tokio::select! {
        result = session.send_path(&path, &reconnect_config, Some(&state_path), Some(&mut progress)) => {
            result?;
            let _ = tokio::fs::remove_file(&state_path).await;
            info!("Transfer resumed and completed!");
        }
        _ = signal::ctrl_c() => {
            warn!("Transfer interrupted again. State has been saved.");
            info!(
                "Use 'p2p-transfer resume {} --to {} --peer-fingerprint {} --path {}' to continue",
                transfer_id,
                to,
                peer_fingerprint_hex,
                path.display(),
            );
            return Ok(());
        }
    }

    Ok(())
}
