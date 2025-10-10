//! Resume operations

use anyhow::Result;
use p2p_core::{
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    transfer_folder::FolderTransferState,
    Uuid,
};
use std::{net::SocketAddr, path::PathBuf};
use tokio::signal;
use tracing::{debug, info, warn};

pub async fn handle_resume(transfer_id: String, to: String, path: PathBuf) -> Result<()> {
    info!("🔄 Resuming transfer");
    info!("  Transfer ID: {}", transfer_id);
    info!("  Folder path: {}", path.display());
    info!("  Peer address: {}", to);

    // Validate folder exists
    if !path.exists() || !path.is_dir() {
        anyhow::bail!(
            "Folder path does not exist or is not a directory: {}",
            path.display()
        );
    }

    // Load state from file
    let state_path = PathBuf::from(format!("transfer_{}.json", transfer_id));
    if !state_path.exists() {
        anyhow::bail!(
            "State file not found: {}. Transfer may have already completed.",
            state_path.display()
        );
    }

    info!("  Loading transfer state...");
    let state = FolderTransferState::load_from_file(&state_path).await?;

    debug!(
        "  Progress: {}/{} files ({:.1}%)",
        state.completed_files.len(),
        state.files.len(),
        state.progress_percentage()
    );

    // Parse peer address
    let peer_addr = to.parse::<SocketAddr>()?;

    // Connect to peer and establish session
    info!("  Reconnecting to peer...");
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();

    // Use default config for resume (should match original)
    // TODO: restore compression_level, window_size, bandwidth_limit from state
    let config = ConfigMessage::default();

    let mut session = P2PSession::connect(peer_addr, device_id, capabilities, config).await?;
    info!("  ✓ Session established");

    // Create progress state for unified progress tracking
    // Initialize with already completed bytes for resume
    let mut progress = p2p_core::progress::ProgressState::new(state.total_bytes);
    // Add the bytes already transferred
    progress.add_bytes(state.transferred_bytes);

    // Resume transfer with signal handling
    info!("📁 Resuming folder transfer...");

    // Single attempt reconnection config for manual resume (user can run resume command again if needed)
    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: 1,
        initial_backoff_secs: 3,
        max_backoff_secs: 180,
        exponential: true,
    };

    tokio::select! {
        result = session.send_path(&path, &reconnect_config, Some(&state_path), Some(&mut progress)) => {
            result?;
            let _ = tokio::fs::remove_file(&state_path).await;
            info!("✅ Transfer resumed and completed!");
            info!("  State file removed");
        }
        _ = signal::ctrl_c() => {
            warn!("⚠️  Transfer interrupted again. State has been saved.");
            info!("  Use 'p2p-transfer resume {} --peer {} --path {}' to continue",
                transfer_id, to, path.display());
            return Ok(());
        }
    }

    Ok(())
}
