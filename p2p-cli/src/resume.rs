//! Resume operations

use anyhow::Result;
use p2p_core::{
    handshake::HandshakeClient,
    network::tcp::TcpConnection,
    protocol::{Capabilities, ConfigMessage},
    transfer_folder::{FolderTransferSession, FolderTransferState},
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

    // Connect to peer
    info!("  Reconnecting to peer...");
    let mut connection = TcpConnection::connect(peer_addr).await?;

    // Perform handshake (use same config as original transfer)
    info!("  Performing handshake...");
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();
    let handshake = HandshakeClient::new(device_id, capabilities);

    // Use default config for resume (should match original)
    // TODO: restore compression_level, window_size, bandwidth_limit from state
    let config = ConfigMessage::default();

    let handshake_result = handshake
        .perform_handshake(&mut connection, config.clone())
        .await?;
    info!(
        "  ✓ Handshake complete (capabilities: {:?})",
        handshake_result.agreed_capabilities
    );

    // Create session and set up callbacks
    let mut session =
        FolderTransferSession::new(&mut connection, config.clone(), state.transfer_id);

    // Set up state callback for auto-save
    let state_file_clone = state_path.clone();
    session.set_state_callback(Box::new(move |state: &FolderTransferState| {
        let state_clone = state.clone();
        let path_clone = state_file_clone.clone();
        tokio::spawn(async move {
            if let Err(e) = state_clone.save_to_file(&path_clone).await {
                warn!("⚠️  Failed to save state: {}", e);
            }
        });
    }));

    // Create progress state for unified progress tracking
    // Initialize with already completed bytes for resume
    let mut progress = p2p_core::progress::ProgressState::new(state.total_bytes);
    // Add the bytes already transferred
    progress.add_bytes(state.transferred_bytes);

    // Resume transfer with signal handling
    info!("📁 Resuming folder transfer...");

    // Use the unified send_folder method with existing state
    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: 1, // Single attempt, no auto-reconnect for manual resume command
        initial_backoff_secs: 3,
        max_backoff_secs: 180,
        exponential: true,
    };

    tokio::select! {
        result = session.send_folder(&path, &reconnect_config, Some(&state_path), None, Some(&state), Some(&mut progress)) => {
            result?;
            let _ = tokio::fs::remove_file(&state_path).await;
            info!("✅ Transfer resumed and completed!");
            info!("  State file removed");
        }
        _ = signal::ctrl_c() => {
            warn!("⚠️  Transfer interrupted again. State has been saved.");
            info!("  Use 'p2p-transfer resume {} --to {} --path {}' to continue",
                transfer_id, to, path.display());
            return Ok(());
        }
    }

    Ok(())
}
