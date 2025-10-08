//! Send operations

use anyhow::Result;
use p2p_core::{
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    transfer_folder::FolderTransferState,
    Uuid,
};
use std::path::{Path, PathBuf};
use tokio::signal;

use crate::cli::{SessionParams, TransferParams};
use tracing::{info, warn};

pub async fn handle_send(
    path: PathBuf,
    session_params: SessionParams,
    transfer_params: TransferParams,
) -> Result<()> {
    info!("📤 Starting send operation");
    info!("  Path: {}", path.display());

    // Determine role (default to client for send)
    let role = session_params.get_role("client");
    info!("  Session role: {}", role);

    info!(
        "  Mode: {} (window size: {})",
        if transfer_params.window_size == 1 {
            "Sequential"
        } else {
            "Windowed"
        },
        transfer_params.window_size
    );
    if transfer_params.max_speed > 0 {
        info!(
            "  Speed limit: {}",
            p2p_core::bandwidth::format_bandwidth(transfer_params.max_speed)
        );
    }

    // Validate path exists
    if !path.exists() {
        anyhow::bail!("Path does not exist: {}", path.display());
    }

    // Build configuration
    let config = ConfigMessage {
        compression_enabled: transfer_params.compress,
        compression_level: transfer_params.compress_level,
        adaptive_compression: transfer_params.adaptive,
        chunk_size: transfer_params.chunk_size * 1024, // Convert KB to bytes
        window_size: transfer_params.window_size,
        bandwidth_limit: transfer_params.max_speed,
    };

    // Establish session based on role (with discovery support)
    // Peer address parsing and status messages are handled by P2PSession::establish()
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();

    let mut session = P2PSession::establish(
        &role,
        session_params.peer.clone(),
        session_params.discover,
        session_params.port,
        device_id,
        capabilities,
        Some(config.clone()),
    )
    .await?;

    info!("✅ Session established");
    info!("    Peer: {}", session.peer_device_id());
    info!("    Capabilities: {:?}", session.capabilities());

    // Send file or folder with signal handling (unified)
    let result = tokio::select! {
        result = send(&mut session, &path, config, transfer_params.max_retries) => {
            result
        }
        _ = signal::ctrl_c() => {
            Err(anyhow::anyhow!("Transfer interrupted by user (Ctrl+C)"))
        }
    };

    match result {
        Ok(_) => {
            info!("✅ Transfer complete!");
            Ok(())
        }
        Err(e) => {
            warn!("⚠️  Transfer interrupted: {}", e);
            info!("  State has been saved. Use 'p2p-transfer resume <transfer-id>' to continue");
            Err(e)
        }
    }
}

async fn send(
    session: &mut P2PSession,
    path: &Path,
    _config: ConfigMessage,
    max_retries: u32,
) -> Result<()> {
    let base_name = path.file_name().unwrap().to_string_lossy().to_string();

    if path.is_file() {
        info!("📄 Sending file: {}", base_name);
    } else {
        info!("📁 Sending folder: {}", base_name);
    }

    let config = session.config();
    if config.window_size == 1 {
        info!("   Using sequential transfer (window size: 1)");
    } else {
        info!(
            "   Using windowed transfer protocol (window size: {})",
            config.window_size
        );
    }

    // Display reconnection behavior based on max_retries
    if max_retries == 0 {
        info!("   Auto-reconnect: enabled (unlimited retries)");
    } else if max_retries == 1 {
        info!("   Auto-reconnect: disabled (no retry)");
    } else {
        info!("   Auto-reconnect: enabled (max {} retries)", max_retries);
    }

    // Generate transfer ID for this operation
    let transfer_id = Uuid::new_v4();

    // Create state file path
    let state_file = PathBuf::from(format!("transfer_{}.json", transfer_id));

    // Keep state in memory using Arc<Mutex> for thread-safe access
    let current_state = std::sync::Arc::new(std::sync::Mutex::new(None::<FolderTransferState>));
    // Create progress state for unified progress tracking
    let mut progress = p2p_core::progress::ProgressState::new(0);

    // Create state callback to update in-memory state
    let current_state_for_callback = current_state.clone();
    let state_callback = Box::new(move |state: &FolderTransferState| {
        if let Ok(mut guard) = current_state_for_callback.lock() {
            *guard = Some(state.clone());
        }
    });

    // Configure reconnection behavior
    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: max_retries,
        initial_backoff_secs: 3,
        max_backoff_secs: 180,
        exponential: true,
    };

    // Create state provider closure that reads from in-memory state
    let state_for_reconnect = current_state.clone();
    let state_provider = Box::new(move || {
        state_for_reconnect
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    });

    // Send file or folder with state tracking (unified method)
    let result = session
        .send_path(
            path,
            Some(&mut progress),
            Some(state_callback),
            &reconnect_config,
            Some(&state_file),
            Some(state_provider),
        )
        .await;

    match result {
        Ok(_) => {
            // Success - save state if debug/trace mode is enabled (for debugging/analysis)
            if tracing::level_enabled!(tracing::Level::DEBUG) {
                let state_to_save = current_state.lock().ok().and_then(|guard| guard.clone());
                if let Some(state) = state_to_save {
                    if let Err(save_err) = state.save_to_file(&state_file).await {
                        warn!("  ⚠️  Failed to save state (debug): {}", save_err);
                    } else {
                        info!("  📝 State saved to: {} (debug mode)", state_file.display());
                    }
                }
            } else {
                // Clean up any old state file
                if state_file.exists() {
                    let _ = tokio::fs::remove_file(&state_file).await;
                }
            }
            info!("✅ Transfer complete!");
            Ok(())
        }
        Err(e) => {
            // Error - save current state to disk for resume
            // Clone state before dropping the lock to avoid holding across await
            let state_to_save = current_state.lock().ok().and_then(|guard| guard.clone());

            if let Some(state) = state_to_save {
                if let Err(save_err) = state.save_to_file(&state_file).await {
                    warn!("  ⚠️  Failed to save state: {}", save_err);
                } else {
                    info!(
                        "  ⚠️  Transfer interrupted, state saved to: {}",
                        state_file.display()
                    );
                }
            }
            Err(e.into())
        }
    }
}
