//! Send operations

use anyhow::Result;
use p2p_core::{
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
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
    result
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

    // Generate transfer ID for this operation (or use existing one from state file)
    let transfer_id = Uuid::new_v4();

    // Create state file path
    let state_file = PathBuf::from(format!("transfer_{}.json", transfer_id));

    // Create progress state for unified progress tracking
    let mut progress = p2p_core::progress::ProgressState::new(0);

    // Configure reconnection behavior
    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: max_retries,
        initial_backoff_secs: 3,
        max_backoff_secs: 180,
        exponential: true,
    };

    // Send file or folder (state is managed internally by session)
    let result = session
        .send_path(
            path,
            &reconnect_config,
            Some(&state_file),
            Some(&mut progress),
        )
        .await;

    match result {
        Ok(_) => {
            // Success - clean up state file (already done by send_path)
            if state_file.exists() {
                let _ = tokio::fs::remove_file(&state_file).await;
            }
            info!("✅ Transfer complete!");
            Ok(())
        }
        Err(e) => {
            // Error - state was already saved by send_path for resume
            if state_file.exists() {
                warn!("  ⚠️  Transfer interrupted after {} attempts", max_retries);
                warn!("  📝 State saved to: {}", state_file.display());
                warn!(
                    "  💡 Resume with: p2p-transfer resume {}",
                    state_file.display()
                );
            }
            Err(e.into())
        }
    }
}
