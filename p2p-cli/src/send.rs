//! Send operations

use anyhow::Result;
use p2p_core::{
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    transfer_folder::{scan_folder_for_parallel, split_files_for_parallel},
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

    if !path.exists() {
        anyhow::bail!("Path does not exist: {}", path.display());
    }

    let config = ConfigMessage {
        compression_enabled: transfer_params.compress,
        compression_level: transfer_params.compress_level,
        adaptive_compression: transfer_params.adaptive,
        chunk_size: transfer_params.chunk_size * 1024,
        window_size: transfer_params.window_size,
        bandwidth_limit: transfer_params.max_speed,
    };

    let parallel = transfer_params.parallel.max(1);

    if parallel > 1 && path.is_dir() {
        handle_parallel_send(path, session_params, config, transfer_params.max_retries, parallel)
            .await
    } else {
        handle_single_send(path, session_params, config, transfer_params.max_retries).await
    }
}

/// Standard single-connection send (original behaviour).
async fn handle_single_send(
    path: PathBuf,
    session_params: SessionParams,
    config: ConfigMessage,
    max_retries: u32,
) -> Result<()> {
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();

    let mut session = P2PSession::establish(
        &session_params.get_role("client"),
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

    let result = tokio::select! {
        result = send_path(&mut session, &path, max_retries) => result,
        _ = signal::ctrl_c() => Err(anyhow::anyhow!("Transfer interrupted by user (Ctrl+C)")),
    };
    result
}

/// Multi-connection parallel send: scan once, split, open N connections simultaneously.
async fn handle_parallel_send(
    path: PathBuf,
    session_params: SessionParams,
    config: ConfigMessage,
    max_retries: u32,
    parallel: usize,
) -> Result<()> {
    info!(
        "🔀 Parallel mode: {} simultaneous connections",
        parallel
    );

    // Scan the folder once on the main thread
    info!("Scanning folder: {}", path.display());
    let (base_path, all_files) = scan_folder_for_parallel(&path).await?;
    let total_files = all_files.len();
    let total_bytes: u64 = all_files.iter().map(|f| f.size).sum();
    info!(
        "Found {} files ({} bytes total)",
        total_files,
        p2p_core::bandwidth::format_bandwidth(total_bytes)
    );

    // Split file list into balanced groups
    let groups = split_files_for_parallel(all_files, parallel);
    let actual_parallel = groups.len();
    info!(
        "Distributing across {} connection(s)",
        actual_parallel
    );
    for (i, g) in groups.iter().enumerate() {
        let g_bytes: u64 = g.iter().map(|f| f.size).sum();
        info!(
            "  Connection {}: {} files, {}",
            i + 1,
            g.len(),
            p2p_core::bandwidth::format_bandwidth(g_bytes)
        );
    }

    let peer_addr = session_params.peer.clone();
    let port = session_params.port;
    let role = session_params.get_role("client");
    let discover = session_params.discover;

    // Spawn one task per group
    let mut handles = Vec::new();
    for (idx, group) in groups.into_iter().enumerate() {
        let config_clone = config.clone();
        let peer_clone = peer_addr.clone();
        let role_clone = role.clone();
        let base_path_clone = base_path.clone();
        let _max_retries = max_retries;

        let handle = tokio::spawn(async move {
            let device_id = Uuid::new_v4();
            let capabilities = Capabilities::all();

            let mut session = P2PSession::establish(
                &role_clone,
                peer_clone,
                discover,
                port,
                device_id,
                capabilities,
                Some(config_clone),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Connection {}: {}", idx + 1, e))?;

            info!("✅ Connection {} established (peer: {})", idx + 1, session.peer_device_id());

            session
                .send_file_group(&base_path_clone, group, None)
                .await
                .map_err(|e| anyhow::anyhow!("Connection {} transfer failed: {}", idx + 1, e))
        });

        handles.push(handle);
    }

    // Wait for all parallel transfers and collect errors
    let mut errors = Vec::new();
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => info!("✅ Connection {} complete", idx + 1),
            Ok(Err(e)) => {
                warn!("Connection {} failed: {}", idx + 1, e);
                errors.push(e);
            }
            Err(e) => {
                warn!("Connection {} task panicked: {}", idx + 1, e);
                errors.push(anyhow::anyhow!("Task panic: {}", e));
            }
        }
    }

    if errors.is_empty() {
        info!("✅ All parallel transfers complete!");
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{} connection(s) failed: {}",
            errors.len(),
            errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        ))
    }
}

async fn send_path(session: &mut P2PSession, path: &Path, max_retries: u32) -> Result<()> {
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

    if max_retries == 0 {
        info!("   Auto-reconnect: enabled (unlimited retries)");
    } else if max_retries == 1 {
        info!("   Auto-reconnect: disabled (no retry)");
    } else {
        info!("   Auto-reconnect: enabled (max {} retries)", max_retries);
    }

    let transfer_id = Uuid::new_v4();
    let state_file = PathBuf::from(format!("transfer_{}.json", transfer_id));
    let mut progress = p2p_core::progress::ProgressState::new(0);

    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: max_retries,
        initial_backoff_secs: 3,
        max_backoff_secs: 180,
        exponential: true,
    };

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
            if state_file.exists() {
                let _ = tokio::fs::remove_file(&state_file).await;
            }
            info!("✅ Transfer complete!");
            Ok(())
        }
        Err(e) => {
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
