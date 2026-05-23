//! Send operations.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tokio::signal;
use tracing::{info, warn};

use p2p_core::{
    identity::Identity,
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    Uuid,
};

use crate::cli::{SessionParams, TransferParams};

pub async fn handle_send(
    path: PathBuf,
    session_params: SessionParams,
    transfer_params: TransferParams,
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

    let identity = Arc::new(Identity::load_or_generate()?);
    info!("  Identity fingerprint: {}", identity.fingerprint_hex());

    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();
    let peer_fp = session_params.parsed_fingerprint()?;

    let mut session = if crate::rendezvous::is_rendezvous_mode(&session_params) {
        crate::rendezvous::establish(
            &session_params,
            identity,
            device_id,
            capabilities,
            config.clone(),
        )
        .await?
    } else {
        P2PSession::establish(
            &role,
            session_params.peer.clone(),
            peer_fp,
            session_params.discover,
            session_params.port,
            identity,
            device_id,
            capabilities,
            Some(config.clone()),
        )
        .await?
    };

    info!("Session established");
    info!("    Peer: {}", session.peer_device_id());
    info!(
        "    Peer fingerprint: {}",
        hex::encode(session.peer_fingerprint())
    );
    info!("    Capabilities: {:?}", session.capabilities());

    tokio::select! {
        result = send(&mut session, &path) => result,
        _ = signal::ctrl_c() => Err(anyhow::anyhow!("Transfer interrupted by user (Ctrl+C)")),
    }
}

async fn send(session: &mut P2PSession, path: &Path) -> Result<()> {
    let base_name = path.file_name().unwrap().to_string_lossy().to_string();
    if path.is_file() {
        info!("Sending file: {}", base_name);
    } else {
        info!("Sending folder: {}", base_name);
    }

    let transfer_id = Uuid::new_v4();
    let state_file = PathBuf::from(format!("transfer_{}.json", transfer_id));
    let mut progress = p2p_core::progress::ProgressState::new(0);
    let reconnect_config = p2p_core::reconnect::ReconnectConfig::default();

    match session
        .send_path(
            path,
            &reconnect_config,
            Some(&state_file),
            Some(&mut progress),
        )
        .await
    {
        Ok(_) => {
            if state_file.exists() {
                let _ = tokio::fs::remove_file(&state_file).await;
            }
            info!("Transfer complete!");
            Ok(())
        }
        Err(e) => {
            if state_file.exists() {
                warn!("Transfer interrupted");
                warn!("State saved to: {}", state_file.display());
                warn!("Resume with: p2p-transfer resume {}", state_file.display());
            }
            Err(e.into())
        }
    }
}
