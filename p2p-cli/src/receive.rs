//! Receive operations.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tracing::{info, warn};

use p2p_core::{
    error::Error,
    history::{record_transfer, TransferDirection, TransferRecord},
    identity::Identity,
    progress::ProgressState,
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    Uuid,
};

use crate::cli::SessionParams;

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

    if auto_accept {
        info!("  Mode: Auto-accept (no prompts)");
    }

    std::fs::create_dir_all(&output)?;

    let identity = Arc::new(Identity::load_or_generate(identity_dir.as_deref())?);
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
            ConfigMessage::default(),
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
            Some(ConfigMessage::default()),
        )
        .await?
    };

    info!("Session established");
    info!("    Peer: {}", session.peer_device_id());
    info!(
        "    Peer fingerprint: {}",
        hex::encode(session.peer_fingerprint())
    );
    info!("    Compression: {}", session.config().compression_enabled);

    info!("Session ready - waiting for incoming transfers... (Ctrl+C to exit)");
    let _ = auto_accept;
    let peer_addr = session.peer_addr().to_string();
    loop {
        let mut progress = ProgressState::new(0);
        let mut record =
            TransferRecord::new(Uuid::new_v4(), TransferDirection::Receive, peer_addr.clone());

        match session.receive_to(&output, None, Some(&mut progress)).await {
            Ok(_) => {
                record.complete(vec![output.display().to_string()], progress.transferred_bytes());
                if let Err(e) = record_transfer(record, None).await {
                    warn!("Failed to record transfer history: {}", e);
                }
            }
            Err(e)
                if matches!(
                    &e,
                    Error::Disconnected | Error::Quic(_) | Error::Network(_)
                ) =>
            {
                break;
            }
            Err(e) => {
                record.fail(e.to_string());
                let _ = record_transfer(record, None).await;
                return Err(e.into());
            }
        }
    }
    info!("Session ended");

    Ok(())
}
