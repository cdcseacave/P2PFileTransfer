//! Receive operations.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tracing::info;

use p2p_core::{
    identity::Identity,
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    Uuid,
};

use crate::cli::SessionParams;

pub async fn handle_receive(
    output: PathBuf,
    auto_accept: bool,
    session_params: SessionParams,
) -> Result<()> {
    info!("Starting receive mode");
    info!("  Output directory: {}", output.display());

    let role = session_params.get_role("server");
    info!("  Session role: {}", role);

    if auto_accept {
        info!("  Mode: Auto-accept (no prompts)");
    }

    std::fs::create_dir_all(&output)?;

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
    session.run_event_loop(&output, auto_accept, true).await?;
    info!("Session ended");

    Ok(())
}
