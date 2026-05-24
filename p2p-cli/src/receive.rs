//! Receive operations.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tracing::{info, warn};

use p2p_core::{
    error::Error,
    history::{record_transfer, TransferDirection, TransferRecord},
    identity::Identity,
    progress::ProgressState,
    protocol::{Capabilities, ConfigMessage, TransferInfo},
    session::P2PSession,
    transfer_folder::AcceptDecision,
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
    } else {
        info!("  Mode: Interactive (prompt y/N per transfer)");
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
    let mut peer_addr = session.peer_addr().to_string();
    loop {
        let mut progress = ProgressState::new(0);
        let mut record = TransferRecord::new(
            Uuid::new_v4(),
            TransferDirection::Receive,
            peer_addr.clone(),
        );

        let accept_cb = |info: &TransferInfo| accept_or_prompt(auto_accept, info);
        match session
            .receive_to(&output, None, accept_cb, Some(&mut progress))
            .await
        {
            Ok(summary) => {
                if summary.files.is_empty() {
                    info!("Transfer rejected; awaiting next");
                    record.interrupt(vec![], 0);
                } else {
                    record.complete(summary.files, summary.bytes);
                }
                if let Err(e) = record_transfer(record, None).await {
                    warn!("Failed to record transfer history: {}", e);
                }
            }
            // Only treat true peer disconnects as a graceful end-of-stream;
            // disk I/O failures (which surface as Error::Network) propagate
            // and get recorded as failed (finding 2.2).
            Err(e) if matches!(&e, Error::Disconnected | Error::Quic(_)) => {
                info!("Peer disconnected; awaiting next inbound session");
                match session.reaccept().await {
                    Ok(()) => {
                        peer_addr = session.peer_addr().to_string();
                        info!("New peer connected: {}", session.peer_device_id());
                    }
                    Err(reaccept_err) => {
                        warn!("Failed to re-accept: {}", reaccept_err);
                        return Err(reaccept_err.into());
                    }
                }
            }
            Err(e) => {
                record.fail(e.to_string());
                let _ = record_transfer(record, None).await;
                return Err(e.into());
            }
        }
    }
}

/// Prompt the user on stderr (y/N) when not in auto-accept mode.
/// Synchronous stdin read inside the async loop is fine here — this only
/// runs at most once per inbound transfer, after which the loop blocks
/// on the network anyway.
fn accept_or_prompt(auto_accept: bool, info: &TransferInfo) -> AcceptDecision {
    if auto_accept {
        return AcceptDecision::Accept;
    }
    let total: u64 = info.items.iter().map(|f| f.size).sum();
    let first = info
        .items
        .first()
        .map(|f| f.path.as_str())
        .unwrap_or("?");
    eprint!(
        "Incoming transfer: {} files starting with {:?} ({} bytes total). Accept? [y/N]: ",
        info.items.len(),
        first,
        total
    );
    let _ = std::io::stderr().flush();
    let mut line = String::new();
    match std::io::stdin().read_line(&mut line) {
        Ok(_) => {
            if line.trim().eq_ignore_ascii_case("y") || line.trim().eq_ignore_ascii_case("yes") {
                AcceptDecision::Accept
            } else {
                AcceptDecision::Reject
            }
        }
        Err(_) => AcceptDecision::Reject,
    }
}
