//! Receive operations

use anyhow::Result;
use p2p_core::{
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    Uuid,
};
use std::path::PathBuf;

use crate::cli::SessionParams;

pub async fn handle_receive(
    output: PathBuf,
    auto_accept: bool,
    session_params: SessionParams,
) -> Result<()> {
    println!("📥 Starting receive mode");
    println!("  Output directory: {}", output.display());

    // Determine role (default to server for receive)
    let role = session_params.get_role("server");
    println!("  Session role: {}", role);

    if auto_accept {
        println!("  Mode: Auto-accept (no prompts)");
    }

    // Create output directory
    std::fs::create_dir_all(&output)?;

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
        Some(ConfigMessage::default()),
    )
    .await?;

    println!("  ✓ Session established");
    println!("    Peer: {}", session.peer_device_id());
    println!("    Compression: {}", session.config().compression_enabled);

    println!("\n📁 Session ready - waiting for incoming transfers...");
    println!("  (Press Ctrl+C to exit)");

    // Run event loop - automatically receives incoming transfers
    // The loop continues until the peer closes the connection
    session.run_event_loop(&output, auto_accept).await?;

    println!("\n✅ Session ended");

    Ok(())
}
