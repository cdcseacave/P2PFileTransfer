//! Receive operations

use anyhow::Result;
use p2p_core::{protocol::Capabilities, session::P2PSession, Uuid};
use std::{net::SocketAddr, path::PathBuf};

pub async fn handle_receive(output: PathBuf, port: u16, auto_accept: bool) -> Result<()> {
    println!("📥 Starting receive mode");
    println!("  Output directory: {}", output.display());
    println!("  Listening on port: {}", port);
    if auto_accept {
        println!("  Mode: Auto-accept (no prompts)");
    }

    // Create output directory
    std::fs::create_dir_all(&output)?;

    // Establish session (accept connection + handshake)
    let bind_addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    println!("  Waiting for connection on {}...", bind_addr);

    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();

    let mut session = P2PSession::accept(bind_addr, device_id, capabilities).await?;
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
