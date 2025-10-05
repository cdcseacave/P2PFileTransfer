//! Send operations

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use p2p_core::{
    handshake::HandshakeClient,
    network::tcp::TcpConnection,
    protocol::{Capabilities, ConfigMessage},
    transfer_folder::{FolderProgress, FolderTransferSession, FolderTransferState},
    Uuid,
};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tokio::signal;

use crate::discover::discover_and_select_peer;

pub async fn handle_send(
    path: PathBuf,
    to: Option<String>,
    discover: bool,
    compress: bool,
    compress_level: i32,
    adaptive: bool,
    chunk_size: u32,
    window_size: usize,
    bandwidth_limit: u64,
    transfer_port: u16,
) -> Result<()> {
    println!("📤 Starting send operation");
    println!("  Path: {}", path.display());
    println!("  Mode: {} (window size: {})", if window_size == 1 { "Sequential" } else { "Windowed" }, window_size);
    if bandwidth_limit > 0 {
        println!("  Speed limit: {}", p2p_core::bandwidth::format_bandwidth(bandwidth_limit));
    }
    
    // Validate path exists
    if !path.exists() {
        anyhow::bail!("Path does not exist: {}", path.display());
    }

    // Determine peer address
    let peer_addr = if let Some(addr_str) = to {
        // Direct connection
        println!("  Connecting to: {}", addr_str);
        addr_str.parse::<SocketAddr>()?
    } else if discover {
        // Use discovery
        println!("  Using peer discovery on port {}...", transfer_port);
        let discovered_addr = discover_and_select_peer(transfer_port).await?;
        println!("  Selected peer: {}", discovered_addr);
        discovered_addr
    } else {
        anyhow::bail!("Either --to <address> or --discover must be specified");
    };

    // Connect to peer
    println!("  Establishing connection...");
    let mut connection = TcpConnection::connect(peer_addr).await?;
    println!("  ✓ Connected");

    // Perform handshake
    println!("  Performing handshake...");
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();
    let handshake = HandshakeClient::new(device_id, capabilities);
    
    let config = ConfigMessage {
        compression_enabled: compress,
        compression_level: compress_level,
        adaptive_compression: adaptive,
        chunk_size: chunk_size * 1024, // Convert KB to bytes
        window_size,
        bandwidth_limit,
        ..Default::default()
    };
    
    let handshake_result = handshake.perform_handshake(&mut connection, config.clone()).await?;
    println!("  ✓ Handshake complete (capabilities: {:?})", handshake_result.agreed_capabilities);

    // Send file or folder with signal handling (unified)
    tokio::select! {
        result = send(&mut connection, &path, config) => {
            result?;
            println!("\n✅ Transfer complete!");
        }
        _ = signal::ctrl_c() => {
            println!("\n⚠️  Transfer interrupted by user. State has been saved.");
            println!("  Use 'p2p-transfer resume <transfer-id>' to continue");
            return Ok(());
        }
    }

    Ok(())
}

async fn send(
    connection: &mut TcpConnection,
    path: &Path,
    config: ConfigMessage,
) -> Result<()> {
    let transfer_id = Uuid::new_v4();
    let base_name = path.file_name().unwrap().to_string_lossy().to_string();
    
    if path.is_file() {
        println!("\n📄 Sending file: {}", base_name);
    } else {
        println!("\n📁 Sending folder: {}", base_name);
    }
    
    if config.window_size == 1 {
        println!("   Using sequential transfer (window size: 1)");
    } else {
        println!("   Using windowed transfer protocol (window size: {})", config.window_size);
    }

    let mut session = FolderTransferSession::new(connection, config.clone(), transfer_id);
    
    // Create state file path
    let state_file = PathBuf::from(format!("transfer_{}.json", transfer_id));
    
    // Set up state callback for auto-save
    session.set_state_callback({
        let state_file = state_file.clone(); // Clone for closure only
        Box::new(move |state: &FolderTransferState| {
            // Save state asynchronously (on successful completion, the file is deleted)
            let state_clone = state.clone();
            let path_clone = state_file.clone();
            tokio::spawn(async move {
                if let Err(e) = state_clone.save_to_file(&path_clone).await {
                    eprintln!("⚠️  Failed to save state: {}", e);
                }
            });
        })
    });
    
    // Create multi-progress for overall and per-file progress
    let multi = MultiProgress::new();
    
    let overall_pb = multi.add(ProgressBar::new(100));
    overall_pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files ({percent}%)")
            .unwrap()
            .progress_chars("=>-"),
    );
    
    let current_pb = multi.add(ProgressBar::new(100));
    current_pb.set_style(
        ProgressStyle::default_bar()
            .template("  Current: {msg} {bar:40.green/yellow} {bytes}/{total_bytes} ({percent}%)")
            .unwrap()
            .progress_chars("=>-"),
    );
    
    // Set up progress callback
    session.set_progress_callback(Box::new(move |progress: FolderProgress| {
        // Update overall progress
        overall_pb.set_length(progress.total_files as u64);
        overall_pb.set_position(progress.completed_files as u64);
        
        // Update current file progress
        if let Some(file) = &progress.current_file {
            current_pb.set_message(file.clone());
            current_pb.set_position((progress.current_file_progress * 100.0) as u64);
        }
        
        // If all files complete, finish both bars
        if progress.completed_files == progress.total_files {
            overall_pb.finish_with_message("Complete!");
            current_pb.finish_and_clear();
        }
    }));
    
    // Send file or folder with state tracking (unified method)
    match session.send(path, &base_name).await {
        Ok(_) => {
            let _ = tokio::fs::remove_file(&state_file).await;
            println!("  ✓ Transfer complete, state file removed");
            Ok(())
        }
        Err(e) => {
            println!("  ⚠️  Transfer interrupted, state saved to: {}", state_file.display());
            Err(e.into())
        }
    }
}
