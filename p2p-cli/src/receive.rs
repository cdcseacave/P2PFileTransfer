//! Receive operations

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use p2p_core::{
    handshake::HandshakeServer,
    network::tcp::{TcpConnection, TcpServer},
    protocol::{Capabilities, ConfigMessage},
    transfer_folder::{FolderProgress, FolderTransferSession},
    Uuid,
};
use std::{
    net::SocketAddr,
    path::Path,
    path::PathBuf,
};

pub async fn handle_receive(
    output: PathBuf,
    port: u16,
    auto_accept: bool,
) -> Result<()> {
    println!("📥 Starting receive mode");
    println!("  Output directory: {}", output.display());
    println!("  Listening on port: {}", port);
    if auto_accept {
        println!("  Mode: Auto-accept (no prompts)");
    }
    
    // Create output directory
    std::fs::create_dir_all(&output)?;

    // Start TCP server
    let bind_addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    println!("  Waiting for connection on {}...", bind_addr);
    
    let server = TcpServer::bind(bind_addr).await?;
    let mut connection = server.accept().await?;
    println!("  ✓ Connection accepted from: {}", connection.peer_addr());

    // Perform handshake as server
    println!("  Performing handshake...");
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();
    let handshake = HandshakeServer::new(device_id, capabilities);
    
    let handshake_result = handshake.perform_handshake(&mut connection).await?;
    println!("  ✓ Handshake complete");
    println!("    Compression: {}", handshake_result.config.compression_enabled);
    
    // Prompt user to accept transfer (unless auto_accept is enabled)
    if !auto_accept {
        use std::io::{self, Write};
        
        print!("\n  Accept this transfer? [Y/n]: ");
        io::stdout().flush()?;
        
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();
        
        if !input.is_empty() && input != "y" && input != "yes" {
            println!("  ❌ Transfer declined by user");
            return Ok(());
        }
        println!("  ✓ Transfer accepted");
    }

    // Unified receive using folder session (works for both files and folders)
    receive_folder(&mut connection, &output, handshake_result.config).await?;
    println!("\n✅ Transfer complete!");

    Ok(())
}

async fn receive_folder(
    connection: &mut TcpConnection,
    output_dir: &Path,
    config: ConfigMessage,
) -> Result<()> {
    println!("\n📁 Receiving...");

    let transfer_id = Uuid::new_v4();
    let mut session = FolderTransferSession::new(connection, config.clone(), transfer_id);
    
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
    
    session.receive_folder(output_dir).await?;
    
    Ok(())
}
