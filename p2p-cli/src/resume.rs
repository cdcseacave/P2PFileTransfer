//! Resume operations

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use p2p_core::{
    handshake::HandshakeClient,
    network::tcp::TcpConnection,
    protocol::{Capabilities, ConfigMessage},
    transfer_folder::{FolderProgress, FolderTransferSession, FolderTransferState},
    Uuid,
};
use std::{net::SocketAddr, path::PathBuf};
use tokio::signal;

pub async fn handle_resume(transfer_id: String, to: String, path: PathBuf) -> Result<()> {
    println!("🔄 Resuming transfer");
    println!("  Transfer ID: {}", transfer_id);
    println!("  Folder path: {}", path.display());
    println!("  Peer address: {}", to);

    // Validate folder exists
    if !path.exists() || !path.is_dir() {
        anyhow::bail!(
            "Folder path does not exist or is not a directory: {}",
            path.display()
        );
    }

    // Load state from file
    let state_path = PathBuf::from(format!("transfer_{}.json", transfer_id));
    if !state_path.exists() {
        anyhow::bail!(
            "State file not found: {}. Transfer may have already completed.",
            state_path.display()
        );
    }

    println!("  Loading transfer state...");
    let state = FolderTransferState::load_from_file(&state_path).await?;

    println!(
        "  Progress: {}/{} files ({:.1}%)",
        state.completed_files.len(),
        state.files.len(),
        state.progress_percentage()
    );

    // Parse peer address
    let peer_addr = to.parse::<SocketAddr>()?;

    // Connect to peer
    println!("  Reconnecting to peer...");
    let mut connection = TcpConnection::connect(peer_addr).await?;
    println!("  ✓ Connected");

    // Perform handshake (use same config as original transfer)
    println!("  Performing handshake...");
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();
    let handshake = HandshakeClient::new(device_id, capabilities);

    // Use default config for resume (should match original)
    // TODO: restore compression_level, window_size, bandwidth_limit from state
    let config = ConfigMessage::default();

    let handshake_result = handshake
        .perform_handshake(&mut connection, config.clone())
        .await?;
    println!(
        "  ✓ Handshake complete (capabilities: {:?})",
        handshake_result.agreed_capabilities
    );

    // Create session and set up callbacks
    let mut session =
        FolderTransferSession::new(&mut connection, config.clone(), state.transfer_id);

    // Set up state callback for auto-save
    let state_file_clone = state_path.clone();
    session.set_state_callback(Box::new(move |state: &FolderTransferState| {
        let state_clone = state.clone();
        let path_clone = state_file_clone.clone();
        tokio::spawn(async move {
            if let Err(e) = state_clone.save_to_file(&path_clone).await {
                eprintln!("⚠️  Failed to save state: {}", e);
            }
        });
    }));

    // Set up progress callback
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

    session.set_progress_callback(Box::new(move |progress: FolderProgress| {
        overall_pb.set_length(progress.total_files as u64);
        overall_pb.set_position(progress.completed_files as u64);

        if let Some(file) = &progress.current_file {
            current_pb.set_message(file.clone());
            current_pb.set_position((progress.current_file_progress * 100.0) as u64);
        }

        if progress.completed_files == progress.total_files {
            overall_pb.finish_with_message("Complete!");
            current_pb.finish_and_clear();
        }
    }));

    // Resume transfer with signal handling
    println!("\n📁 Resuming folder transfer...");
    tokio::select! {
        result = session.resume_send_folder(&path, &state) => {
            result?;
            let _ = tokio::fs::remove_file(&state_path).await;
            println!("\n✅ Transfer resumed and completed!");
            println!("  State file removed");
        }
        _ = signal::ctrl_c() => {
            println!("\n⚠️  Transfer interrupted again. State has been saved.");
            println!("  Use 'p2p-transfer resume {} --to {} --path {}' to continue",
                transfer_id, to, path.display());
            return Ok(());
        }
    }

    Ok(())
}
