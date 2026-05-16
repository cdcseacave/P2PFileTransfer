//! Receive operations

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use p2p_core::{
    handshake::HandshakeServer,
    network::tcp::TcpServer,
    progress::ProgressState,
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    transfer_folder::FolderTransferSession,
    Uuid,
};
use std::{path::PathBuf, sync::Arc};
use tracing::info;

use crate::cli::SessionParams;

pub async fn handle_receive(
    output: PathBuf,
    auto_accept: bool,
    parallel: usize,
    session_params: SessionParams,
) -> Result<()> {
    info!("📥 Starting receive mode");
    info!("  Output directory: {}", output.display());

    let role = session_params.get_role("server");
    info!("  Session role: {}", role);

    if auto_accept {
        info!("  Mode: Auto-accept (no prompts)");
    }

    std::fs::create_dir_all(&output)?;

    let parallel = parallel.max(1);

    if parallel > 1 && role == "server" {
        handle_parallel_receive(output, parallel, session_params).await
    } else {
        handle_single_receive(output, auto_accept, session_params).await
    }
}

/// Standard single-connection receive (original behaviour).
async fn handle_single_receive(
    output: PathBuf,
    auto_accept: bool,
    session_params: SessionParams,
) -> Result<()> {
    let device_id = Uuid::new_v4();
    let capabilities = Capabilities::all();

    let mut session = P2PSession::establish(
        &session_params.get_role("server"),
        session_params.peer.clone(),
        session_params.discover,
        session_params.port,
        device_id,
        capabilities,
        Some(ConfigMessage::default()),
    )
    .await?;

    info!("✅ Session established");
    info!("    Peer: {}", session.peer_device_id());
    info!("    Compression: {}", session.config().compression_enabled);
    info!("📁 Session ready - waiting for incoming transfers...");
    info!("  (Press Ctrl+C to exit)");

    session.run_event_loop(&output, auto_accept, true).await?;
    info!("✅ Session ended");
    Ok(())
}

/// Parallel receive: bind once, accept N connections, handle each in its own task.
async fn handle_parallel_receive(
    output: PathBuf,
    parallel: usize,
    session_params: SessionParams,
) -> Result<()> {
    info!(
        "🔀 Parallel receive mode: expecting {} connection(s)",
        parallel
    );

    let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{}", session_params.port).parse()?;
    let server = TcpServer::bind(bind_addr).await?;

    info!(
        "📁 Listening for {} parallel connection(s)... (Ctrl+C to exit)",
        parallel
    );

    // Build multi-progress display.
    // Total bytes unknown until connections arrive, so the overall bar is a spinner.
    // Each connection bar is created inside its task (also unknown total until handshake).
    let multi = MultiProgress::new();
    let overall_bar = multi.add(ProgressBar::new_spinner());
    overall_bar.set_style(
        ProgressStyle::with_template(
            "  [Total ] {spinner:.yellow} {bytes} received ({bytes_per_sec})",
        )
        .unwrap(),
    );
    overall_bar.enable_steady_tick(std::time::Duration::from_millis(100));

    let output = Arc::new(output);
    let mut handles = Vec::new();

    for idx in 0..parallel {
        let conn = server.accept().await?;
        let device_id = Uuid::new_v4();
        let capabilities = Capabilities::all();
        let output_clone = Arc::clone(&output);
        let overall_clone = overall_bar.clone();
        let multi_clone = multi.clone();

        let handle = tokio::spawn(async move {
            let handshaker = HandshakeServer::new(device_id, capabilities);
            let mut conn = conn;
            let handshake = handshaker
                .perform_handshake(&mut conn)
                .await
                .map_err(|e| anyhow::anyhow!("Connection {}: handshake failed: {}", idx + 1, e))?;

            info!(
                "✅ Connection {} established (peer: {})",
                idx + 1,
                handshake.peer_device_id
            );

            // Create the connection bar now that the connection is live.
            // Total bytes unknown until TransferInfo arrives — bar will show 0/? until set.
            let conn_bar = multi_clone.add(ProgressBar::new(0));
            conn_bar.set_style(
                ProgressStyle::with_template(&format!(
                    "  [Conn {:>2}] {{bar:35.green/white}} {{bytes}}/{{total_bytes}} ({{bytes_per_sec}}) {{msg}}",
                    idx + 1
                ))
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏ "),
            );
            conn_bar.enable_steady_tick(std::time::Duration::from_millis(100));

            let transfer_id = Uuid::new_v4();
            let mut folder_session =
                FolderTransferSession::new(&mut conn, handshake.config.clone(), transfer_id);

            let mut progress = ProgressState::from_bars(conn_bar, overall_clone);
            folder_session
                .receive_folder(&output_clone, None, Some(&mut progress))
                .await
                .map_err(|e| anyhow::anyhow!("Connection {} receive failed: {}", idx + 1, e))
        });

        handles.push(handle);
    }

    let mut errors = Vec::new();
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => info!("✅ Connection {} complete", idx + 1),
            Ok(Err(e)) => {
                tracing::warn!("Connection {} failed: {}", idx + 1, e);
                errors.push(e);
            }
            Err(e) => {
                tracing::warn!("Connection {} task panicked: {}", idx + 1, e);
                errors.push(anyhow::anyhow!("Task panic: {}", e));
            }
        }
    }

    overall_bar.finish_with_message("all done");

    if errors.is_empty() {
        info!("✅ All parallel transfers received!");
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
