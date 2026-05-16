//! Receive operations

use anyhow::Result;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use p2p_core::{
    error::Error as P2PError,
    handshake::HandshakeServer,
    network::tcp::TcpServer,
    progress::ProgressState,
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    transfer_folder::FolderTransferSession,
    Uuid,
};
use std::{path::PathBuf, sync::Arc};
use tokio::signal;
use tracing::{info, warn};

use crate::cli::SessionParams;

fn make_spinner(msg: &str) -> ProgressBar {
    let bar = ProgressBar::new_spinner();
    bar.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    bar.set_message(msg.to_string());
    bar.enable_steady_tick(std::time::Duration::from_millis(80));
    bar
}

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

/// Standard single-connection receive — re-listens after connection drops.
async fn handle_single_receive(
    output: PathBuf,
    auto_accept: bool,
    session_params: SessionParams,
) -> Result<()> {
    let role = session_params.get_role("server");
    let port = session_params.port;

    loop {
        let sp = make_spinner(&format!("Listening on port {}... (Ctrl+C to stop)", port));

        let mut session = tokio::select! {
            result = P2PSession::establish(
                &role,
                session_params.peer.clone(),
                session_params.discover,
                port,
                Uuid::new_v4(),
                Capabilities::all(),
                Some(ConfigMessage::default()),
            ) => {
                sp.finish_and_clear();
                result?
            }
            _ = signal::ctrl_c() => {
                sp.finish_and_clear();
                eprintln!("Stopped.");
                return Ok(());
            }
        };

        eprintln!(
            "✓ Connected  peer={}  compression={}",
            session.peer_device_id(),
            session.config().compression_enabled
        );

        let result = tokio::select! {
            r = session.run_event_loop(&output, auto_accept, true) => r,
            _ = signal::ctrl_c() => {
                eprintln!("Stopped.");
                return Ok(());
            }
        };

        match result {
            Ok(()) => {
                // Graceful close from peer — continue listening for next connection
                eprintln!("Connection closed. Re-listening...");
            }
            Err(e) if matches!(e, P2PError::Disconnected) => {
                warn!("Connection dropped. Re-listening on port {}...", port);
                // Small delay so sender's backoff can kick in before we re-bind
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => {
                warn!("Error: {}. Re-listening...", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
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

            let mut progress = ProgressState::from_bars_persistent(conn_bar, overall_clone);
            let mut batch_count = 0usize;

            loop {
                let transfer_id = Uuid::new_v4();
                let mut folder_session =
                    FolderTransferSession::new(&mut conn, handshake.config.clone(), transfer_id);

                match folder_session
                    .receive_folder(&output_clone, None, Some(&mut progress))
                    .await
                {
                    Ok(()) => {
                        batch_count += 1;
                        // Connection still open — sender may send another batch
                    }
                    Err(P2PError::Disconnected) => break,
                    Err(P2PError::Network(ref io_err))
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof
                            || io_err.kind() == std::io::ErrorKind::ConnectionReset
                            || io_err.kind() == std::io::ErrorKind::BrokenPipe =>
                    {
                        // Sender closed connection after last batch
                        break;
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!(
                            "Connection {} receive failed (batch {}): {}",
                            idx + 1,
                            batch_count + 1,
                            e
                        ));
                    }
                }
            }

            info!("Connection {} finished ({} batch(es))", idx + 1, batch_count);
            Ok::<(), anyhow::Error>(())
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
