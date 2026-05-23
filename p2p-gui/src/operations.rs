//! Message handling and async operations
//!
//! This module handles all message processing and coordinates async operations.

use crate::{
    message::Message,
    state::{AppState, ConsoleIcon},
};
use anyhow::Result;
use iced::Command;
use p2p_core::{
    protocol::{Capabilities, ConfigMessage},
    session::P2PSession,
    Uuid,
};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// Handle incoming messages and update state
pub fn handle_message(state: &mut AppState, message: Message) -> Command<Message> {
    match message {
        Message::TabSelected(tab) => {
            state.current_tab = tab;
            Command::none()
        }

        // Connection tab
        Message::ModeSelected(mode) => {
            state.connection_state.mode = mode;
            Command::none()
        }
        Message::PeerAddressChanged(addr) => {
            state.connection_state.peer_address = addr;
            Command::none()
        }
        Message::PortChanged(port) => {
            state.connection_state.port = port;
            Command::none()
        }
        Message::DiscoveryToggled(enabled) => {
            state.connection_state.use_discovery = enabled;
            Command::none()
        }
        Message::StartConnection => handle_start_connection(state),
        Message::StopConnection => {
            // Signal listener to stop
            state
                .cancel_listener
                .store(true, std::sync::atomic::Ordering::Relaxed);

            state.session = None;
            state.connection_state.is_active = false;
            state.connection_state.status_message = String::from("Idle");
            state.add_console_message(String::from("Connection stopped"), ConsoleIcon::Info);
            Command::none()
        }
        Message::ConnectionEstablished(msg) => {
            state.connection_state.is_active = true;
            state.connection_state.status_message = String::from("Connected");
            state.add_console_message(msg, ConsoleIcon::Success);
            Command::none()
        }
        Message::ConnectionEstablishedWithSession(session, msg) => {
            state.session = Some(session);
            state.connection_state.is_active = true;
            state.connection_state.status_message = String::from("Connected");
            state.add_console_message(msg, ConsoleIcon::Success);
            Command::none()
        }
        Message::ConnectionFailed(msg) => {
            state.connection_state.is_active = false;
            state.connection_state.status_message = String::from("Idle");
            state.add_console_message(format!("Connection failed: {}", msg), ConsoleIcon::Error);
            Command::none()
        }
        Message::ListenerWaiting => {
            state.connection_state.status_message = String::from("Listening");
            state.add_console_message(
                String::from("Waiting for incoming connection..."),
                ConsoleIcon::Info,
            );
            Command::none()
        }
        Message::ListenerActive(peer_id) => {
            state.connection_state.status_message = String::from("Connected");
            state.add_console_message(
                format!("Connected to peer: {}", peer_id),
                ConsoleIcon::Success,
            );

            // Create transfer record for incoming transfer
            let transfer_id = Uuid::new_v4();
            state.current_transfer = Some(p2p_core::history::TransferRecord::new(
                transfer_id,
                p2p_core::history::TransferDirection::Receive,
                peer_id.clone(),
            ));

            Command::none()
        }
        Message::TransferStarted(msg) => {
            state.receive_state.status_message = format!("📥 {}", msg);
            state.add_console_message(msg, ConsoleIcon::Info);
            Command::none()
        }
        Message::TransferInProgress(msg) => {
            state.receive_state.status_message = msg.clone();
            state.add_console_message(msg, ConsoleIcon::Info);
            Command::none()
        }
        Message::TransferCompleted(msg) => {
            state.receive_state.status_message = format!("✅ {}", msg);
            state.transfer_progress = None;
            state.add_console_message(msg, ConsoleIcon::Success);
            Command::none()
        }
        Message::TransferError(msg) => {
            state.receive_state.status_message = format!("❌ {}", msg);
            state.transfer_progress = None;
            state.add_console_message(format!("Transfer error: {}", msg), ConsoleIcon::Error);
            Command::none()
        }

        // Send tab
        Message::PathInputChanged(path) => {
            state.send_state.path_input = path;
            Command::none()
        }
        Message::BrowseFile => Command::perform(
            async {
                rfd::AsyncFileDialog::new()
                    .pick_file()
                    .await
                    .map(|f| f.path().to_path_buf())
            },
            Message::PathSelected,
        ),
        Message::BrowseFolder => Command::perform(
            async {
                rfd::AsyncFileDialog::new()
                    .pick_folder()
                    .await
                    .map(|f| f.path().to_path_buf())
            },
            Message::PathSelected,
        ),
        Message::PathSelected(path) => {
            if let Some(path) = path {
                state.send_state.path_input = path.display().to_string();
                state.send_state.selected_path = Some(path);
            }
            Command::none()
        }
        Message::StartSend => handle_start_send(state),
        Message::SendComplete(msg, bytes_transferred) => {
            state.send_state.status_message = msg.clone();
            state.add_console_message(msg, ConsoleIcon::Success);

            // Log completed transfer to history
            if let Some(mut transfer) = state.current_transfer.take() {
                let file_name = if let Some(progress) = state.transfer_progress.as_ref() {
                    progress.name.clone()
                } else {
                    String::from("unknown")
                };

                transfer.complete(vec![file_name], bytes_transferred);

                if let Ok(mut history) = state.history.lock() {
                    history.add_record(transfer);
                }
            }

            state.transfer_progress = None;
            Command::none()
        }
        Message::SendFailed(msg) => {
            state.send_state.status_message = format!("Error: {}", msg);
            state.transfer_progress = None;
            state.add_console_message(format!("Send failed: {}", msg), ConsoleIcon::Error);

            // Log failed transfer to history
            if let Some(mut transfer) = state.current_transfer.take() {
                transfer.fail(msg.clone());

                if let Ok(mut history) = state.history.lock() {
                    history.add_record(transfer);
                }
            }

            Command::none()
        }

        // Receive tab
        Message::OutputDirChanged(dir) => {
            state.receive_state.output_input = dir;
            Command::none()
        }
        Message::BrowseOutputDir => Command::perform(
            async {
                rfd::AsyncFileDialog::new()
                    .pick_folder()
                    .await
                    .map(|f| f.path().to_path_buf())
            },
            Message::OutputDirSelected,
        ),
        Message::OpenOutputDir => {
            let output_dir = state.receive_state.output_dir.clone();

            if output_dir.exists() {
                // Open the directory in file explorer
                #[cfg(target_os = "windows")]
                let _ = std::process::Command::new("explorer")
                    .arg(&output_dir)
                    .spawn();

                #[cfg(target_os = "macos")]
                let _ = std::process::Command::new("open").arg(&output_dir).spawn();

                #[cfg(target_os = "linux")]
                let _ = std::process::Command::new("xdg-open")
                    .arg(&output_dir)
                    .spawn();

                state.add_console_message(
                    format!("Opened directory: {}", output_dir.display()),
                    ConsoleIcon::Info,
                );
            } else {
                state.add_console_message(
                    format!("Directory does not exist: {}", output_dir.display()),
                    ConsoleIcon::Error,
                );
            }

            Command::none()
        }
        Message::OutputDirSelected(dir) => {
            if let Some(dir) = dir {
                state.receive_state.output_input = dir.display().to_string();
                state.receive_state.output_dir = dir;
            }
            Command::none()
        }
        Message::AutoAcceptToggled(enabled) => {
            state.receive_state.auto_accept = enabled;
            Command::none()
        }
        Message::StartReceive => handle_start_receive(state),
        Message::ReceiveComplete(msg, bytes_transferred) => {
            state.receive_state.status_message = msg.clone();
            state.add_console_message(msg, ConsoleIcon::Success);

            // Log completed transfer to history
            if let Some(mut transfer) = state.current_transfer.take() {
                let file_name = if let Some(progress) = state.transfer_progress.as_ref() {
                    progress.name.clone()
                } else {
                    String::from("received files")
                };

                transfer.complete(vec![file_name], bytes_transferred);

                if let Ok(mut history) = state.history.lock() {
                    history.add_record(transfer);
                }
            }

            state.transfer_progress = None;
            Command::none()
        }
        Message::ReceiveFailed(msg) => {
            state.receive_state.status_message = format!("Error: {}", msg);
            state.transfer_progress = None;
            state.add_console_message(format!("Receive failed: {}", msg), ConsoleIcon::Error);

            // Log failed transfer to history
            if let Some(mut transfer) = state.current_transfer.take() {
                transfer.fail(msg.clone());

                if let Ok(mut history) = state.history.lock() {
                    history.add_record(transfer);
                }
            }

            Command::none()
        }

        // Settings
        Message::CompressionToggled(enabled) => {
            state.settings.compression_enabled = enabled;
            Command::none()
        }
        Message::CompressionLevelChanged(level) => {
            state.settings.compression_level = level;
            Command::none()
        }
        Message::AdaptiveCompressionToggled(enabled) => {
            state.settings.adaptive_compression = enabled;
            Command::none()
        }
        Message::ChunkSizeChanged(size) => {
            state.settings.chunk_size_kb = size;
            Command::none()
        }
        Message::BandwidthLimitChanged(limit) => {
            state.settings.bandwidth_input = limit.clone();
            if let Ok(bw) = p2p_core::bandwidth::parse_bandwidth(&limit) {
                state.settings.bandwidth_limit = bw;
            }
            Command::none()
        }
        Message::MaxRetriesChanged(retries) => {
            state.settings.max_retries = retries;
            Command::none()
        }

        // Progress
        Message::ProgressUpdate {
            transferred,
            total,
            speed,
            eta,
        } => {
            if let Some(progress) = &mut state.transfer_progress {
                progress.transferred_bytes = transferred;
                progress.total_bytes = total;
                progress.speed_bps = speed;
                progress.eta_seconds = eta;
            }
            Command::none()
        }

        // History
        Message::RefreshHistory => Command::none(),

        // Console - handle text editor actions for selection/copy
        Message::ConsoleAction(action) => {
            state.console_content.perform(action);
            Command::none()
        }
    }
}

fn handle_start_connection(state: &mut AppState) -> Command<Message> {
    use crate::state::ConnectionMode;

    match state.connection_state.mode {
        ConnectionMode::Listen => {
            let port = state.connection_state.port.parse::<u16>().unwrap_or(14567);

            state.connection_state.status_message = String::from("Listening");
            state.connection_state.is_active = true;
            state.add_console_message(
                format!("Starting listener on port {}", port),
                ConsoleIcon::Info,
            );

            let device_id = state.connection_state.device_id.unwrap();
            let config = state.settings.to_config_message();
            let output_dir = state.receive_state.output_dir.clone();
            let auto_accept = state.receive_state.auto_accept;

            // Reset cancel flag and clone for background task
            state
                .cancel_listener
                .store(false, std::sync::atomic::Ordering::Relaxed);
            let cancel_flag = Arc::clone(&state.cancel_listener);

            Command::perform(
                async move {
                    // Continuous listening loop
                    let mut count = 0;
                    loop {
                        // Check if cancellation requested
                        if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                            info!("Listener cancelled by user");
                            return Message::ConnectionEstablished(String::from(
                                "Listener stopped",
                            ));
                        }

                        match start_listener_once(
                            port,
                            device_id,
                            config.clone(),
                            output_dir.clone(),
                            auto_accept,
                            count,
                            Arc::clone(&cancel_flag),
                        )
                        .await
                        {
                            Ok((msg, should_continue, new_count)) => {
                                info!("{}", msg);
                                count = new_count;
                                if !should_continue {
                                    return Message::ConnectionEstablished(msg);
                                }
                                // Continue looping to accept next connection
                            }
                            Err(e) => {
                                return Message::ConnectionFailed(e.to_string());
                            }
                        }
                    }
                },
                |msg| msg,
            )
        }
        ConnectionMode::Connect => {
            let address = state.connection_state.peer_address.clone();
            let port = state.connection_state.port.parse::<u16>().unwrap_or(14567);
            let use_discovery = state.connection_state.use_discovery;

            state.connection_state.status_message = String::from("Connecting...");
            state.connection_state.is_active = true;

            if use_discovery {
                state.add_console_message(
                    String::from("Connecting using peer discovery..."),
                    ConsoleIcon::Info,
                );
            } else if !address.is_empty() {
                state.add_console_message(
                    format!("Connecting to {}:{}...", address, port),
                    ConsoleIcon::Info,
                );
            } else {
                state.add_console_message(String::from("Connecting to peer..."), ConsoleIcon::Info);
            }

            let device_id = state.connection_state.device_id.unwrap();
            let config = state.settings.to_config_message();

            Command::perform(
                async move {
                    match connect_to_peer(address, port, use_discovery, device_id, config).await {
                        Ok((session, msg)) => {
                            // Wrap session in Arc<Mutex> and return with message
                            Message::ConnectionEstablishedWithSession(
                                Arc::new(Mutex::new(session)),
                                msg,
                            )
                        }
                        Err(e) => Message::ConnectionFailed(e.to_string()),
                    }
                },
                |msg| msg,
            )
        }
    }
}

fn handle_start_send(state: &mut AppState) -> Command<Message> {
    if state.session.is_none() {
        state.send_state.status_message =
            String::from("Error: Not connected. Please establish a connection first.");
        return Command::none();
    }

    let path = if let Some(ref p) = state.send_state.selected_path {
        p.clone()
    } else if !state.send_state.path_input.is_empty() {
        PathBuf::from(&state.send_state.path_input)
    } else {
        state.send_state.status_message = String::from("Error: No path selected");
        return Command::none();
    };

    if !path.exists() {
        state.send_state.status_message = format!("Error: Path does not exist: {}", path.display());
        state.add_console_message(
            format!("Path does not exist: {}", path.display()),
            ConsoleIcon::Error,
        );
        return Command::none();
    }

    state.send_state.status_message = format!("Sending {}...", path.display());
    state.add_console_message(
        format!("Starting send: {}", path.display()),
        ConsoleIcon::Info,
    );

    // Initialize progress
    state.transfer_progress = Some(crate::state::TransferProgress {
        name: path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string(),
        total_bytes: 0,
        transferred_bytes: 0,
        speed_bps: 0.0,
        eta_seconds: 0,
        is_sending: true,
    });

    // Create transfer record
    let transfer_id = Uuid::new_v4();
    let peer_address = if state.session.is_some() {
        // Would need to get peer address from session, use placeholder for now
        String::from("peer")
    } else {
        String::from("unknown")
    };

    state.current_transfer = Some(p2p_core::history::TransferRecord::new(
        transfer_id,
        p2p_core::history::TransferDirection::Send,
        peer_address,
    ));

    let session = state.session.clone();
    let config = state.settings.to_config_message();
    let max_retries = state.settings.max_retries;

    Command::perform(
        async move {
            match send_path(session, path, config, max_retries).await {
                Ok((msg, bytes)) => Message::SendComplete(msg, bytes),
                Err(e) => Message::SendFailed(e.to_string()),
            }
        },
        |msg| msg,
    )
}

fn handle_start_receive(state: &mut AppState) -> Command<Message> {
    use crate::state::ConnectionMode;

    // In Listen mode, receiving is automatic when you start the connection
    // No need for a separate "Start Receive" action
    if matches!(state.connection_state.mode, ConnectionMode::Listen) {
        state.receive_state.status_message = String::from(
            "Note: In Listen mode, receiving starts automatically when a sender connects.",
        );
        return Command::none();
    }

    if state.session.is_none() {
        state.receive_state.status_message =
            String::from("Error: Not connected. Please establish a connection first.");
        return Command::none();
    }

    let output_dir = state.receive_state.output_dir.clone();
    let auto_accept = state.receive_state.auto_accept;

    state.receive_state.status_message = String::from("Waiting for incoming transfer...");

    // Initialize progress
    state.transfer_progress = Some(crate::state::TransferProgress {
        name: String::from("Incoming transfer"),
        total_bytes: 0,
        transferred_bytes: 0,
        speed_bps: 0.0,
        eta_seconds: 0,
        is_sending: false,
    });

    let session = state.session.clone();

    Command::perform(
        async move {
            match setup_receive(session, output_dir, auto_accept).await {
                Ok((msg, bytes)) => Message::ReceiveComplete(msg, bytes),
                Err(e) => Message::ReceiveFailed(e.to_string()),
            }
        },
        |msg| msg,
    )
}

// ============================================================================
// Async Operations
// ============================================================================

async fn start_listener_once(
    port: u16,
    device_id: Uuid,
    config: ConfigMessage,
    output_dir: PathBuf,
    auto_accept: bool,
    transfer_count: usize,
    cancel_flag: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(String, bool, usize)> {
    let capabilities = Capabilities::all();
    let identity = Arc::new(p2p_core::identity::Identity::load_or_generate()?);

    info!(
        "[Transfer #{}] Waiting for incoming connection on port {} (fp={})...",
        transfer_count + 1,
        port,
        identity.fingerprint_hex(),
    );

    tokio::fs::create_dir_all(&output_dir).await?;

    let session_fut = P2PSession::establish(
        "server",
        None,
        None,
        false,
        port,
        identity,
        device_id,
        capabilities,
        Some(config),
    );

    // Poll the session establishment with periodic cancel checks
    let mut session = tokio::select! {
        result = session_fut => result?,
        _ = async {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }
        } => {
            return Err(anyhow::anyhow!("Listener cancelled"));
        }
    };

    let peer_id = session.peer_device_id();
    info!("🟢 Connection established with peer: {}", peer_id);

    // Start event loop to handle incoming transfers
    info!("Starting event loop for incoming transfers...");
    session
        .run_event_loop(&output_dir, auto_accept, true)
        .await?;

    info!("✅ Transfer complete from peer: {}", peer_id);

    // Return message, continuation flag, and updated count
    Ok((
        format!(
            "✅ Transfer #{} completed. Ready for next connection.",
            transfer_count + 1
        ),
        true,
        transfer_count + 1,
    ))
}

async fn connect_to_peer(
    address: String,
    port: u16,
    use_discovery: bool,
    device_id: Uuid,
    config: ConfigMessage,
) -> Result<(P2PSession, String)> {
    let capabilities = Capabilities::all();
    let identity = Arc::new(p2p_core::identity::Identity::load_or_generate()?);

    info!(
        "Connecting to peer (local fp={})...",
        identity.fingerprint_hex()
    );

    let peer_addr_opt = if !address.is_empty() {
        Some(address)
    } else {
        None
    };

    // Direct `--peer` mode in the GUI needs an explicit fingerprint in a future
    // pass; for now only the discovery path (which pulls the fingerprint from
    // the beacon inside session::establish) works without UI changes.
    let peer_fingerprint = None;

    let session = P2PSession::establish(
        "client",
        peer_addr_opt,
        peer_fingerprint,
        use_discovery,
        port,
        identity,
        device_id,
        capabilities,
        Some(config),
    )
    .await?;

    let peer_id = session.peer_device_id();
    info!("Connection established with peer: {}", peer_id);

    Ok((session, format!("Connected to peer: {}", peer_id)))
}

async fn send_path(
    session: Option<Arc<Mutex<P2PSession>>>,
    path: PathBuf,
    _config: ConfigMessage,
    max_retries: u32,
) -> Result<(String, u64)> {
    let session = session.ok_or_else(|| anyhow::anyhow!("No active session"))?;

    info!("Starting send operation for: {}", path.display());

    let reconnect_config = p2p_core::reconnect::ReconnectConfig {
        max_attempts: max_retries,
        initial_backoff_secs: 3,
        max_backoff_secs: 180,
        exponential: true,
    };

    let transfer_id = Uuid::new_v4();
    let state_file = PathBuf::from(format!("transfer_{}.json", transfer_id));

    let mut progress = p2p_core::progress::ProgressState::new(0);

    let mut session_guard = session.lock().await;

    session_guard
        .send_path(
            &path,
            &reconnect_config,
            Some(&state_file),
            Some(&mut progress),
        )
        .await?;

    drop(session_guard);

    // Get bytes transferred from progress
    let bytes_transferred = progress.transferred_bytes();

    if state_file.exists() {
        let _ = tokio::fs::remove_file(&state_file).await;
    }

    info!("✅ Transfer complete! {} bytes", bytes_transferred);
    Ok((
        format!(
            "✅ Successfully sent: {}",
            path.file_name().unwrap_or_default().to_string_lossy()
        ),
        bytes_transferred,
    ))
}

async fn setup_receive(
    session: Option<Arc<Mutex<P2PSession>>>,
    output_dir: PathBuf,
    auto_accept: bool,
) -> Result<(String, u64)> {
    let session = session.ok_or_else(|| anyhow::anyhow!("No active session"))?;

    info!("Starting receive mode, output: {}", output_dir.display());

    tokio::fs::create_dir_all(&output_dir).await?;

    let mut session_guard = session.lock().await;

    session_guard
        .run_event_loop(&output_dir, auto_accept, true)
        .await?;

    drop(session_guard);

    // TODO: Get actual bytes received from run_event_loop
    // For now, return 0 as placeholder
    let bytes_received = 0u64;

    info!("✅ Receive complete!");
    Ok((
        String::from("✅ Received transfer successfully"),
        bytes_received,
    ))
}
