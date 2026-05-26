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
    error::Error,
    protocol::{ConfigMessage, TransferInfo},
    session::P2PSession,
    transfer_folder::AcceptDecision,
    Uuid,
};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// Receive-loop equivalent for the GUI: accept transfers per `auto_accept`,
/// re-accept on peer disconnect, propagate disk errors. Mirrors the CLI
/// loop in p2p-cli/src/receive.rs but without stdin prompting (GUI users
/// flip the auto-accept toggle in the UI). Returns the cumulative byte
/// count across every accepted transfer so the GUI can surface it in
/// history.
async fn run_gui_receive_loop(
    session: &mut P2PSession,
    output_dir: &Path,
    auto_accept: bool,
) -> Result<u64> {
    let policy = move |_info: &TransferInfo| {
        if auto_accept {
            AcceptDecision::Accept
        } else {
            // Without stdin in the GUI, "not auto-accept" currently means
            // reject. A future PR can wire this to a modal dialog.
            AcceptDecision::Reject
        }
    };
    let mut total_bytes = 0u64;
    loop {
        match session.receive_to(output_dir, None, policy, None).await {
            Ok(summary) => {
                info!(
                    "Received {} files ({} bytes)",
                    summary.files.len(),
                    summary.bytes
                );
                total_bytes = total_bytes.saturating_add(summary.bytes);
            }
            Err(e) if matches!(&e, Error::Disconnected | Error::Quic(_)) => {
                info!("Peer disconnected after {total_bytes} bytes");
                return Ok(total_bytes);
            }
            Err(e) => return Err(e.into()),
        }
    }
}

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
        Message::PeerFingerprintChanged(fp) => {
            state.connection_state.peer_fingerprint = fp;
            Command::none()
        }
        Message::RendezvousAddressChanged(addr) => {
            state.connection_state.rendezvous_address = addr;
            Command::none()
        }
        Message::CodeChanged(code) => {
            state.connection_state.code = code;
            Command::none()
        }
        Message::GenerateCode => {
            state.connection_state.code = p2p_core::traversal::generate_code();
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
        // Settings
        Message::CompressionToggled(enabled) => {
            state.settings.compression_enabled = enabled;
            Command::none()
        }
        Message::CompressionLevelChanged(level) => {
            state.settings.compression_level = level;
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
        ConnectionMode::Rendezvous => {
            let rendezvous = state.connection_state.rendezvous_address.trim().to_string();
            let code = state.connection_state.code.trim().to_string();

            if rendezvous.is_empty() || code.is_empty() {
                state.connection_state.is_active = false;
                state.connection_state.status_message = String::from("Idle");
                state.add_console_message(
                    String::from("Enter both a rendezvous server and a code before pairing"),
                    ConsoleIcon::Error,
                );
                return Command::none();
            }

            state.connection_state.status_message = String::from("Pairing...");
            state.connection_state.is_active = true;
            state.add_console_message(
                format!(
                    "Pairing through {rendezvous} with code '{code}' (this may take a moment)..."
                ),
                ConsoleIcon::Info,
            );

            let device_id = state.connection_state.device_id.unwrap();
            let config = state.settings.to_config_message();

            Command::perform(
                async move {
                    match pair_via_rendezvous(rendezvous, code, device_id, config).await {
                        Ok((session, msg)) => Message::ConnectionEstablishedWithSession(
                            Arc::new(Mutex::new(session)),
                            msg,
                        ),
                        Err(e) => Message::ConnectionFailed(e.to_string()),
                    }
                },
                |msg| msg,
            )
        }
        ConnectionMode::Connect => {
            let address = state.connection_state.peer_address.clone();
            let port = state.connection_state.port.parse::<u16>().unwrap_or(14567);
            let use_discovery = state.connection_state.use_discovery;
            let peer_fp_hex = state.connection_state.peer_fingerprint.trim().to_string();

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
                    match connect_to_peer(
                        address,
                        port,
                        use_discovery,
                        peer_fp_hex,
                        device_id,
                        config,
                    )
                    .await
                    {
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
        state.add_console_message(
            String::from("Not connected. Please establish a connection first."),
            ConsoleIcon::Error,
        );
        return Command::none();
    }

    let path = if let Some(ref p) = state.send_state.selected_path {
        p.clone()
    } else if !state.send_state.path_input.is_empty() {
        PathBuf::from(&state.send_state.path_input)
    } else {
        state.add_console_message(String::from("No path selected"), ConsoleIcon::Error);
        return Command::none();
    };

    if !path.exists() {
        state.add_console_message(
            format!("Path does not exist: {}", path.display()),
            ConsoleIcon::Error,
        );
        return Command::none();
    }

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
    let identity = Arc::new(p2p_core::identity::Identity::load_or_generate(None)?);

    info!(
        "[Transfer #{}] Waiting for incoming connection on port {} (fp={})...",
        transfer_count + 1,
        port,
        identity.fingerprint_hex(),
    );

    tokio::fs::create_dir_all(&output_dir).await?;
    let _ = config; // server role doesn't negotiate config until handshake

    let bind_addr: SocketAddr = format!("0.0.0.0:{port}")
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid port {port}: {e}"))?;
    let session_fut = P2PSession::accept(bind_addr, identity, device_id);

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
    let _ = run_gui_receive_loop(&mut session, &output_dir, auto_accept).await?;

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
    peer_fp_hex: String,
    device_id: Uuid,
    config: ConfigMessage,
) -> Result<(P2PSession, String)> {
    let identity = Arc::new(p2p_core::identity::Identity::load_or_generate(None)?);

    info!(
        "Connecting to peer (local fp={})...",
        identity.fingerprint_hex()
    );

    let (peer_addr, peer_fp) = if !address.is_empty() {
        if peer_fp_hex.len() != 64 {
            return Err(anyhow::anyhow!(
                "peer fingerprint must be 64 hex chars, got {}",
                peer_fp_hex.len()
            ));
        }
        let bytes = hex::decode(&peer_fp_hex)
            .map_err(|e| anyhow::anyhow!("invalid peer fingerprint hex: {e}"))?;
        let mut fp = [0u8; 32];
        fp.copy_from_slice(&bytes);
        let parsed = P2PSession::parse_peer_addr(&address, port)?;
        (parsed, fp)
    } else if use_discovery {
        P2PSession::discover_one_peer(port, &identity, device_id).await?
    } else {
        return Err(anyhow::anyhow!(
            "peer address or discovery required for client mode"
        ));
    };

    let session = P2PSession::connect(peer_addr, peer_fp, identity, device_id, config).await?;

    let peer_id = session.peer_device_id();
    info!("Connection established with peer: {}", peer_id);

    Ok((session, format!("Connected to peer: {}", peer_id)))
}

async fn pair_via_rendezvous(
    rendezvous: String,
    code: String,
    device_id: Uuid,
    config: ConfigMessage,
) -> Result<(P2PSession, String)> {
    use std::net::SocketAddr;
    use tokio::net::lookup_host;

    let identity = Arc::new(p2p_core::identity::Identity::load_or_generate(None)?);

    // Default the rendezvous port when only a hostname was supplied.
    let host_port = p2p_core::with_default_port(&rendezvous, p2p_core::DEFAULT_RENDEZVOUS_PORT);
    let rendezvous_addr: SocketAddr = lookup_host(&host_port)
        .await
        .map_err(|e| anyhow::anyhow!("resolving rendezvous '{host_port}': {e}"))?
        .next()
        .ok_or_else(|| anyhow::anyhow!("rendezvous host '{host_port}' resolved to no addresses"))?;

    info!(
        "Pairing through rendezvous {rendezvous_addr} with code '{code}' (local fp={})",
        identity.fingerprint_hex(),
    );

    let session =
        P2PSession::from_rendezvous(rendezvous_addr, code, identity, device_id, config, false)
            .await?;

    let peer_id = session.peer_device_id();
    info!("Rendezvous pairing established with peer: {peer_id}");
    Ok((session, format!("Paired with peer: {peer_id}")))
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
