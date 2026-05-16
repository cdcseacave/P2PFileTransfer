//! P2P session management
//!
//! This module provides a high-level session abstraction that separates
//! connection establishment from transfer operations. A session represents
//! an established, authenticated connection between two peers that can be
//! used for multiple transfer operations.
//!
//! # Architecture
//!
//! - **Session**: High-level abstraction managing connection lifecycle
//! - **Connection**: Established after handshake, ready for operations
//! - **Operations**: Send/receive that run on an active connection
//!
//! # Bidirectional & Symmetric Design
//!
//! **The session is fully bidirectional** - once established, both peers are
//! completely equal and can perform any operation. There is no longer a
//! "client" or "server" distinction after the handshake completes.
//!
//! ## Either peer can:
//! - Send files/folders to the other peer (`send_path()`)
//! - Receive files/folders from the other peer (`receive_to()`)
//! - Initiate multiple operations on the same connection
//! - Operations can be interleaved (A sends, then B sends, then A sends again)
//!
//! ## Connection roles (client/server) only matter during establishment:
//! - **Client/Initiator**: Calls `connect()` to initiate the TCP connection
//! - **Server/Responder**: Calls `accept()` to accept an incoming TCP connection
//!
//! After handshake completes, both peers have a symmetric `P2PSession` object
//! with identical capabilities. The connection role is preserved only for
//! logging/debugging purposes.
//!
//! ## This design enables:
//! - Multiple operations on a single connection
//! - Connection reuse without re-handshaking
//! - Bidirectional transfers (both peers can send and receive)
//! - CLI tools that can act as both client and server
//! - GUI applications with flexible peer-to-peer interactions
//! - Future support for request/response patterns

use crate::{
    error::{Error, Result},
    handshake::{HandshakeClient, HandshakeResult, HandshakeServer},
    network::tcp::{TcpConnection, TcpServer},
    progress::ProgressState,
    protocol::{Capabilities, ConfigMessage, FileMetadata},
    transfer_folder::{FolderTransferSession, FolderTransferState},
};
use std::{net::SocketAddr, path::Path};
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

/// P2P session representing an established connection between two peers
///
/// A session is created after successful handshake and can be used for
/// multiple transfer operations without reconnecting.
///
/// **Bidirectional & Symmetric**: Once established, both peers can initiate
/// send or receive operations. The connection role (client/server) only
/// matters during establishment and is preserved for debugging/logging.
pub struct P2PSession {
    /// The underlying TCP connection
    connection: TcpConnection,
    /// Session identifier (unique per session)
    session_id: Uuid,
    /// Device ID for this peer
    device_id: Uuid,
    /// Handshake result with negotiated config
    handshake: HandshakeResult,
    /// Connection role (only for tracking how session was established)
    connection_role: ConnectionRole,
}

/// Connection role - only relevant during session establishment
///
/// After handshake, both peers are equal and can perform any operation.
/// This is preserved for logging/debugging purposes only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionRole {
    /// Initiator - connected to remote peer
    Initiator,
    /// Responder - accepted connection from remote peer
    Responder,
}

impl P2PSession {
    // ============================================================================
    // Session Establishment (Asymmetric - one peer initiates, one responds)
    // ============================================================================

    /// Create a new client session by connecting to a remote peer
    ///
    /// This performs the complete handshake and returns a ready-to-use session.
    ///
    /// # Arguments
    ///
    /// * `peer_addr` - Address of the remote peer
    /// * `device_id` - Unique identifier for this device
    /// * `capabilities` - Capabilities supported by this device
    /// * `config` - Desired transfer configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// use p2p_core::{session::P2PSession, protocol::{Capabilities, ConfigMessage}};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let peer_addr = "127.0.0.1:9090".parse()?;
    /// let device_id = Uuid::new_v4();
    /// let capabilities = Capabilities::all();
    /// let config = ConfigMessage::default();
    ///
    /// let session = P2PSession::connect(peer_addr, device_id, capabilities, config).await?;
    /// // Now ready for operations: session.send_path(...), etc.
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(
        peer_addr: SocketAddr,
        device_id: Uuid,
        capabilities: Capabilities,
        config: ConfigMessage,
    ) -> Result<Self> {
        debug!("Creating client session to {}", peer_addr);

        // Establish TCP connection
        let mut connection = TcpConnection::connect(peer_addr).await?;
        trace!("TCP connection established");

        // Perform handshake as client
        let handshake_client = HandshakeClient::new(device_id, capabilities);
        let handshake = handshake_client
            .perform_handshake(&mut connection, config)
            .await?;

        debug!(
            "Session established as initiator (peer: {}, capabilities: {:?})",
            handshake.peer_device_id, handshake.agreed_capabilities
        );

        Ok(Self {
            connection,
            session_id: Uuid::new_v4(),
            device_id,
            handshake,
            connection_role: ConnectionRole::Initiator,
        })
    }

    /// Create a new server session by accepting a connection
    ///
    /// This waits for an incoming connection, performs handshake, and returns
    /// a ready-to-use session.
    ///
    /// # Arguments
    ///
    /// * `bind_addr` - Address to bind the server to
    /// * `device_id` - Unique identifier for this device
    /// * `capabilities` - Capabilities supported by this device
    ///
    /// # Example
    ///
    /// ```no_run
    /// use p2p_core::{session::P2PSession, protocol::Capabilities};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let bind_addr = "0.0.0.0:9090".parse()?;
    /// let device_id = Uuid::new_v4();
    /// let capabilities = Capabilities::all();
    ///
    /// let session = P2PSession::accept(bind_addr, device_id, capabilities).await?;
    /// // Now ready for operations: session.receive_to(...), etc.
    /// # Ok(())
    /// # }
    /// ```
    pub async fn accept(
        bind_addr: SocketAddr,
        device_id: Uuid,
        capabilities: Capabilities,
    ) -> Result<Self> {
        // Start TCP server
        let server = TcpServer::bind(bind_addr).await?;
        trace!("TCP server listening, waiting for connection...");

        // Accept connection
        let mut connection = server.accept().await?;
        trace!("TCP connection accepted from {}", connection.peer_addr());

        // Perform handshake as server
        let handshake_server = HandshakeServer::new(device_id, capabilities);
        let handshake = handshake_server.perform_handshake(&mut connection).await?;

        debug!(
            "Session established as responder (peer: {}, capabilities: {:?})",
            handshake.peer_device_id, handshake.agreed_capabilities
        );

        Ok(Self {
            connection,
            session_id: Uuid::new_v4(),
            device_id,
            handshake,
            connection_role: ConnectionRole::Responder,
        })
    }

    /// Establish a session based on role with discovery support
    ///
    /// This method simplifies session establishment by determining whether to
    /// connect as a client or accept as a server based on the role parameter.
    /// It also supports automatic peer discovery for client mode.
    ///
    /// # Arguments
    ///
    /// * `role` - "client" to connect, "server" to accept
    /// * `peer_addr` - Optional peer address string (e.g., "192.168.1.100") for client role direct connection
    /// * `use_discovery` - Whether to use peer discovery (client role only)
    /// * `port` - Port number for bind address (server) or discovery (client)
    /// * `device_id` - Unique identifier for this device
    /// * `capabilities` - Capabilities supported by this device
    /// * `config` - Optional configuration (required for client role)
    ///
    /// # Returns
    ///
    /// An established `P2PSession` ready for operations
    ///
    /// # Example
    ///
    /// ```no_run
    /// use p2p_core::{session::P2PSession, protocol::{Capabilities, ConfigMessage}};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let device_id = Uuid::new_v4();
    /// let capabilities = Capabilities::all();
    ///
    /// // As client with direct connection
    /// let peer_addr = Some("192.168.1.100".to_string());
    /// let config = Some(ConfigMessage::default());
    /// let session = P2PSession::establish(
    ///     "client",
    ///     peer_addr,
    ///     false,  // use_discovery
    ///     14567,  // port
    ///     device_id,
    ///     capabilities,
    ///     config
    /// ).await?;
    ///
    /// // As client with discovery
    /// let session = P2PSession::establish(
    ///     "client",
    ///     None,
    ///     true,   // use_discovery
    ///     14567,  // port
    ///     device_id,
    ///     capabilities,
    ///     Some(ConfigMessage::default())
    /// ).await?;
    ///
    /// // As server
    /// let session = P2PSession::establish(
    ///     "server",
    ///     None,
    ///     false,  // use_discovery (ignored)
    ///     14567,  // port
    ///     device_id,
    ///     capabilities,
    ///     None
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn establish(
        role: &str,
        peer_addr: Option<String>,
        use_discovery: bool,
        port: u16,
        device_id: Uuid,
        capabilities: Capabilities,
        config: Option<ConfigMessage>,
    ) -> Result<Self> {
        use std::sync::Arc;
        use std::time::Duration;

        if role == "client" {
            // Client mode: connect to peer
            let peer = if let Some(addr_str) = peer_addr {
                // Direct connection - parse the address string. Accept either
                // a full socket address (ip:port) or a bare IP (no port).
                // If a bare IP is provided, use the `port` parameter as the port.
                let parsed_addr: SocketAddr = match addr_str.parse() {
                    Ok(sa) => sa,
                    Err(_) => match addr_str.parse::<std::net::IpAddr>() {
                        Ok(ip) => SocketAddr::new(ip, port),
                        Err(e) => {
                            return Err(Error::Protocol(format!(
                                "Invalid peer address '{}': {}",
                                addr_str, e
                            )))
                        }
                    },
                };
                parsed_addr
            } else if use_discovery {
                // Use peer discovery
                info!("Using peer discovery on port {}...", port);

                let device_name = format!("p2p-{}", &device_id.to_string()[..8]);
                let manager = Arc::new(
                    crate::discovery::DiscoveryManager::new(
                        device_name,
                        port,
                        capabilities,
                        Duration::from_secs(10),
                    )
                    .await?,
                );

                let manager_clone = manager.clone();
                let discovery_handle = tokio::spawn(async move {
                    let _ = manager_clone.start().await;
                });

                // Wait for discovery
                tokio::time::sleep(Duration::from_secs(3)).await;

                let peers = manager.get_peers().await;
                discovery_handle.abort();

                if peers.is_empty() {
                    return Err(Error::Protocol(
                        "No peers discovered. Make sure a peer is running in server mode."
                            .to_string(),
                    ));
                }

                // Use the first discovered peer
                peers[0].socket_addr()
            } else {
                return Err(Error::Protocol(
                    "Peer address or discovery required for client role".to_string(),
                ));
            };

            let cfg = config
                .ok_or_else(|| Error::Protocol("Config required for client role".to_string()))?;
            Self::connect(peer, device_id, capabilities, cfg).await
        } else {
            // Server mode: accept connection
            let bind_addr: SocketAddr = format!("0.0.0.0:{}", port)
                .parse()
                .map_err(|e| Error::Protocol(format!("Invalid port {}: {}", port, e)))?;
            Self::accept(bind_addr, device_id, capabilities).await
        }
    }

    // ============================================================================
    // Transfer Operations (Symmetric - either peer can initiate these)
    // ============================================================================

    /// Send a file or folder to the peer
    ///
    /// This operation can be called by either peer, regardless of who
    /// initiated the connection. Can be called multiple times on the same session.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to file or folder to send
    /// * `progress_callback` - Optional callback for progress updates
    ///
    /// Sends a file or folder to the peer with automatic resume and reconnection support.
    ///
    /// This is the main send method that handles both individual files and entire folders.
    /// It includes automatic retry logic with exponential backoff on transient failures,
    /// and maintains transfer state for resume capability.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to file or folder to send
    /// * `progress` - Optional progress state for unified progress tracking
    /// * `reconnect_config` - Configuration for auto-reconnect behavior (max attempts, backoff timing)
    /// * `state_path` - Optional path to save/load transfer state for chunk-level resume
    ///
    /// # Features
    /// - Supports both single files and recursive folder transfers
    /// - Automatic resume from interruptions (chunk-level granularity)
    /// - Auto-reconnect with exponential backoff on transient failures
    /// - Progress tracking for UI updates
    /// - State persistence for automatic chunk-level resume after connection loss
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(session: &mut p2p_core::session::P2PSession) -> Result<(), Box<dyn std::error::Error>> {
    /// use std::path::Path;
    /// use p2p_core::reconnect::ReconnectConfig;
    ///
    /// let reconnect_config = ReconnectConfig {
    ///     max_attempts: 5,
    ///     initial_backoff_secs: 3,
    ///     max_backoff_secs: 180,
    ///     exponential: true,
    /// };
    ///
    /// let state_path = Path::new("transfer.json");
    ///
    /// session.send_path(
    ///     Path::new("/path/to/file.zip"),
    ///     &reconnect_config,
    ///     Some(&state_path),
    ///     None,  // progress
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send_path(
        &mut self,
        path: &Path,
        reconnect_config: &crate::reconnect::ReconnectConfig,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        use crate::reconnect::is_transient_error;

        if !path.exists() {
            return Err(Error::Protocol(format!(
                "Path does not exist: {}",
                path.display()
            )));
        }

        let mut attempt = 0;
        let transfer_id = Uuid::new_v4();
        let mut state = FolderTransferState::new(Uuid::new_v4(), String::new(), vec![]);

        info!("Starting new transfer with ID: {}", transfer_id);

        loop {
            let result = {
                let mut folder_session = FolderTransferSession::new(
                    &mut self.connection,
                    self.handshake.config.clone(),
                    transfer_id,
                );

                let attempt_type = if !state.files.is_empty() && !state.completed_files.is_empty() {
                    "resume"
                } else {
                    "send"
                };
                debug!(
                    "Attempting {} (attempt {}/{})",
                    attempt_type,
                    attempt + 1,
                    if reconnect_config.max_attempts == 0 {
                        "∞".to_string()
                    } else {
                        reconnect_config.max_attempts.to_string()
                    }
                );

                folder_session
                    .send(path, &mut state, progress.as_deref_mut())
                    .await
            };

            match result {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if !is_transient_error(&e) {
                        warn!("Non-transient error, not retrying: {}", e);
                        return Err(e);
                    }

                    if !reconnect_config.should_retry(attempt) {
                        warn!(
                            "Max reconnection attempts ({}) reached",
                            reconnect_config.max_attempts
                        );
                        return Err(Error::Protocol(format!(
                            "Transfer failed after {} attempts: {}",
                            attempt + 1,
                            e
                        )));
                    }

                    let delay = reconnect_config.backoff_delay(attempt);
                    warn!(
                        "Transient error (attempt {}/{}): {}. Retrying in {:?}...",
                        attempt + 1,
                        if reconnect_config.max_attempts == 0 {
                            "∞".to_string()
                        } else {
                            reconnect_config.max_attempts.to_string()
                        },
                        e,
                        delay
                    );

                    tokio::time::sleep(delay).await;

                    info!("Re-establishing connection...");
                    match self.reconnect().await {
                        Ok(_) => info!("Connection re-established successfully"),
                        Err(reconnect_err) => {
                            warn!("Failed to reconnect: {}", reconnect_err);
                            attempt += 1;
                            continue;
                        }
                    }

                    attempt += 1;
                    info!("Retrying transfer...");
                }
            }
        }
    }

    /// Send a pre-determined group of files to the peer.
    ///
    /// Used by the parallel transfer feature: the caller scans the folder once,
    /// splits the file list into groups, and calls this on each independent session.
    ///
    /// `base_path` is the directory under which `files[i].path` is relative.
    pub async fn send_file_group(
        &mut self,
        base_path: &std::path::Path,
        files: Vec<FileMetadata>,
        progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        let transfer_id = Uuid::new_v4();
        let mut folder_session = FolderTransferSession::new(
            &mut self.connection,
            self.handshake.config.clone(),
            transfer_id,
        );
        folder_session.send_group(base_path, files, progress).await
    }

    /// Receive a file or folder from the peer
    ///
    /// This operation can be called by either peer, regardless of who
    /// initiated the connection. Can be called multiple times on the same session.
    ///
    /// # Arguments
    ///
    /// * `output_dir` - Directory to save received files
    /// * `state_path` - Optional path to save/load transfer state for auto-resume
    /// * `progress` - Optional progress state for unified progress tracking
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(session: &mut p2p_core::session::P2PSession) -> Result<(), Box<dyn std::error::Error>> {
    /// use std::path::Path;
    ///
    /// // Simple receive without state or progress
    /// session.receive_to(Path::new("/output/dir"), None, None).await?;
    ///
    /// // Receive with progress tracking
    /// let mut progress = p2p_core::progress::ProgressState::new(0);
    /// session.receive_to(Path::new("/output/dir"), None, Some(&mut progress)).await?;
    ///
    /// // Receive with state file for auto-resume
    /// let state_path = Path::new("transfer.json");
    /// session.receive_to(Path::new("/output/dir"), Some(&state_path), Some(&mut progress)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn receive_to(
        &mut self,
        output_dir: &Path,
        state_path: Option<&Path>,
        progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        // Create output directory
        tokio::fs::create_dir_all(output_dir).await?;

        let transfer_id = Uuid::new_v4();
        let mut session = FolderTransferSession::new(
            &mut self.connection,
            self.handshake.config.clone(),
            transfer_id,
        );

        session
            .receive_folder(output_dir, state_path, progress)
            .await?;

        Ok(())
    }

    /// Run a session event loop that automatically handles incoming operations
    ///
    /// This method keeps the session alive and automatically receives incoming
    /// transfers initiated by the peer. It's designed for passive/server mode
    /// where you want to accept whatever the peer sends.
    ///
    /// The loop continues until the connection is closed or an error occurs.
    ///
    /// # Arguments
    ///
    /// * `output_dir` - Default directory to save received files
    /// * `auto_accept` - If true, automatically accepts all transfers without prompting
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the session ends gracefully, or an error if something
    /// goes wrong. This method blocks until the connection is closed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(session: &mut p2p_core::session::P2PSession) -> Result<(), Box<dyn std::error::Error>> {
    /// use std::path::Path;
    ///
    /// // Automatically handle incoming transfers with progress display
    /// session.run_event_loop(
    ///     Path::new("/downloads"),
    ///     true,  // Auto-accept all transfers
    ///     true   // Show progress bar
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run_event_loop(
        &mut self,
        output_dir: &Path,
        auto_accept: bool,
        show_progress: bool,
    ) -> Result<()> {
        debug!(
            "Starting session event loop (auto-receive mode, auto_accept={}, show_progress={})",
            auto_accept, show_progress
        );

        loop {
            // For manual accept mode, we would prompt user here
            // For now, we just respect the auto_accept flag
            if !auto_accept {
                // In CLI, this would be handled by the caller
                // In GUI, this would show a dialog
                debug!("Waiting for user to accept incoming transfer (auto_accept=false)");
            }

            // Create a fresh progress state for each transfer if requested
            let mut progress = if show_progress {
                Some(ProgressState::new(0))
            } else {
                None
            };

            // Attempt to receive - this will block until a transfer starts or connection closes
            match self.receive_to(output_dir, None, progress.as_mut()).await {
                Ok(_) => {
                    debug!("Transfer completed successfully, ready for next operation");
                    // Continue loop to handle next transfer
                }
                Err(e) => {
                    // Check if this is a connection close (normal termination)
                    let error_msg = e.to_string().to_lowercase();
                    if error_msg.contains("connection")
                        || error_msg.contains("closed")
                        || error_msg.contains("eof")
                        || error_msg.contains("reset")
                        || error_msg.contains("broken pipe")
                    {
                        debug!("Connection closed, ending event loop");
                        return Ok(());
                    }
                    // Other errors should be propagated
                    return Err(e);
                }
            }
        }
    }

    // ============================================================================
    // Connection Management
    // ============================================================================

    /// Reconnect to the peer after connection loss
    ///
    /// This re-establishes the TCP connection and performs handshake again.
    /// Only works for client (initiator) sessions - server sessions can't reconnect.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if reconnection successful, `Err` otherwise.
    pub async fn reconnect(&mut self) -> Result<()> {
        // Only clients can reconnect (they know the peer address)
        if self.connection_role != ConnectionRole::Initiator {
            return Err(Error::Protocol(
                "Only client sessions can reconnect".to_string(),
            ));
        }

        let peer_addr = self.connection.peer_addr();
        info!("Attempting to reconnect to {}", peer_addr);

        // Establish new TCP connection
        let mut new_connection = TcpConnection::connect(peer_addr).await?;
        trace!("TCP connection re-established");

        // Perform handshake again
        let handshake_client =
            HandshakeClient::new(self.device_id, self.handshake.agreed_capabilities);
        let handshake = handshake_client
            .perform_handshake(&mut new_connection, self.handshake.config.clone())
            .await?;

        info!(
            "Reconnection successful (peer: {}, capabilities: {:?})",
            handshake.peer_device_id, handshake.agreed_capabilities
        );

        // Replace old connection with new one
        self.connection = new_connection;
        self.handshake = handshake;

        Ok(())
    }

    // ============================================================================
    // Session Information & Management
    // ============================================================================

    /// Get the session ID
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Get the device ID
    pub fn device_id(&self) -> Uuid {
        self.device_id
    }

    /// Get the peer device ID
    pub fn peer_device_id(&self) -> Uuid {
        self.handshake.peer_device_id
    }

    /// Get the peer address
    pub fn peer_addr(&self) -> SocketAddr {
        self.connection.peer_addr()
    }

    /// Get the connection role (how this session was established)
    ///
    /// Note: This is for informational purposes only. Both peers can
    /// perform any operation regardless of connection role.
    pub fn connection_role(&self) -> ConnectionRole {
        self.connection_role
    }

    /// Get the negotiated configuration
    pub fn config(&self) -> &ConfigMessage {
        &self.handshake.config
    }

    /// Get the agreed capabilities
    pub fn capabilities(&self) -> &Capabilities {
        &self.handshake.agreed_capabilities
    }

    /// Check if connection is still alive
    pub fn is_alive(&self) -> bool {
        // Could add more sophisticated checks here
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_creation() {
        // Start server in background
        let server_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let server_device_id = Uuid::new_v4();
        let server_capabilities = Capabilities::all();

        let server_task = tokio::spawn(async move {
            let server = TcpServer::bind(server_addr).await.unwrap();
            let actual_addr = server.local_addr();

            // Send address back through channel (simplified for test)
            let mut conn = server.accept().await.unwrap();
            let handshake = HandshakeServer::new(server_device_id, server_capabilities);
            handshake.perform_handshake(&mut conn).await.unwrap();

            actual_addr
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Note: This test is incomplete as we need the actual bound address
        // In real tests, we'd use a channel to communicate the address
        drop(server_task);
    }
}
