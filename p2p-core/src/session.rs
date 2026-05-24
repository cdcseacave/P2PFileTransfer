//! P2P session management.
//!
//! A session is an established, authenticated QUIC connection between two
//! peers. Once the handshake completes, both sides are fully symmetric:
//! either peer can initiate sends or receives over the same connection.
//! The [`ConnectionRole`] is preserved only for `reconnect()` (only the
//! initiator knows where to reconnect to).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::handshake::{HandshakeClient, HandshakeResult, HandshakeServer};
use crate::identity::{Fingerprint, Identity};
use crate::network::quic::{QuicConnection, QuicEndpoint};
use crate::progress::ProgressState;
use crate::protocol::{Capabilities, ConfigMessage};
use crate::transfer_folder::{FolderTransferSession, FolderTransferState};
use crate::traversal::{establish_via_rendezvous, RendezvousParams, DEFAULT_STUN_SERVERS};

/// An established connection plus the parameters needed to resurrect it.
pub struct P2PSession {
    endpoint: QuicEndpoint,
    connection: QuicConnection,
    identity: Arc<Identity>,
    session_id: Uuid,
    device_id: Uuid,
    handshake: HandshakeResult,
    role: ConnectionRole,
    /// For initiators: the peer's address + fingerprint, kept so we can
    /// reconnect after a transient failure.
    initiator_target: Option<(SocketAddr, Fingerprint)>,
}

/// Connection role — only relevant during establishment and reconnection.
/// After handshake, both peers can send and receive on the same connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionRole {
    Initiator,
    Responder,
}

impl P2PSession {
    // ------------------------------------------------------------------
    // Session establishment
    // ------------------------------------------------------------------

    /// Initiate a session to `peer_addr` with `peer_fingerprint` pinned at
    /// the TLS layer.
    pub async fn connect(
        peer_addr: SocketAddr,
        peer_fingerprint: Fingerprint,
        identity: Arc<Identity>,
        device_id: Uuid,
        capabilities: Capabilities,
        config: ConfigMessage,
    ) -> Result<Self> {
        debug!("Creating client session to {}", peer_addr);

        let endpoint = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            identity.clone(),
        )?;
        let mut connection = endpoint.connect(peer_addr, peer_fingerprint).await?;
        trace!("QUIC connection established");

        let handshake_client = HandshakeClient::new(device_id, capabilities, &identity);
        let handshake = handshake_client
            .perform_handshake(&mut connection, config)
            .await?;

        debug!(
            "Session established as initiator (peer: {}, capabilities: {:?})",
            handshake.peer_device_id, handshake.agreed_capabilities
        );

        Ok(Self {
            endpoint,
            connection,
            identity,
            session_id: Uuid::new_v4(),
            device_id,
            handshake,
            role: ConnectionRole::Initiator,
            initiator_target: Some((peer_addr, peer_fingerprint)),
        })
    }

    /// Establish a session via a rendezvous server + shared code.
    ///
    /// Both peers run this with the same `code` and the same
    /// `rendezvous` address. The function binds a UDP socket, runs STUN
    /// on it, exchanges public endpoints + cert fingerprints over the
    /// rendezvous, then races `QuicEndpoint::connect`/`accept` as the
    /// hole-punch. After the QUIC connection is up, both peers run the
    /// application handshake — initiator role is decided by lexical
    /// comparison of cert fingerprints so it's deterministic without
    /// extra coordination.
    pub async fn from_rendezvous(
        rendezvous: SocketAddr,
        code: String,
        identity: Arc<Identity>,
        device_id: Uuid,
        capabilities: Capabilities,
        config: ConfigMessage,
        force_relay: bool,
    ) -> Result<Self> {
        let session = establish_via_rendezvous(RendezvousParams {
            rendezvous,
            code,
            identity: identity.clone(),
            device_id,
            stun_servers: [
                DEFAULT_STUN_SERVERS[0].to_string(),
                DEFAULT_STUN_SERVERS[1].to_string(),
            ],
            force_relay,
        })
        .await?;

        let crate::traversal::EstablishedSession {
            endpoint,
            mut connection,
            peer_endpoint,
            peer_fingerprint,
            peer_device_id,
        } = session;

        // Deterministic initiator/responder split. Compare device IDs
        // (fresh UUIDs per process — always unique even when both
        // peers run on the same machine with a shared identity).
        // Fingerprints would alias when a user pairs themselves;
        // device_id is always fresh.
        let we_initiate = device_id < peer_device_id;
        let handshake = if we_initiate {
            HandshakeClient::new(device_id, capabilities, &identity)
                .perform_handshake(&mut connection, config)
                .await?
        } else {
            HandshakeServer::new(device_id, capabilities, &identity)
                .perform_handshake(&mut connection)
                .await?
        };

        info!(
            "rendezvous session established (peer device {}, addr {peer_endpoint}, capabilities {:?})",
            handshake.peer_device_id, handshake.agreed_capabilities,
        );

        let role = if we_initiate {
            ConnectionRole::Initiator
        } else {
            ConnectionRole::Responder
        };

        // Suppress unused warning when peer_fingerprint isn't needed beyond
        // the handshake result.
        let _ = peer_fingerprint;

        Ok(Self {
            endpoint,
            connection,
            identity,
            session_id: Uuid::new_v4(),
            device_id,
            handshake,
            role,
            // Rendezvous codes are single-use and expire; reconnect()
            // would need a fresh code re-coordinated with the peer.
            // Skip auto-reconnect for traversal sessions in Phase 1.
            initiator_target: None,
        })
    }

    /// Bind to `bind_addr` and accept the next inbound session. Returns the
    /// established session once the handshake completes.
    pub async fn accept(
        bind_addr: SocketAddr,
        identity: Arc<Identity>,
        device_id: Uuid,
        capabilities: Capabilities,
    ) -> Result<Self> {
        let endpoint = QuicEndpoint::bind(bind_addr, identity.clone())?;
        trace!(
            "QUIC server listening on {}, awaiting peer",
            endpoint.local_addr()?
        );

        let mut connection = endpoint.accept().await?;
        trace!("QUIC connection accepted from {}", connection.peer_addr());

        let handshake_server = HandshakeServer::new(device_id, capabilities, &identity);
        let handshake = handshake_server.perform_handshake(&mut connection).await?;

        debug!(
            "Session established as responder (peer: {}, capabilities: {:?})",
            handshake.peer_device_id, handshake.agreed_capabilities
        );

        Ok(Self {
            endpoint,
            connection,
            identity,
            session_id: Uuid::new_v4(),
            device_id,
            handshake,
            role: ConnectionRole::Responder,
            initiator_target: None,
        })
    }

    /// High-level establish: dispatch based on the role string.
    ///
    /// * `role = "client"` — direct `--peer` if `peer_addr` is `Some`, else
    ///   use LAN discovery if `use_discovery` is true.
    /// * `role = "server"` — bind on `0.0.0.0:port` and accept.
    ///
    /// `peer_fingerprint` is required for direct `--peer` mode; LAN discovery
    /// pulls it from the beacon.
    #[allow(clippy::too_many_arguments)]
    pub async fn establish(
        role: &str,
        peer_addr: Option<String>,
        peer_fingerprint: Option<Fingerprint>,
        use_discovery: bool,
        port: u16,
        identity: Arc<Identity>,
        device_id: Uuid,
        capabilities: Capabilities,
        config: Option<ConfigMessage>,
    ) -> Result<Self> {
        if role == "client" {
            let (peer, fp) = if let Some(addr_str) = peer_addr {
                let parsed: SocketAddr = match addr_str.parse() {
                    Ok(sa) => sa,
                    Err(_) => match addr_str.parse::<IpAddr>() {
                        Ok(ip) => SocketAddr::new(ip, port),
                        Err(e) => {
                            return Err(Error::Protocol(format!(
                                "Invalid peer address '{}': {}",
                                addr_str, e
                            )))
                        }
                    },
                };
                let fp = peer_fingerprint.ok_or_else(|| {
                    Error::Protocol(
                        "--peer-fingerprint is required for direct connections".to_string(),
                    )
                })?;
                (parsed, fp)
            } else if use_discovery {
                info!("Using peer discovery on port {}...", port);

                let device_name = format!("p2p-{}", &device_id.to_string()[..8]);
                let manager = Arc::new(
                    crate::discovery::DiscoveryManager::new(
                        device_name,
                        port,
                        capabilities,
                        identity.fingerprint(),
                        Duration::from_secs(10),
                    )
                    .await?,
                );

                let manager_clone = manager.clone();
                let discovery_handle = tokio::spawn(async move {
                    let _ = manager_clone.start().await;
                });

                tokio::time::sleep(Duration::from_secs(3)).await;
                let peers = manager.get_peers().await;
                discovery_handle.abort();

                let peer = peers.into_iter().next().ok_or_else(|| {
                    Error::Protocol(
                        "No peers discovered. Make sure a peer is running in server mode."
                            .to_string(),
                    )
                })?;
                (peer.socket_addr(), peer.cert_fingerprint)
            } else {
                return Err(Error::Protocol(
                    "Peer address or discovery required for client role".to_string(),
                ));
            };

            let cfg = config
                .ok_or_else(|| Error::Protocol("Config required for client role".to_string()))?;
            Self::connect(peer, fp, identity, device_id, capabilities, cfg).await
        } else {
            let bind_addr: SocketAddr = format!("0.0.0.0:{}", port)
                .parse()
                .map_err(|e| Error::Protocol(format!("Invalid port {}: {}", port, e)))?;
            Self::accept(bind_addr, identity, device_id, capabilities).await
        }
    }

    // ------------------------------------------------------------------
    // Transfer operations
    // ------------------------------------------------------------------

    /// Send a file or folder to the peer, with automatic resume + reconnect.
    pub async fn send_path(
        &mut self,
        path: &Path,
        reconnect_config: &crate::reconnect::ReconnectConfig,
        state_path: Option<&Path>,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        if !path.exists() {
            return Err(Error::Protocol(format!(
                "Path does not exist: {}",
                path.display()
            )));
        }

        let mut attempt = 0;

        let mut state = if let Some(state_file) = state_path {
            if state_file.exists() {
                info!("Loading existing transfer state from {:?}", state_file);
                match FolderTransferState::load_from_file(state_file).await {
                    Ok(loaded) => {
                        info!(
                            "Loaded state: {} files total, {} completed ({:.1}% done)",
                            loaded.files.len(),
                            loaded.completed_files.len(),
                            loaded.progress_percentage()
                        );
                        loaded
                    }
                    Err(e) => {
                        warn!("Failed to load state file: {}", e);
                        FolderTransferState::new(Uuid::new_v4(), String::new(), vec![], &self.handshake.config)
                    }
                }
            } else {
                FolderTransferState::new(Uuid::new_v4(), String::new(), vec![], &self.handshake.config)
            }
        } else {
            FolderTransferState::new(Uuid::new_v4(), String::new(), vec![], &self.handshake.config)
        };

        let transfer_id = if state.files.is_empty() {
            Uuid::new_v4()
        } else {
            state.transfer_id
        };

        if !state.files.is_empty() {
            info!("Resuming transfer with ID: {}", transfer_id);
        } else {
            info!("Starting new transfer with ID: {}", transfer_id);
        }

        loop {
            let result = {
                let mut folder_session = FolderTransferSession::new(
                    &mut self.connection,
                    self.handshake.config.clone(),
                    transfer_id,
                );

                folder_session
                    .send(path, &mut state, progress.as_deref_mut())
                    .await
            };

            match result {
                Ok(_) => {
                    if let Some(state_file) = state_path {
                        if state_file.exists() {
                            let _ = tokio::fs::remove_file(state_file).await;
                        }
                    }
                    return Ok(());
                }
                Err(e) => {
                    if !e.is_recoverable() {
                        warn!("Non-recoverable error, not retrying: {}", e);
                        if let Some(state_file) = state_path {
                            let _ = state.save_to_file(state_file).await;
                        }
                        return Err(e);
                    }

                    if !reconnect_config.should_retry(attempt) {
                        warn!(
                            "Max reconnection attempts ({}) reached",
                            reconnect_config.max_attempts
                        );
                        if let Some(state_file) = state_path {
                            let _ = state.save_to_file(state_file).await;
                        }
                        return Err(Error::Protocol(format!(
                            "Transfer failed after {} attempts: {}",
                            attempt + 1,
                            e
                        )));
                    }

                    let delay = reconnect_config.backoff_delay(attempt);
                    warn!(
                        "Recoverable error (attempt {}): {}. Retrying in {:?}...",
                        attempt + 1,
                        e,
                        delay
                    );

                    if let Some(state_file) = state_path {
                        if let Err(save_err) = state.save_to_file(state_file).await {
                            warn!("Failed to save state to disk: {}", save_err);
                        }
                    }

                    tokio::time::sleep(delay).await;

                    info!("Re-establishing connection...");
                    if let Err(reconnect_err) = self.reconnect().await {
                        warn!("Failed to reconnect: {}", reconnect_err);
                    } else {
                        info!("Connection re-established");
                    }
                    attempt += 1;
                }
            }
        }
    }

    /// Receive a file or folder from the peer.
    pub async fn receive_to(
        &mut self,
        output_dir: &Path,
        state_path: Option<&Path>,
        progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        tokio::fs::create_dir_all(output_dir).await?;

        let transfer_id = Uuid::new_v4();
        let mut session = FolderTransferSession::new(
            &mut self.connection,
            self.handshake.config.clone(),
            transfer_id,
        );

        session
            .receive_folder(output_dir, state_path, progress)
            .await
    }

    /// Auto-receive loop: handle incoming transfers until the connection closes.
    pub async fn run_event_loop(
        &mut self,
        output_dir: &Path,
        auto_accept: bool,
        show_progress: bool,
    ) -> Result<()> {
        debug!(
            "Starting session event loop (auto_accept={}, show_progress={})",
            auto_accept, show_progress
        );
        loop {
            let mut progress = if show_progress {
                Some(ProgressState::new(0))
            } else {
                None
            };

            match self.receive_to(output_dir, None, progress.as_mut()).await {
                Ok(_) => {
                    debug!("Transfer completed, awaiting next");
                }
                Err(e) => {
                    if matches!(&e, Error::Disconnected | Error::Quic(_) | Error::Network(_)) {
                        debug!("Connection closed, ending event loop");
                        return Ok(());
                    }
                    return Err(e);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Connection management
    // ------------------------------------------------------------------

    /// Re-establish a dropped session. Only initiators can reconnect because
    /// they hold the peer's address + fingerprint.
    pub async fn reconnect(&mut self) -> Result<()> {
        let (peer_addr, peer_fp) = self
            .initiator_target
            .ok_or_else(|| Error::Protocol("Only initiator sessions can reconnect".to_string()))?;

        info!("Attempting to reconnect to {}", peer_addr);
        let endpoint = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            self.identity.clone(),
        )?;
        let mut new_connection = endpoint.connect(peer_addr, peer_fp).await?;

        let handshake_client = HandshakeClient::new(
            self.device_id,
            self.handshake.agreed_capabilities,
            &self.identity,
        );
        let handshake = handshake_client
            .perform_handshake(&mut new_connection, self.handshake.config.clone())
            .await?;

        info!(
            "Reconnection successful (peer: {}, capabilities: {:?})",
            handshake.peer_device_id, handshake.agreed_capabilities
        );

        self.endpoint = endpoint;
        self.connection = new_connection;
        self.handshake = handshake;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------

    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub fn device_id(&self) -> Uuid {
        self.device_id
    }

    pub fn peer_device_id(&self) -> Uuid {
        self.handshake.peer_device_id
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.connection.peer_addr()
    }

    pub fn peer_fingerprint(&self) -> Fingerprint {
        self.handshake.peer_fingerprint
    }

    pub fn connection_role(&self) -> ConnectionRole {
        self.role
    }

    pub fn config(&self) -> &ConfigMessage {
        &self.handshake.config
    }

    pub fn capabilities(&self) -> &Capabilities {
        &self.handshake.agreed_capabilities
    }

    pub fn is_alive(&self) -> bool {
        true
    }
}
