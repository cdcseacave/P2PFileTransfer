//! P2P session management.
//!
//! A session is an established, authenticated QUIC connection between two
//! peers. Once the handshake completes, both sides are fully symmetric:
//! either peer can initiate sends or receives over the same connection.
//! Whether this end is the initiator or responder is captured by
//! `initiator_target`: it's `Some(addr, fp)` on the initiator (which uses
//! it for `reconnect()`) and `None` on the responder (which uses
//! `reaccept()` to keep listening on the same endpoint).

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
use crate::protocol::ConfigMessage;
use crate::transfer_folder::{
    AcceptDecision, FolderTransferSession, FolderTransferState, TransferSummary,
};
use crate::traversal::{establish_via_rendezvous, RendezvousParams, DEFAULT_STUN_SERVERS};

/// An established connection plus the parameters needed to resurrect it.
pub struct P2PSession {
    endpoint: QuicEndpoint,
    connection: QuicConnection,
    identity: Arc<Identity>,
    session_id: Uuid,
    device_id: Uuid,
    handshake: HandshakeResult,
    /// For initiators: the peer's address + fingerprint, kept so we can
    /// reconnect after a transient failure. `None` on the responder.
    initiator_target: Option<(SocketAddr, Fingerprint)>,
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
        config: ConfigMessage,
    ) -> Result<Self> {
        debug!("Creating client session to {}", peer_addr);

        let endpoint = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            identity.clone(),
        )?;
        let mut connection = endpoint.connect(peer_addr, peer_fingerprint).await?;
        trace!("QUIC connection established");

        let handshake_client = HandshakeClient::new(device_id, &identity);
        let handshake = handshake_client
            .perform_handshake(&mut connection, config)
            .await?;

        debug!(
            "Session established as initiator (peer: {})",
            handshake.peer_device_id
        );

        Ok(Self {
            endpoint,
            connection,
            identity,
            session_id: Uuid::new_v4(),
            device_id,
            handshake,
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
    /// application handshake — initiator role is decided by comparing
    /// device IDs so it's deterministic without extra coordination.
    pub async fn from_rendezvous(
        rendezvous: SocketAddr,
        code: String,
        identity: Arc<Identity>,
        device_id: Uuid,
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
            peer_fingerprint: _,
            peer_device_id,
        } = session;

        // Deterministic initiator/responder split: compare device IDs.
        // Fresh per-process UUIDs are always unique even when both peers
        // run on the same machine sharing an identity; fingerprints would
        // alias when a user pairs themselves.
        let we_initiate = device_id < peer_device_id;
        let handshake = if we_initiate {
            HandshakeClient::new(device_id, &identity)
                .perform_handshake(&mut connection, config)
                .await?
        } else {
            HandshakeServer::new(device_id, &identity)
                .perform_handshake(&mut connection)
                .await?
        };

        info!(
            "rendezvous session established (peer device {}, addr {peer_endpoint})",
            handshake.peer_device_id,
        );

        Ok(Self {
            endpoint,
            connection,
            identity,
            session_id: Uuid::new_v4(),
            device_id,
            handshake,
            // Rendezvous codes are single-use and expire; reconnect()
            // would need a fresh code re-coordinated with the peer.
            // Skip auto-reconnect for traversal sessions.
            initiator_target: None,
        })
    }

    /// Bind to `bind_addr` and accept the next inbound session. Returns the
    /// established session once the handshake completes.
    pub async fn accept(
        bind_addr: SocketAddr,
        identity: Arc<Identity>,
        device_id: Uuid,
    ) -> Result<Self> {
        let endpoint = QuicEndpoint::bind(bind_addr, identity.clone())?;
        trace!(
            "QUIC server listening on {}, awaiting peer",
            endpoint.local_addr()?
        );

        let mut connection = endpoint.accept().await?;
        trace!("QUIC connection accepted from {}", connection.peer_addr());

        let handshake_server = HandshakeServer::new(device_id, &identity);
        let handshake = handshake_server.perform_handshake(&mut connection).await?;

        debug!(
            "Session established as responder (peer: {})",
            handshake.peer_device_id
        );

        Ok(Self {
            endpoint,
            connection,
            identity,
            session_id: Uuid::new_v4(),
            device_id,
            handshake,
            initiator_target: None,
        })
    }

    /// Parse a user-supplied peer string (`host:port`, `host`, or bare IP)
    /// into a `SocketAddr`, defaulting to `port` when no port was given.
    pub fn parse_peer_addr(addr_str: &str, port: u16) -> Result<SocketAddr> {
        if let Ok(sa) = addr_str.parse::<SocketAddr>() {
            return Ok(sa);
        }
        if let Ok(ip) = addr_str.parse::<IpAddr>() {
            return Ok(SocketAddr::new(ip, port));
        }
        Err(Error::Protocol(format!(
            "invalid peer address '{addr_str}'"
        )))
    }

    /// Run LAN UDP-beacon discovery for up to ~3 s and return the first
    /// peer that announces itself, plus its cert fingerprint pulled from
    /// the beacon. Used by direct-mode `--discover` and the GUI's
    /// "discover toggle".
    pub async fn discover_one_peer(
        port: u16,
        identity: &Identity,
        device_id: Uuid,
    ) -> Result<(SocketAddr, Fingerprint)> {
        info!("Using peer discovery on port {}...", port);
        let device_name = format!("p2p-{}", &device_id.to_string()[..8]);
        let manager = Arc::new(
            crate::discovery::DiscoveryManager::new(
                device_name,
                port,
                identity.fingerprint(),
                Duration::from_secs(10),
            )
            .await?,
        );

        let manager_clone = manager.clone();
        let handle = tokio::spawn(async move {
            let _ = manager_clone.start().await;
        });

        tokio::time::sleep(Duration::from_secs(3)).await;
        let peers = manager.get_peers().await;
        handle.abort();

        let peer = peers.into_iter().next().ok_or_else(|| {
            Error::Protocol(
                "No peers discovered. Make sure a peer is running in server mode.".to_string(),
            )
        })?;
        Ok((peer.socket_addr(), peer.cert_fingerprint))
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
    ) -> Result<TransferSummary> {
        if !path.exists() {
            return Err(Error::Protocol(format!(
                "Path does not exist: {}",
                path.display()
            )));
        }

        let mut attempt = 0;

        let fresh_state = || {
            FolderTransferState::new(
                Uuid::new_v4(),
                String::new(),
                vec![],
                &self.handshake.config,
            )
        };

        let mut state = match state_path {
            Some(state_file) if state_file.exists() => {
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
                        fresh_state()
                    }
                }
            }
            _ => fresh_state(),
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
                    let summary = TransferSummary {
                        root_name: state.folder_name.clone(),
                        files: state.files.iter().map(|f| f.path.clone()).collect(),
                        bytes: state.total_bytes,
                    };
                    return Ok(summary);
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

    /// Receive a file or folder from the peer. `accept_decision` is
    /// consulted after TransferInfo arrives and before any data flows —
    /// the CLI uses this to honour `--auto-accept` and/or prompt the
    /// user. Returns a `TransferSummary` describing what landed on disk
    /// so callers can record an accurate history entry.
    pub async fn receive_to(
        &mut self,
        output_dir: &Path,
        state_path: Option<&Path>,
        accept_decision: impl FnOnce(&crate::protocol::TransferInfo) -> AcceptDecision,
        progress: Option<&mut ProgressState>,
    ) -> Result<TransferSummary> {
        tokio::fs::create_dir_all(output_dir).await?;

        let transfer_id = Uuid::new_v4();
        let mut session = FolderTransferSession::new(
            &mut self.connection,
            self.handshake.config.clone(),
            transfer_id,
        );

        session
            .receive_folder(output_dir, state_path, accept_decision, progress)
            .await
    }

    /// Re-accept on the existing endpoint and re-perform the handshake.
    /// Used by the receive CLI to keep listening after a peer disconnects
    /// without re-binding (so the user's --port stays stable).
    pub async fn reaccept(&mut self) -> Result<()> {
        if self.initiator_target.is_some() {
            return Err(Error::Protocol(
                "reaccept() is only valid for responder sessions".into(),
            ));
        }
        info!(
            "Re-listening for next peer on {}",
            self.endpoint.local_addr()?
        );
        let mut new_connection = self.endpoint.accept().await?;
        let handshake_server = HandshakeServer::new(self.device_id, &self.identity);
        let handshake = handshake_server
            .perform_handshake(&mut new_connection)
            .await?;
        self.connection = new_connection;
        self.handshake = handshake;
        debug!(
            "Re-established session with new peer ({})",
            self.handshake.peer_device_id
        );
        Ok(())
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

        let handshake_client = HandshakeClient::new(self.device_id, &self.identity);
        let handshake = handshake_client
            .perform_handshake(&mut new_connection, self.handshake.config.clone())
            .await?;

        info!(
            "Reconnection successful (peer: {})",
            handshake.peer_device_id
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

    pub fn config(&self) -> &ConfigMessage {
        &self.handshake.config
    }
}
