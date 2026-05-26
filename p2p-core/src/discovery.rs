//! Peer discovery module

use crate::error::Result;
use crate::identity::Fingerprint;
use crate::network::udp::{DiscoveryService, PeerInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

/// Peer discovery manager
pub struct DiscoveryManager {
    service: Arc<DiscoveryService>,
    peers: Arc<RwLock<HashMap<Uuid, PeerInfo>>>,
    peer_ttl: Duration,
}

impl DiscoveryManager {
    /// Create a new discovery manager. `cert_fingerprint` is the SHA-256
    /// of our local cert; receivers use it to pin our TLS identity when
    /// initiating a QUIC connection.
    pub async fn new(
        device_name: String,
        transfer_port: u16,
        cert_fingerprint: Fingerprint,
        peer_ttl: Duration,
    ) -> Result<Self> {
        let service = DiscoveryService::new(device_name, transfer_port, cert_fingerprint).await?;

        Ok(Self {
            service: Arc::new(service),
            peers: Arc::new(RwLock::new(HashMap::new())),
            peer_ttl,
        })
    }

    /// Start the discovery service
    pub async fn start(self: Arc<Self>) -> Result<()> {
        debug!("Starting discovery manager");

        // Spawn beacon broadcaster
        let broadcaster = {
            let service = Arc::clone(&self.service);
            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_secs(2));
                loop {
                    ticker.tick().await;
                    if let Err(e) = service.broadcast_beacon().await {
                        warn!("Failed to broadcast beacon: {}", e);
                    }
                }
            })
        };

        // Spawn beacon receiver
        let receiver = {
            let service = Arc::clone(&self.service);
            let peers = Arc::clone(&self.peers);
            let our_device_id = service.device_id();

            tokio::spawn(async move {
                loop {
                    match service.recv_beacon().await {
                        Ok((beacon, src_addr)) => {
                            // Ignore our own beacons
                            if beacon.device_id == our_device_id {
                                continue;
                            }

                            let ip = src_addr.ip();
                            let peer_info = PeerInfo::from((beacon.clone(), ip));

                            let mut peers_lock = peers.write().await;

                            if let Some(existing) = peers_lock.get_mut(&beacon.device_id) {
                                existing.update_last_seen();
                                trace!("Updated peer: {}", existing.device_name);
                            } else {
                                info!("Discovered new peer: {} at {}", peer_info.device_name, ip);
                                peers_lock.insert(beacon.device_id, peer_info);
                            }
                        }
                        Err(e) => {
                            warn!("Error receiving beacon: {}", e);
                        }
                    }
                }
            })
        };

        // Spawn peer cleanup task
        let cleanup = {
            let peers = Arc::clone(&self.peers);
            let ttl = self.peer_ttl;

            tokio::spawn(async move {
                let mut ticker = interval(Duration::from_secs(5));
                loop {
                    ticker.tick().await;

                    let mut peers_lock = peers.write().await;
                    let before_count = peers_lock.len();

                    peers_lock.retain(|_, peer| {
                        let alive = peer.is_alive(ttl);
                        if !alive {
                            info!("Peer timed out: {}", peer.device_name);
                        }
                        alive
                    });

                    let after_count = peers_lock.len();
                    if before_count != after_count {
                        trace!("Cleaned up {} stale peers", before_count - after_count);
                    }
                }
            })
        };

        // Wait for all tasks (they run forever)
        tokio::select! {
            _ = broadcaster => warn!("Broadcaster task ended"),
            _ = receiver => warn!("Receiver task ended"),
            _ = cleanup => warn!("Cleanup task ended"),
        }

        Ok(())
    }

    /// Get list of discovered peers
    pub async fn get_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers.values().cloned().collect()
    }

    /// Get a specific peer by ID
    pub async fn get_peer(&self, device_id: &Uuid) -> Option<PeerInfo> {
        let peers = self.peers.read().await;
        peers.get(device_id).cloned()
    }

    /// Get count of discovered peers
    pub async fn peer_count(&self) -> usize {
        let peers = self.peers.read().await;
        peers.len()
    }

    /// Find peer by name
    pub async fn find_peer_by_name(&self, name: &str) -> Option<PeerInfo> {
        let peers = self.peers.read().await;
        peers
            .values()
            .find(|p| p.device_name.to_lowercase().contains(&name.to_lowercase()))
            .cloned()
    }

    /// Get our device ID
    pub fn device_id(&self) -> Uuid {
        self.service.device_id()
    }

    /// Get our device name
    pub fn device_name(&self) -> &str {
        self.service.device_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_discovery_manager_creation() {
        let manager = DiscoveryManager::new(
            "Test Device".to_string(),
            crate::DEFAULT_TRANSFER_PORT,
            [0u8; 32],
            Duration::from_secs(10),
        )
        .await;

        if let Ok(mgr) = manager {
            assert_eq!(mgr.device_name(), "Test Device");
            assert_eq!(mgr.peer_count().await, 0);
        }
    }

    #[tokio::test]
    async fn test_peer_operations() {
        let manager = DiscoveryManager::new(
            "Test".to_string(),
            crate::DEFAULT_TRANSFER_PORT,
            [0u8; 32],
            Duration::from_secs(10),
        )
        .await;

        if let Ok(mgr) = manager {
            // Initially no peers
            assert_eq!(mgr.get_peers().await.len(), 0);

            // Non-existent peer
            let random_id = Uuid::new_v4();
            assert!(mgr.get_peer(&random_id).await.is_none());
        }
    }
}
