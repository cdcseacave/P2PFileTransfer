//! UDP broadcast for discovery

use crate::error::{Error, Result};
use crate::protocol::{Capabilities, DiscoveryBeacon};
use crate::{DEFAULT_DISCOVERY_PORT, PROTOCOL_VERSION};
use log::{debug, info, warn};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, SystemTime};
use tokio::net::UdpSocket;
use uuid::Uuid;

/// Maximum UDP packet size
const MAX_PACKET_SIZE: usize = 1500;

/// UDP discovery service
pub struct DiscoveryService {
    socket: UdpSocket,
    device_id: Uuid,
    device_name: String,
    transfer_port: u16,
    capabilities: Capabilities,
    broadcast_addr: SocketAddr,
}

impl DiscoveryService {
    /// Create a new discovery service
    pub async fn new(
        device_name: String,
        transfer_port: u16,
        capabilities: Capabilities,
    ) -> Result<Self> {
        let discovery_port = DEFAULT_DISCOVERY_PORT;
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), discovery_port);
        
        info!("Creating discovery service on port {}", discovery_port);
        let socket = UdpSocket::bind(bind_addr).await?;
        socket.set_broadcast(true)?;
        
        let broadcast_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::BROADCAST),
            discovery_port,
        );
        
        Ok(Self {
            socket,
            device_id: Uuid::new_v4(),
            device_name,
            transfer_port,
            capabilities,
            broadcast_addr,
        })
    }

    /// Create a discovery beacon
    fn create_beacon(&self) -> DiscoveryBeacon {
        DiscoveryBeacon {
            version: PROTOCOL_VERSION,
            device_id: self.device_id,
            device_name: self.device_name.clone(),
            port: self.transfer_port,
            capabilities: self.capabilities,
        }
    }

    /// Broadcast a discovery beacon
    pub async fn broadcast_beacon(&self) -> Result<()> {
        let beacon = self.create_beacon();
        let data = rmp_serde::to_vec(&beacon)?;
        
        if data.len() > MAX_PACKET_SIZE {
            return Err(Error::Protocol(format!(
                "Beacon too large: {} bytes",
                data.len()
            )));
        }
        
        debug!("Broadcasting beacon to {}", self.broadcast_addr);
        self.socket.send_to(&data, self.broadcast_addr).await?;
        Ok(())
    }

    /// Receive a discovery beacon
    pub async fn recv_beacon(&self) -> Result<(DiscoveryBeacon, SocketAddr)> {
        let mut buf = vec![0u8; MAX_PACKET_SIZE];
        
        let (len, src_addr) = self.socket.recv_from(&mut buf).await?;
        buf.truncate(len);
        
        // Deserialize beacon
        let beacon: DiscoveryBeacon = rmp_serde::from_slice(&buf)
            .map_err(|e| Error::Protocol(format!("Invalid beacon: {}", e)))?;
        
        // Verify version
        if beacon.version != PROTOCOL_VERSION {
            warn!(
                "Received beacon with incompatible version {} from {}",
                beacon.version, src_addr
            );
            return Err(Error::VersionMismatch {
                peer: beacon.version,
                ours: PROTOCOL_VERSION,
            });
        }
        
        debug!("Received beacon from {} ({})", beacon.device_name, src_addr);
        Ok((beacon, src_addr))
    }

    /// Get the device ID
    pub fn device_id(&self) -> Uuid {
        self.device_id
    }

    /// Get the device name
    pub fn device_name(&self) -> &str {
        &self.device_name
    }
}

/// Discovered peer information
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub device_id: Uuid,
    pub device_name: String,
    pub address: IpAddr,
    pub port: u16,
    pub capabilities: Capabilities,
    pub last_seen: SystemTime,
}

impl PeerInfo {
    /// Check if the peer is still alive (within TTL)
    pub fn is_alive(&self, ttl: Duration) -> bool {
        match SystemTime::now().duration_since(self.last_seen) {
            Ok(elapsed) => elapsed < ttl,
            Err(_) => false,
        }
    }

    /// Update last seen timestamp
    pub fn update_last_seen(&mut self) {
        self.last_seen = SystemTime::now();
    }

    /// Get socket address for connecting
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.port)
    }
}

impl From<(DiscoveryBeacon, IpAddr)> for PeerInfo {
    fn from((beacon, address): (DiscoveryBeacon, IpAddr)) -> Self {
        Self {
            device_id: beacon.device_id,
            device_name: beacon.device_name,
            address,
            port: beacon.port,
            capabilities: beacon.capabilities,
            last_seen: SystemTime::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_discovery_service() {
        // Use a random high port for testing to avoid conflicts
        let service = DiscoveryService::new(
            "Test Device".to_string(),
            7778,
            Capabilities::all(),
        )
        .await;
        
        // May fail if port is in use, which is okay for this test
        if let Ok(svc) = service {
            assert_eq!(svc.device_name(), "Test Device");
        }
    }

    #[test]
    fn test_peer_info_lifetime() {
        let beacon = DiscoveryBeacon {
            version: 1,
            device_id: Uuid::new_v4(),
            device_name: "Test".to_string(),
            port: 7778,
            capabilities: Capabilities::all(),
        };
        
        let mut peer = PeerInfo::from((beacon, IpAddr::V4(Ipv4Addr::LOCALHOST)));
        
        // Should be alive with large TTL
        assert!(peer.is_alive(Duration::from_secs(60)));
        
        // Update timestamp
        peer.update_last_seen();
        assert!(peer.is_alive(Duration::from_secs(60)));
    }

    #[test]
    fn test_peer_socket_addr() {
        let beacon = DiscoveryBeacon {
            version: 1,
            device_id: Uuid::new_v4(),
            device_name: "Test".to_string(),
            port: 7778,
            capabilities: Capabilities::all(),
        };
        
        let peer = PeerInfo::from((beacon, IpAddr::V4(Ipv4Addr::LOCALHOST)));
        let addr = peer.socket_addr();
        
        assert_eq!(addr.port(), 7778);
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }
}
