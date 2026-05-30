//! UDP broadcast for LAN peer discovery.
//!
//! Beacons now carry the sender's certificate fingerprint so the receiver
//! has everything it needs to pin the peer's TLS cert when initiating a
//! QUIC connection.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, SystemTime};

use tokio::net::UdpSocket;
use tracing::{trace, warn};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::identity::Fingerprint;
use crate::protocol::DiscoveryBeacon;
use crate::{DEFAULT_DISCOVERY_PORT, PROTOCOL_VERSION};

const MAX_PACKET_SIZE: usize = 1500;

pub struct DiscoveryService {
    socket: UdpSocket,
    device_id: Uuid,
    device_name: String,
    transfer_port: u16,
    cert_fingerprint: Fingerprint,
    broadcast_addr: SocketAddr,
}

impl DiscoveryService {
    pub async fn new(
        device_name: String,
        transfer_port: u16,
        cert_fingerprint: Fingerprint,
    ) -> Result<Self> {
        let discovery_port = DEFAULT_DISCOVERY_PORT;
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), discovery_port);

        trace!("Creating discovery service on port {}", discovery_port);
        let socket = UdpSocket::bind(bind_addr).await?;
        socket.set_broadcast(true)?;

        let broadcast_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), discovery_port);

        Ok(Self {
            socket,
            device_id: Uuid::new_v4(),
            device_name,
            transfer_port,
            cert_fingerprint,
            broadcast_addr,
        })
    }

    fn create_beacon(&self) -> DiscoveryBeacon {
        DiscoveryBeacon {
            version: PROTOCOL_VERSION,
            device_id: self.device_id,
            device_name: self.device_name.clone(),
            port: self.transfer_port,
            cert_fingerprint: self.cert_fingerprint,
        }
    }

    pub async fn broadcast_beacon(&self) -> Result<()> {
        let beacon = self.create_beacon();
        let data = rmp_serde::to_vec(&beacon)?;

        if data.len() > MAX_PACKET_SIZE {
            return Err(Error::Protocol(format!(
                "Beacon too large: {} bytes",
                data.len()
            )));
        }

        trace!("Broadcasting beacon to {}", self.broadcast_addr);
        self.socket.send_to(&data, self.broadcast_addr).await?;
        Ok(())
    }

    pub async fn recv_beacon(&self) -> Result<(DiscoveryBeacon, SocketAddr)> {
        let mut buf = vec![0u8; MAX_PACKET_SIZE];
        let (len, src_addr) = self.socket.recv_from(&mut buf).await?;
        buf.truncate(len);

        let beacon: DiscoveryBeacon = rmp_serde::from_slice(&buf)
            .map_err(|e| Error::Protocol(format!("Invalid beacon: {}", e)))?;

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

        trace!("Received beacon from {} ({})", beacon.device_name, src_addr);
        Ok((beacon, src_addr))
    }

    pub fn device_id(&self) -> Uuid {
        self.device_id
    }

    pub fn device_name(&self) -> &str {
        &self.device_name
    }
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub device_id: Uuid,
    pub device_name: String,
    pub address: IpAddr,
    pub port: u16,
    pub cert_fingerprint: Fingerprint,
    pub last_seen: SystemTime,
}

impl PeerInfo {
    pub fn is_alive(&self, ttl: Duration) -> bool {
        match SystemTime::now().duration_since(self.last_seen) {
            Ok(elapsed) => elapsed < ttl,
            Err(_) => false,
        }
    }

    pub fn update_last_seen(&mut self) {
        self.last_seen = SystemTime::now();
    }

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
            cert_fingerprint: beacon.cert_fingerprint,
            last_seen: SystemTime::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_beacon() -> DiscoveryBeacon {
        DiscoveryBeacon {
            version: PROTOCOL_VERSION,
            device_id: Uuid::new_v4(),
            device_name: "Test".to_string(),
            port: crate::DEFAULT_TRANSFER_PORT,
            cert_fingerprint: [0u8; 32],
        }
    }

    #[test]
    fn peer_info_lifetime() {
        let mut peer = PeerInfo::from((sample_beacon(), IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(peer.is_alive(Duration::from_secs(60)));
        peer.update_last_seen();
        assert!(peer.is_alive(Duration::from_secs(60)));
    }

    #[test]
    fn peer_socket_addr_matches_beacon_port() {
        let peer = PeerInfo::from((sample_beacon(), IpAddr::V4(Ipv4Addr::LOCALHOST)));
        let addr = peer.socket_addr();
        assert_eq!(addr.port(), crate::DEFAULT_TRANSFER_PORT);
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    }
}
