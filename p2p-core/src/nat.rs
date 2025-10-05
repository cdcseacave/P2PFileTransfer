//! NAT traversal (hole punching) implementation
//!
//! This module implements UDP hole punching to enable P2P connections
//! between peers behind NAT/firewall. The approach:
//!
//! 1. Each peer discovers their public IP:port via STUN
//! 2. Peers exchange their public endpoints via a rendezvous server
//! 3. Both peers simultaneously send UDP packets to each other's public endpoint
//! 4. NAT devices create bidirectional mappings
//! 5. Once hole is punched, upgrade to TCP connection
//!
//! ## STUN Protocol
//!
//! We implement a minimal STUN client (RFC 5389) that:
//! - Sends BINDING requests to public STUN servers
//! - Parses BINDING responses to extract public IP:port
//! - Handles XOR-MAPPED-ADDRESS attributes
//!
//! ## Hole Punching Process
//!
//! ```text
//! Peer A (behind NAT)          Rendezvous Server          Peer B (behind NAT)
//!      |                              |                          |
//!      |------ STUN query ----------->|                          |
//!      |<----- Public A:portA --------|                          |
//!      |                              |<------ STUN query -------|
//!      |                              |------ Public B:portB --->|
//!      |                              |                          |
//!      |-- Register A:portA --------->|                          |
//!      |                              |<-- Register B:portB -----|
//!      |                              |                          |
//!      |<--- Get B:portB -------------|                          |
//!      |                              |---- Get A:portA -------->|
//!      |                              |                          |
//!      |=========== Simultaneous UDP packets ===================>|
//!      |<========== Establish bidirectional UDP =================|
//!      |                              |                          |
//!      |=========== Upgrade to TCP connection ==================>|
//! ```

use crate::error::{Error, Result};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default STUN servers (Google's public STUN servers)
pub const DEFAULT_STUN_SERVERS: &[&str] = &[
    "stun.l.google.com:19302",
    "stun1.l.google.com:19302",
    "stun2.l.google.com:19302",
    "stun3.l.google.com:19302",
    "stun4.l.google.com:19302",
];

/// STUN message types
const BINDING_REQUEST: u16 = 0x0001;
const BINDING_RESPONSE: u16 = 0x0101;

/// STUN magic cookie (RFC 5389)
const MAGIC_COOKIE: u32 = 0x2112A442;

/// STUN attribute types
const ATTR_MAPPED_ADDRESS: u16 = 0x0001;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;

/// NAT type detection results
#[derive(Debug, Clone, PartialEq)]
pub enum NatType {
    /// No NAT - direct internet connection
    Open,
    /// Full cone NAT - any external host can send packets
    FullCone,
    /// Restricted cone NAT - only contacted hosts can reply
    RestrictedCone,
    /// Port restricted cone NAT - only contacted host:port can reply
    PortRestrictedCone,
    /// Symmetric NAT - different mapping per destination (hardest to traverse)
    Symmetric,
    /// Could not determine NAT type
    Unknown,
}

/// Public endpoint information from STUN
#[derive(Debug, Clone)]
pub struct PublicEndpoint {
    /// Public IP address
    pub ip: IpAddr,
    /// Public port
    pub port: u16,
    /// NAT type
    pub nat_type: NatType,
}

impl PublicEndpoint {
    /// Create a socket address from the public endpoint
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip, self.port)
    }
}

/// STUN client for discovering public IP and port
pub struct StunClient {
    stun_servers: Vec<String>,
    timeout: Duration,
}

impl StunClient {
    /// Create a new STUN client with default servers
    pub fn new() -> Self {
        Self {
            stun_servers: DEFAULT_STUN_SERVERS.iter().map(|s| s.to_string()).collect(),
            timeout: Duration::from_secs(3),
        }
    }

    /// Create a STUN client with custom servers
    pub fn with_servers(servers: Vec<String>) -> Self {
        Self {
            stun_servers: servers,
            timeout: Duration::from_secs(3),
        }
    }

    /// Set the timeout for STUN requests
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Discover public endpoint by querying STUN servers
    pub fn discover_public_endpoint(&self) -> Result<PublicEndpoint> {
        for stun_server in &self.stun_servers {
            match self.query_stun_server(stun_server) {
                Ok(endpoint) => {
                    info!("Discovered public endpoint via {}: {:?}", stun_server, endpoint);
                    return Ok(endpoint);
                }
                Err(e) => {
                    warn!("Failed to query STUN server {}: {}", stun_server, e);
                    continue;
                }
            }
        }
        
        Err(Error::Network(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to discover public endpoint from any STUN server"
        )))
    }

    /// Query a single STUN server
    fn query_stun_server(&self, server: &str) -> Result<PublicEndpoint> {
        // Create UDP socket bound to any available port
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_read_timeout(Some(self.timeout))?;
        
        let local_addr = socket.local_addr()?;
        debug!("Local socket bound to: {}", local_addr);

        // Build STUN BINDING request
        let request = self.build_binding_request();
        
        // Send request to STUN server
        socket.send_to(&request, server)?;
        debug!("Sent BINDING request to {}", server);

        // Receive response
        let mut buffer = vec![0u8; 1024];
        let (len, _) = socket.recv_from(&mut buffer)?;
        buffer.truncate(len);

        // Parse response
        self.parse_binding_response(&buffer, local_addr)
    }

    /// Build a STUN BINDING request packet
    fn build_binding_request(&self) -> Vec<u8> {
        let mut packet = Vec::new();
        
        // Message Type (2 bytes): BINDING REQUEST
        packet.extend_from_slice(&BINDING_REQUEST.to_be_bytes());
        
        // Message Length (2 bytes): 0 (no attributes)
        packet.extend_from_slice(&0u16.to_be_bytes());
        
        // Magic Cookie (4 bytes)
        packet.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
        
        // Transaction ID (12 bytes) - random
        let transaction_id: [u8; 12] = rand::random();
        packet.extend_from_slice(&transaction_id);
        
        packet
    }

    /// Parse a STUN BINDING response packet
    fn parse_binding_response(&self, data: &[u8], local_addr: SocketAddr) -> Result<PublicEndpoint> {
        if data.len() < 20 {
            return Err(Error::Protocol("STUN response too short".to_string()));
        }

        // Verify message type
        let msg_type = u16::from_be_bytes([data[0], data[1]]);
        if msg_type != BINDING_RESPONSE {
            return Err(Error::Protocol(format!(
                "Expected BINDING RESPONSE, got message type: 0x{:04x}",
                msg_type
            )));
        }

        // Parse message length
        let msg_length = u16::from_be_bytes([data[2], data[3]]) as usize;
        
        // Verify magic cookie
        let cookie = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        if cookie != MAGIC_COOKIE {
            return Err(Error::Protocol("Invalid STUN magic cookie".to_string()));
        }

        // Extract transaction ID for XOR operations
        let transaction_id = &data[8..20];

        // Parse attributes
        let mut offset = 20;
        let end = 20 + msg_length;
        
        while offset < end {
            if offset + 4 > data.len() {
                break;
            }

            let attr_type = u16::from_be_bytes([data[offset], data[offset + 1]]);
            let attr_length = u16::from_be_bytes([data[offset + 2], data[offset + 3]]) as usize;
            offset += 4;

            if offset + attr_length > data.len() {
                break;
            }

            let attr_data = &data[offset..offset + attr_length];

            match attr_type {
                ATTR_XOR_MAPPED_ADDRESS => {
                    if let Ok(endpoint) = self.parse_xor_mapped_address(attr_data, transaction_id) {
                        let nat_type = self.detect_nat_type(&endpoint, &local_addr);
                        return Ok(PublicEndpoint {
                            ip: endpoint.ip(),
                            port: endpoint.port(),
                            nat_type,
                        });
                    }
                }
                ATTR_MAPPED_ADDRESS => {
                    if let Ok(endpoint) = self.parse_mapped_address(attr_data) {
                        let nat_type = self.detect_nat_type(&endpoint, &local_addr);
                        return Ok(PublicEndpoint {
                            ip: endpoint.ip(),
                            port: endpoint.port(),
                            nat_type,
                        });
                    }
                }
                _ => {
                    // Unknown attribute, skip
                    debug!("Skipping unknown STUN attribute: 0x{:04x}", attr_type);
                }
            }

            // Move to next attribute (with padding to 4-byte boundary)
            offset += (attr_length + 3) & !3;
        }

        Err(Error::Protocol("No address attribute found in STUN response".to_string()))
    }

    /// Parse XOR-MAPPED-ADDRESS attribute
    fn parse_xor_mapped_address(&self, data: &[u8], transaction_id: &[u8]) -> Result<SocketAddr> {
        if data.len() < 8 {
            return Err(Error::Protocol("XOR-MAPPED-ADDRESS too short".to_string()));
        }

        let family = data[1];
        let xor_port = u16::from_be_bytes([data[2], data[3]]);
        
        // XOR port with most significant 16 bits of magic cookie
        let port = xor_port ^ (MAGIC_COOKIE >> 16) as u16;

        match family {
            0x01 => {
                // IPv4
                if data.len() < 8 {
                    return Err(Error::Protocol("XOR-MAPPED-ADDRESS IPv4 data too short".to_string()));
                }
                
                let xor_addr = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
                let addr = xor_addr ^ MAGIC_COOKIE;
                let ip = Ipv4Addr::from(addr);
                
                Ok(SocketAddr::new(IpAddr::V4(ip), port))
            }
            0x02 => {
                // IPv6 - XOR with magic cookie + transaction ID
                if data.len() < 20 {
                    return Err(Error::Protocol("XOR-MAPPED-ADDRESS IPv6 data too short".to_string()));
                }
                
                let mut xor_key = Vec::new();
                xor_key.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
                xor_key.extend_from_slice(transaction_id);
                
                let mut addr_bytes = [0u8; 16];
                for i in 0..16 {
                    addr_bytes[i] = data[4 + i] ^ xor_key[i];
                }
                
                let ip = std::net::Ipv6Addr::from(addr_bytes);
                Ok(SocketAddr::new(IpAddr::V6(ip), port))
            }
            _ => Err(Error::Protocol(format!("Unknown address family: {}", family))),
        }
    }

    /// Parse MAPPED-ADDRESS attribute (non-XOR)
    fn parse_mapped_address(&self, data: &[u8]) -> Result<SocketAddr> {
        if data.len() < 8 {
            return Err(Error::Protocol("MAPPED-ADDRESS too short".to_string()));
        }

        let family = data[1];
        let port = u16::from_be_bytes([data[2], data[3]]);

        match family {
            0x01 => {
                // IPv4
                let addr = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
                let ip = Ipv4Addr::from(addr);
                Ok(SocketAddr::new(IpAddr::V4(ip), port))
            }
            0x02 => {
                // IPv6
                if data.len() < 20 {
                    return Err(Error::Protocol("MAPPED-ADDRESS IPv6 data too short".to_string()));
                }
                let mut addr_bytes = [0u8; 16];
                addr_bytes.copy_from_slice(&data[4..20]);
                let ip = std::net::Ipv6Addr::from(addr_bytes);
                Ok(SocketAddr::new(IpAddr::V6(ip), port))
            }
            _ => Err(Error::Protocol(format!("Unknown address family: {}", family))),
        }
    }

    /// Detect NAT type by comparing public and local addresses
    fn detect_nat_type(&self, public: &SocketAddr, local: &SocketAddr) -> NatType {
        if public.ip() == local.ip() {
            // Public IP matches local IP - no NAT
            NatType::Open
        } else {
            // Behind NAT - would need multiple STUN queries to different servers
            // to fully determine NAT type. For now, assume restricted cone.
            NatType::RestrictedCone
        }
    }
}

impl Default for StunClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_binding_request() {
        let client = StunClient::new();
        let request = client.build_binding_request();
        
        // Verify structure
        assert_eq!(request.len(), 20); // Header only, no attributes
        
        // Verify message type
        let msg_type = u16::from_be_bytes([request[0], request[1]]);
        assert_eq!(msg_type, BINDING_REQUEST);
        
        // Verify magic cookie
        let cookie = u32::from_be_bytes([request[4], request[5], request[6], request[7]]);
        assert_eq!(cookie, MAGIC_COOKIE);
    }

    #[test]
    fn test_parse_xor_mapped_address() {
        let client = StunClient::new();
        
        // Create test data for 192.0.2.1:32853
        // XOR with magic cookie: 0x2112A442
        let port = 32853u16;
        let xor_port = port ^ (MAGIC_COOKIE >> 16) as u16;
        
        let ip = 0xC0000201u32; // 192.0.2.1
        let xor_ip = ip ^ MAGIC_COOKIE;
        
        let mut data = vec![0u8, 0x01]; // Reserved, Family (IPv4)
        data.extend_from_slice(&xor_port.to_be_bytes());
        data.extend_from_slice(&xor_ip.to_be_bytes());
        
        let transaction_id = [0u8; 12];
        let result = client.parse_xor_mapped_address(&data, &transaction_id).unwrap();
        
        assert_eq!(result.port(), port);
        assert_eq!(result.ip(), IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)));
    }

    #[test]
    fn test_nat_type_detection() {
        let client = StunClient::new();
        
        let local = "192.168.1.100:5000".parse().unwrap();
        let public_nat = "203.0.113.5:5000".parse().unwrap();
        let public_open = "192.168.1.100:5000".parse().unwrap();
        
        assert_eq!(client.detect_nat_type(&public_open, &local), NatType::Open);
        assert_eq!(client.detect_nat_type(&public_nat, &local), NatType::RestrictedCone);
    }
}
