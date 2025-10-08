//! TCP connection management

use crate::error::{Error, Result};
use crate::network::{read_message, write_message};
use crate::protocol::Message;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{debug, info, trace, warn};

/// TCP connection with keepalive support
pub struct TcpConnection {
    stream: TcpStream,
    peer_addr: SocketAddr,
    last_activity: Instant,
    keepalive_interval: Duration,
}

impl TcpConnection {
    /// Create a new TCP connection from a stream
    pub fn new(stream: TcpStream, peer_addr: SocketAddr) -> Self {
        Self {
            stream,
            peer_addr,
            last_activity: Instant::now(),
            keepalive_interval: Duration::from_secs(5),
        }
    }

    /// Connect to a remote peer
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        info!("Connecting to {}", addr);
        let stream = timeout(Duration::from_secs(10), TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(Error::Network)?;

        stream.set_nodelay(true)?;

        info!("Connected to {}", addr);
        Ok(Self::new(stream, addr))
    }

    /// Send a message to the peer
    pub async fn send_message(&mut self, message: &Message) -> Result<()> {
        trace!("Sending message to {}: {:?}", self.peer_addr, message);
        write_message(&mut self.stream, message).await?;
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Receive a message from the peer with timeout
    pub async fn recv_message(&mut self) -> Result<Message> {
        let msg = timeout(Duration::from_secs(30), read_message(&mut self.stream))
            .await
            .map_err(|_| Error::Timeout)??;

        self.last_activity = Instant::now();
        trace!("Received message from {}: {:?}", self.peer_addr, msg);
        Ok(msg)
    }

    /// Send a keepalive ping
    pub async fn send_ping(&mut self) -> Result<()> {
        trace!("Sending ping to {}", self.peer_addr);
        self.send_message(&Message::Ping).await
    }

    /// Check if keepalive ping should be sent
    pub fn should_send_keepalive(&self) -> bool {
        self.last_activity.elapsed() >= self.keepalive_interval
    }

    /// Get the peer address
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Get time since last activity
    pub fn time_since_last_activity(&self) -> Duration {
        self.last_activity.elapsed()
    }

    /// Set keepalive interval
    pub fn set_keepalive_interval(&mut self, interval: Duration) {
        self.keepalive_interval = interval;
    }
}

/// TCP listener for accepting connections
pub struct TcpServer {
    listener: TcpListener,
    local_addr: SocketAddr,
}

impl TcpServer {
    /// Create a new TCP server
    pub async fn bind(addr: SocketAddr) -> Result<Self> {
        debug!("Binding TCP server to {}", addr);
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;

        let server = Self {
            listener,
            local_addr,
        };

        // Log all reachable addresses if bound to wildcard
        let reachable_addrs = server.reachable_addrs();
        let addr_strings: Vec<String> = reachable_addrs.iter().map(|a| a.to_string()).collect();
        info!(
            "TCP server listening on {} (reachable via: {})",
            local_addr,
            addr_strings.join(", ")
        );

        Ok(server)
    }

    /// Accept a new connection
    pub async fn accept(&self) -> Result<TcpConnection> {
        let (stream, peer_addr) = self.listener.accept().await?;
        info!("Accepted connection from {}", peer_addr);

        stream.set_nodelay(true)?;

        Ok(TcpConnection::new(stream, peer_addr))
    }

    /// Get the local address
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Get all reachable addresses for this server
    ///
    /// If the server is bound to 0.0.0.0 (all interfaces), this returns a list
    /// of all local IP addresses where the server can be reached.
    /// Otherwise, returns just the bound address.
    pub fn reachable_addrs(&self) -> Vec<SocketAddr> {
        let port = self.local_addr.port();

        // If not bound to wildcard address, just return the local address
        if !self.local_addr.ip().is_unspecified() {
            return vec![self.local_addr];
        }

        // Get all network interfaces
        Self::list_local_addrs(port)
    }

    /// List all local IP addresses with the given port
    ///
    /// This is useful when binding to 0.0.0.0 to discover all addresses
    /// where the server is reachable.
    pub fn list_local_addrs(port: u16) -> Vec<SocketAddr> {
        use std::net::IpAddr;

        let mut addrs = Vec::new();

        // Try to get network interfaces
        if let Ok(interfaces) = local_ip_address::list_afinet_netifas() {
            for (name, ip) in interfaces {
                // Skip loopback unless it's the only interface
                if ip.is_loopback() {
                    continue;
                }

                // Filter out link-local IPv6 addresses (fe80::/10)
                if let IpAddr::V6(ipv6) = ip {
                    if (ipv6.segments()[0] & 0xffc0) == 0xfe80 {
                        continue;
                    }
                }

                debug!("Found network interface '{}' with IP: {}", name, ip);
                addrs.push(SocketAddr::new(ip, port));
            }
        }

        // Always include localhost as fallback
        if addrs.is_empty() {
            addrs.push(SocketAddr::new(IpAddr::from([127, 0, 0, 1]), port));
        }

        // Sort addresses: IPv4 first, then IPv6
        addrs.sort_by_key(|addr| match addr {
            SocketAddr::V4(_) => 0,
            SocketAddr::V6(_) => 1,
        });

        addrs
    }
}

/// Connection manager with auto-reconnect support
pub struct ConnectionManager {
    peer_addr: SocketAddr,
    connection: Option<TcpConnection>,
    max_retries: u32,
    retry_count: u32,
}

impl ConnectionManager {
    /// Create a new connection manager
    pub fn new(peer_addr: SocketAddr, max_retries: u32) -> Self {
        Self {
            peer_addr,
            connection: None,
            max_retries,
            retry_count: 0,
        }
    }

    /// Connect or reconnect to the peer
    pub async fn connect(&mut self) -> Result<()> {
        if self.retry_count >= self.max_retries {
            return Err(Error::Other(format!(
                "Max reconnection attempts ({}) exceeded",
                self.max_retries
            )));
        }

        match TcpConnection::connect(self.peer_addr).await {
            Ok(conn) => {
                self.connection = Some(conn);
                self.retry_count = 0;
                Ok(())
            }
            Err(e) => {
                self.retry_count += 1;
                let backoff = Duration::from_secs(2u64.pow(self.retry_count.min(5)));
                warn!(
                    "Connection failed (attempt {}/{}): {}. Retrying in {:?}",
                    self.retry_count, self.max_retries, e, backoff
                );
                tokio::time::sleep(backoff).await;
                Err(e)
            }
        }
    }

    /// Get a mutable reference to the connection
    pub fn connection_mut(&mut self) -> Result<&mut TcpConnection> {
        self.connection.as_mut().ok_or(Error::Disconnected)
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    /// Disconnect
    pub fn disconnect(&mut self) {
        self.connection = None;
    }

    /// Get retry count
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Capabilities, HelloMessage};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_tcp_server_bind() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let server = TcpServer::bind(addr).await.unwrap();
        assert!(server.local_addr().port() > 0);
    }

    #[tokio::test]
    async fn test_tcp_connection() {
        // Start server
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = server.local_addr();

        // Spawn server task
        let server_task = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let msg = conn.recv_message().await.unwrap();
            conn.send_message(&msg).await.unwrap();
        });

        // Connect client
        let mut client = TcpConnection::connect(server_addr).await.unwrap();

        // Send hello message
        let hello = Message::Hello(HelloMessage {
            protocol_version: 1,
            min_version: 1,
            device_id: Uuid::new_v4(),
            capabilities: Capabilities::all(),
        });

        client.send_message(&hello).await.unwrap();
        let response = client.recv_message().await.unwrap();

        // Verify echo
        match response {
            Message::Hello(_) => {}
            _ => panic!("Expected Hello message"),
        }

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_keepalive_check() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let server = TcpServer::bind(addr).await.unwrap();
        let mut conn = TcpConnection::connect(server.local_addr()).await.unwrap();

        // Initially should not need keepalive
        assert!(!conn.should_send_keepalive());

        // Set short interval for testing
        conn.set_keepalive_interval(Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Now should need keepalive
        assert!(conn.should_send_keepalive());
    }
}
