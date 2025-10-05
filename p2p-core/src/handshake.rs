//! Connection handshake protocol

use crate::error::{Error, Result};
use crate::network::tcp::TcpConnection;
use crate::protocol::{Capabilities, ConfigMessage, HelloMessage, Message, TransferInfo};
use crate::{MIN_PROTOCOL_VERSION, PROTOCOL_VERSION};
use log::{debug, info};
use uuid::Uuid;

/// Handshake result containing negotiated parameters
#[derive(Debug, Clone)]
pub struct HandshakeResult {
    pub peer_device_id: Uuid,
    pub peer_capabilities: Capabilities,
    pub agreed_capabilities: Capabilities,
    pub config: ConfigMessage,
}

/// Handshake client (initiator)
pub struct HandshakeClient {
    device_id: Uuid,
    capabilities: Capabilities,
}

impl HandshakeClient {
    /// Create a new handshake client
    pub fn new(device_id: Uuid, capabilities: Capabilities) -> Self {
        Self {
            device_id,
            capabilities,
        }
    }

    /// Perform the complete handshake as initiator
    pub async fn perform_handshake(
        &self,
        conn: &mut TcpConnection,
        config: ConfigMessage,
    ) -> Result<HandshakeResult> {
        info!("Starting handshake with {}", conn.peer_addr());

        // Step 1: Send HELLO
        debug!("Sending HELLO");
        let hello = Message::Hello(HelloMessage {
            protocol_version: PROTOCOL_VERSION,
            min_version: MIN_PROTOCOL_VERSION,
            device_id: self.device_id,
            capabilities: self.capabilities,
        });
        conn.send_message(&hello).await?;

        // Step 2: Receive HELLO_ACK
        debug!("Waiting for HELLO_ACK");
        let peer_hello = match conn.recv_message().await? {
            Message::HelloAck(h) => h,
            Message::Error(e) => {
                return Err(Error::Protocol(format!("Handshake error: {}", e.message)))
            }
            msg => return Err(Error::Protocol(format!("Expected HelloAck, got {:?}", msg))),
        };

        // Step 3: Verify protocol version compatibility
        if peer_hello.protocol_version < MIN_PROTOCOL_VERSION
            || peer_hello.protocol_version > PROTOCOL_VERSION
        {
            return Err(Error::VersionMismatch {
                peer: peer_hello.protocol_version,
                ours: PROTOCOL_VERSION,
            });
        }

        // Step 4: Negotiate capabilities
        let agreed_capabilities = self.capabilities.intersect(&peer_hello.capabilities);
        debug!("Agreed capabilities: {:?}", agreed_capabilities);

        // Step 5: Send CONFIG
        debug!("Sending CONFIG");
        conn.send_message(&Message::Config(config.clone())).await?;

        // Step 6: Receive CONFIG_ACK
        debug!("Waiting for CONFIG_ACK");
        match conn.recv_message().await? {
            Message::ConfigAck => {}
            Message::Error(e) => {
                return Err(Error::Protocol(format!("Config rejected: {}", e.message)))
            }
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected ConfigAck, got {:?}",
                    msg
                )))
            }
        }

        info!("Handshake completed successfully");
        Ok(HandshakeResult {
            peer_device_id: peer_hello.device_id,
            peer_capabilities: peer_hello.capabilities,
            agreed_capabilities,
            config,
        })
    }

    /// Send transfer information
    pub async fn send_transfer_info(
        &self,
        conn: &mut TcpConnection,
        info: TransferInfo,
    ) -> Result<()> {
        debug!("Sending TRANSFER_INFO");
        conn.send_message(&Message::TransferInfo(info)).await?;

        debug!("Waiting for READY");
        match conn.recv_message().await? {
            Message::Ready => Ok(()),
            Message::Error(e) => Err(Error::Protocol(format!("Transfer rejected: {}", e.message))),
            msg => Err(Error::Protocol(format!("Expected Ready, got {:?}", msg))),
        }
    }
}

/// Handshake server (responder)
pub struct HandshakeServer {
    device_id: Uuid,
    capabilities: Capabilities,
}

impl HandshakeServer {
    /// Create a new handshake server
    pub fn new(device_id: Uuid, capabilities: Capabilities) -> Self {
        Self {
            device_id,
            capabilities,
        }
    }

    /// Perform the complete handshake as responder
    pub async fn perform_handshake(&self, conn: &mut TcpConnection) -> Result<HandshakeResult> {
        info!("Starting handshake with {}", conn.peer_addr());

        // Step 1: Receive HELLO
        debug!("Waiting for HELLO");
        let peer_hello = match conn.recv_message().await? {
            Message::Hello(h) => h,
            msg => return Err(Error::Protocol(format!("Expected Hello, got {:?}", msg))),
        };

        // Step 2: Verify protocol version
        if peer_hello.protocol_version < MIN_PROTOCOL_VERSION
            || peer_hello.min_version > PROTOCOL_VERSION
        {
            return Err(Error::VersionMismatch {
                peer: peer_hello.protocol_version,
                ours: PROTOCOL_VERSION,
            });
        }

        // Step 3: Send HELLO_ACK
        debug!("Sending HELLO_ACK");
        let hello_ack = Message::HelloAck(HelloMessage {
            protocol_version: PROTOCOL_VERSION,
            min_version: MIN_PROTOCOL_VERSION,
            device_id: self.device_id,
            capabilities: self.capabilities,
        });
        conn.send_message(&hello_ack).await?;

        // Step 4: Negotiate capabilities
        let agreed_capabilities = self.capabilities.intersect(&peer_hello.capabilities);
        debug!("Agreed capabilities: {:?}", agreed_capabilities);

        // Step 5: Receive CONFIG
        debug!("Waiting for CONFIG");
        let config = match conn.recv_message().await? {
            Message::Config(c) => c,
            msg => return Err(Error::Protocol(format!("Expected Config, got {:?}", msg))),
        };

        // Step 6: Validate and send CONFIG_ACK
        if config.compression_enabled && !agreed_capabilities.has_compression() {
            return Err(Error::UnsupportedCapability(
                "Compression not supported".to_string(),
            ));
        }

        debug!("Sending CONFIG_ACK");
        conn.send_message(&Message::ConfigAck).await?;

        info!("Handshake completed successfully");
        Ok(HandshakeResult {
            peer_device_id: peer_hello.device_id,
            peer_capabilities: peer_hello.capabilities,
            agreed_capabilities,
            config,
        })
    }

    /// Receive transfer information
    pub async fn recv_transfer_info(&self, conn: &mut TcpConnection) -> Result<TransferInfo> {
        debug!("Waiting for TRANSFER_INFO");
        let info = match conn.recv_message().await? {
            Message::TransferInfo(i) => i,
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected TransferInfo, got {:?}",
                    msg
                )))
            }
        };

        debug!("Sending READY");
        conn.send_message(&Message::Ready).await?;

        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::tcp::TcpServer;

    #[tokio::test]
    async fn test_handshake_flow() {
        // Start server
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server_addr = server.local_addr();

        // Spawn server task
        let server_task = tokio::spawn(async move {
            let mut conn = server.accept().await.unwrap();
            let handshake_server = HandshakeServer::new(Uuid::new_v4(), Capabilities::all());
            handshake_server.perform_handshake(&mut conn).await.unwrap()
        });

        // Client performs handshake
        let mut client_conn = TcpConnection::connect(server_addr).await.unwrap();
        let handshake_client = HandshakeClient::new(Uuid::new_v4(), Capabilities::all());

        let config = ConfigMessage::default();

        let client_result = handshake_client
            .perform_handshake(&mut client_conn, config)
            .await
            .unwrap();

        let server_result = server_task.await.unwrap();

        // Verify both sides agree
        assert_eq!(client_result.config.compression_enabled, true);
        assert_eq!(server_result.config.compression_enabled, true);
        assert!(client_result.agreed_capabilities.has_compression());
        assert!(server_result.agreed_capabilities.has_compression());
    }
}
