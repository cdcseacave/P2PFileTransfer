//! Integration tests for P2P networking

use p2p_core::{
    discovery::DiscoveryManager,
    handshake::{HandshakeClient, HandshakeServer},
    network::tcp::{TcpConnection, TcpServer},
    protocol::{Capabilities, ConfigMessage},
    Uuid,
};
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, info};

#[tokio::test]
async fn test_full_connection_flow() {
    // This test simulates a complete connection flow:
    // 1. Server starts listening
    // 2. Client connects
    // 3. Handshake is performed
    // 4. Both sides verify the connection

    // Start server
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("Failed to bind server");
    let server_addr = server.local_addr();
    info!("Server listening on {}", server_addr);

    // Spawn server task
    let server_handle = tokio::spawn(async move {
        info!("Server: Waiting for connection...");
        let mut conn = server.accept().await.expect("Failed to accept connection");
        info!("Server: Connection accepted from {}", conn.peer_addr());

        let handshake = HandshakeServer::new(Uuid::new_v4(), Capabilities::all());
        let result = handshake
            .perform_handshake(&mut conn)
            .await
            .expect("Server handshake failed");

        info!("Server: Handshake complete");
        result
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client connects
    let mut client_conn = TcpConnection::connect(server_addr)
        .await
        .expect("Failed to connect");

    let handshake = HandshakeClient::new(Uuid::new_v4(), Capabilities::all());
    let config = ConfigMessage::default();

    let client_result = handshake
        .perform_handshake(&mut client_conn, config)
        .await
        .expect("Client handshake failed");
    info!("Client: Handshake complete");

    let server_result = server_handle.await.expect("Server task failed");

    // Verify both sides agree
    assert!(client_result.config.compression_enabled);
    assert!(server_result.config.compression_enabled);
    assert!(client_result.agreed_capabilities.has_compression());
    assert!(server_result.agreed_capabilities.has_compression());

    info!("✅ Full connection flow test passed!");
}

#[tokio::test]
async fn test_discovery_timeout() {
    // Test that discovery manager handles timeouts correctly
    let result = timeout(
        Duration::from_secs(1),
        DiscoveryManager::new(
            "Test Device".to_string(),
            p2p_core::DEFAULT_TRANSFER_PORT,
            Capabilities::all(),
            Duration::from_secs(10),
        ),
    )
    .await;

    // Should complete within timeout (even if it fails to bind)
    assert!(result.is_ok());
    info!("✅ Discovery timeout test passed!");
}

#[tokio::test]
async fn test_concurrent_connections() {
    // Test multiple concurrent connections
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("Failed to bind server");
    let server_addr = server.local_addr();

    // Spawn server to accept multiple connections
    let server_handle = tokio::spawn(async move {
        let mut connections = Vec::new();
        for i in 0..3 {
            let mut conn = server.accept().await.expect("Failed to accept");
            info!("Server: Accepted connection {}", i);

            let handshake = HandshakeServer::new(Uuid::new_v4(), Capabilities::all());
            handshake
                .perform_handshake(&mut conn)
                .await
                .expect("Handshake failed");

            connections.push(conn);
        }
        connections.len()
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn 3 clients concurrently
    let mut client_handles = Vec::new();
    for i in 0..3 {
        let handle = tokio::spawn(async move {
            let mut conn = TcpConnection::connect(server_addr)
                .await
                .expect("Failed to connect");
            debug!("Client {}: Connected", i);

            let handshake = HandshakeClient::new(Uuid::new_v4(), Capabilities::all());
            let config = ConfigMessage::default();

            handshake
                .perform_handshake(&mut conn, config)
                .await
                .expect("Handshake failed");

            info!("Client {}: Handshake complete", i);
        });
        client_handles.push(handle);
    }

    // Wait for all clients
    for handle in client_handles {
        handle.await.expect("Client task failed");
    }

    // Wait for server
    let connection_count = server_handle.await.expect("Server task failed");
    assert_eq!(connection_count, 3);

    info!("✅ Concurrent connections test passed!");
}

#[tokio::test]
async fn test_capability_negotiation() {
    // Test capability negotiation between incompatible peers
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let server_addr = server.local_addr();

    // Server with limited capabilities
    let server_handle = tokio::spawn(async move {
        let mut conn = server.accept().await.unwrap();

        // Server only supports compression, not resume
        let capabilities = Capabilities::new().with_compression();
        let handshake = HandshakeServer::new(Uuid::new_v4(), capabilities);

        handshake.perform_handshake(&mut conn).await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client with all capabilities
    let mut client_conn = TcpConnection::connect(server_addr).await.unwrap();
    let handshake = HandshakeClient::new(Uuid::new_v4(), Capabilities::all());
    let config = ConfigMessage::default();

    let client_result = handshake
        .perform_handshake(&mut client_conn, config)
        .await
        .unwrap();

    let server_result = server_handle.await.unwrap();

    // Both should agree on compression only
    assert!(client_result.agreed_capabilities.has_compression());
    assert!(!client_result.agreed_capabilities.has_resume());
    assert!(server_result.agreed_capabilities.has_compression());
    assert!(!server_result.agreed_capabilities.has_resume());

    info!("✅ Capability negotiation test passed!");
}
