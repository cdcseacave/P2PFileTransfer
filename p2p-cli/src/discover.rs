//! Discovery operations

use anyhow::Result;
use p2p_core::{discovery::DiscoveryManager, protocol::Capabilities, Uuid};
use std::{net::SocketAddr, sync::Arc, time::Duration};

pub async fn handle_discover(timeout_secs: u64) -> Result<()> {
    println!("🔍 Discovering peers on network...");
    println!("  Timeout: {} seconds", timeout_secs);

    let device_name = format!("cli-{}", &Uuid::new_v4().to_string()[..8]);
    let manager = Arc::new(
        DiscoveryManager::new(
            device_name,
            7778,
            Capabilities::all(),
            Duration::from_secs(10),
        )
        .await?,
    );

    // Start discovery
    let manager_clone = manager.clone();
    let discovery_handle = tokio::spawn(async move {
        let _ = manager_clone.start().await;
    });

    // Wait for discovery period
    tokio::time::sleep(Duration::from_secs(timeout_secs)).await;

    // Get discovered peers
    let peers = manager.get_peers().await;

    println!("\n📡 Discovered {} peer(s):", peers.len());
    for (idx, peer) in peers.iter().enumerate() {
        println!(
            "  [{}] {} - {} ({})",
            idx + 1,
            peer.device_name,
            peer.socket_addr(),
            peer.device_id
        );
    }

    // Cancel discovery
    discovery_handle.abort();

    Ok(())
}

pub async fn discover_and_select_peer(transfer_port: u16) -> Result<SocketAddr> {
    let device_name = format!("cli-{}", &Uuid::new_v4().to_string()[..8]);
    let manager = Arc::new(
        DiscoveryManager::new(
            device_name,
            transfer_port,
            Capabilities::all(),
            Duration::from_secs(10),
        )
        .await?,
    );

    let manager_clone = manager.clone();
    let discovery_handle = tokio::spawn(async move {
        let _ = manager_clone.start().await;
    });

    // Wait a bit for discovery
    tokio::time::sleep(Duration::from_secs(3)).await;

    let peers = manager.get_peers().await;
    discovery_handle.abort();

    if peers.is_empty() {
        anyhow::bail!("No peers discovered. Make sure a peer is running in receive mode.");
    }

    // For now, just use the first peer
    // In a real CLI, we'd prompt the user to select
    Ok(peers[0].socket_addr())
}
