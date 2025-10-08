//! Discovery operations

use anyhow::Result;
use p2p_core::{discovery::DiscoveryManager, protocol::Capabilities, Uuid};
use std::{sync::Arc, time::Duration};
use tracing::info;

pub async fn handle_discover(timeout_secs: u64, port: u16) -> Result<()> {
    info!("🔍 Discovering peers on network...");
    info!("  Timeout: {} seconds", timeout_secs);

    let device_name = format!("cli-{}", &Uuid::new_v4().to_string()[..8]);
    let manager = Arc::new(
        DiscoveryManager::new(
            device_name,
            port,
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

    info!("📡 Discovered {} peer(s):", peers.len());
    for (idx, peer) in peers.iter().enumerate() {
        info!(
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
