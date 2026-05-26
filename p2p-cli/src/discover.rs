//! Discovery operations.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tracing::info;

use p2p_core::{discovery::DiscoveryManager, identity::Identity, Uuid};

pub async fn handle_discover(
    timeout_secs: u64,
    port: u16,
    identity_dir: Option<std::path::PathBuf>,
) -> Result<()> {
    info!("Discovering peers on network...");
    info!("  Timeout: {} seconds", timeout_secs);

    let identity = Identity::load_or_generate(identity_dir.as_deref())?;
    let device_name = format!("cli-{}", &Uuid::new_v4().to_string()[..8]);
    let manager = Arc::new(
        DiscoveryManager::new(
            device_name,
            port,
            identity.fingerprint(),
            Duration::from_secs(10),
        )
        .await?,
    );

    let manager_clone = manager.clone();
    let discovery_handle = tokio::spawn(async move {
        let _ = manager_clone.start().await;
    });

    tokio::time::sleep(Duration::from_secs(timeout_secs)).await;

    let peers = manager.get_peers().await;
    info!("Discovered {} peer(s):", peers.len());
    for (idx, peer) in peers.iter().enumerate() {
        info!(
            "  [{}] {} - {} (id={}, fp={})",
            idx + 1,
            peer.device_name,
            peer.socket_addr(),
            peer.device_id,
            hex::encode(peer.cert_fingerprint),
        );
    }

    discovery_handle.abort();
    Ok(())
}
