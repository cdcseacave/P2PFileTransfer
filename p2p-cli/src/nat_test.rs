//! NAT traversal test operations

use anyhow::Result;
use p2p_core::nat::{NatType, StunClient};
use tracing::info;

pub async fn handle_nat_test(stun_server: Option<String>) -> Result<()> {
    info!("🔌 Testing NAT traversal...");
    info!("");

    // Create STUN client
    let client = if let Some(server) = stun_server {
        info!("  Using STUN server: {}", server);
        StunClient::with_servers(vec![server])
    } else {
        info!("  Using default STUN servers (Google public STUN)");
        StunClient::new()
    };

    // Discover public endpoint
    info!("  Querying STUN server...");
    match client.discover_public_endpoint() {
        Ok(endpoint) => {
            info!("");
            info!("✅ Successfully discovered public endpoint:");
            info!("  Public IP:   {}", endpoint.ip);
            info!("  Public Port: {}", endpoint.port);
            info!("  NAT Type:    {:?}", endpoint.nat_type);
            info!("");

            match endpoint.nat_type {
                NatType::Open => {
                    info!("📡 No NAT detected - you have a direct internet connection.");
                    info!("   P2P connections should work without hole punching.");
                }
                NatType::FullCone | NatType::RestrictedCone | NatType::PortRestrictedCone => {
                    info!("🔓 Cone NAT detected - hole punching should work!");
                    info!("   You can establish P2P connections with most peers.");
                }
                NatType::Symmetric => {
                    info!("🔒 Symmetric NAT detected - hole punching may be difficult.");
                    info!("   P2P connections may require a relay server (TURN).");
                }
                NatType::Unknown => {
                    info!("❓ Could not determine NAT type.");
                    info!("   Try using --to <address> for direct connections.");
                }
            }

            Ok(())
        }
        Err(e) => {
            info!("");
            info!("❌ Failed to discover public endpoint: {}", e);
            info!("");
            info!("Possible reasons:");
            info!("  • No internet connection");
            info!("  • Firewall blocking UDP traffic");
            info!("  • STUN server unavailable");
            info!("");
            info!("Try:");
            info!("  • Check your internet connection");
            info!("  • Use a different STUN server with --stun-server <server:port>");
            info!("  • Check firewall settings");

            Err(e.into())
        }
    }
}
