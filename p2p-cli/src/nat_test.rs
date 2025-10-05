//! NAT traversal test operations

use anyhow::Result;
use p2p_core::nat::{NatType, StunClient};

pub async fn handle_nat_test(stun_server: Option<String>) -> Result<()> {
    println!("🔌 Testing NAT traversal...");
    println!();

    // Create STUN client
    let client = if let Some(server) = stun_server {
        println!("  Using STUN server: {}", server);
        StunClient::with_servers(vec![server])
    } else {
        println!("  Using default STUN servers (Google public STUN)");
        StunClient::new()
    };

    // Discover public endpoint
    println!("  Querying STUN server...");
    match client.discover_public_endpoint() {
        Ok(endpoint) => {
            println!();
            println!("✅ Successfully discovered public endpoint:");
            println!("  Public IP:   {}", endpoint.ip);
            println!("  Public Port: {}", endpoint.port);
            println!("  NAT Type:    {:?}", endpoint.nat_type);
            println!();

            match endpoint.nat_type {
                NatType::Open => {
                    println!("📡 No NAT detected - you have a direct internet connection.");
                    println!("   P2P connections should work without hole punching.");
                }
                NatType::FullCone | NatType::RestrictedCone | NatType::PortRestrictedCone => {
                    println!("🔓 Cone NAT detected - hole punching should work!");
                    println!("   You can establish P2P connections with most peers.");
                }
                NatType::Symmetric => {
                    println!("🔒 Symmetric NAT detected - hole punching may be difficult.");
                    println!("   P2P connections may require a relay server (TURN).");
                }
                NatType::Unknown => {
                    println!("❓ Could not determine NAT type.");
                    println!("   Try using --to <address> for direct connections.");
                }
            }

            Ok(())
        }
        Err(e) => {
            println!();
            println!("❌ Failed to discover public endpoint: {}", e);
            println!();
            println!("Possible reasons:");
            println!("  • No internet connection");
            println!("  • Firewall blocking UDP traffic");
            println!("  • STUN server unavailable");
            println!();
            println!("Try:");
            println!("  • Check your internet connection");
            println!("  • Use a different STUN server with --stun-server <server:port>");
            println!("  • Check firewall settings");

            Err(e.into())
        }
    }
}
