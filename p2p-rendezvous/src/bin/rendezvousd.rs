//! `rendezvousd` — the self-hostable rendezvous server for `p2p-transfer`.
//!
//! No public-default URL is bundled into the `p2p-transfer` binary; the
//! operator runs `rendezvousd` somewhere reachable (a free-tier VPS, a
//! `docker-compose` stack, a home server) and shares the resulting
//! `host:port` with the peers that should pair through it.

use std::net::SocketAddr;

use clap::Parser;
use tracing_subscriber::{prelude::*, EnvFilter};

use p2p_rendezvous::{Server, DEFAULT_PORT};

#[derive(Parser, Debug)]
#[command(name = "rendezvousd")]
#[command(about = "Pairing-by-code rendezvous server for p2p-transfer", long_about = None)]
#[command(version)]
struct Cli {
    /// Address to listen on (TCP).
    #[arg(long, default_value_t = default_bind())]
    bind: SocketAddr,

    /// Code lifetime in seconds. After this point an unmatched code
    /// is dropped; the waiting peer receives `Expired`.
    #[arg(long, default_value_t = 300)]
    code_ttl_secs: u64,

    /// Logging verbosity: off, error, warn, info, debug, trace.
    #[arg(long, default_value = "info")]
    verbosity: String,
}

fn default_bind() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], DEFAULT_PORT))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    init_logging(&cli.verbosity);

    let server = Server::bind_with_ttl(cli.bind, std::time::Duration::from_secs(cli.code_ttl_secs)).await?;
    server.run().await?;
    Ok(())
}

fn init_logging(verbosity: &str) {
    let filter = if std::env::var("RUST_LOG").is_ok() {
        EnvFilter::from_default_env()
    } else {
        EnvFilter::new(format!("p2p_rendezvous={verbosity},rendezvousd={verbosity}"))
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(false).compact())
        .init();
}
