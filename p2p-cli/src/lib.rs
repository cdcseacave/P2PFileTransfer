//! CLI interface for P2P file transfer
//!
//! This module provides a command-line interface organized into separate submodules:
//! - `cli`: Command definitions and parsing
//! - `send`: Send operations for files and folders
//! - `receive`: Receive operations
//! - `discover`: Peer discovery functionality
//! - `resume`: Resume interrupted transfers

mod cli;
mod discover;
mod history;
mod nat_test;
mod receive;
mod resume;
mod send;

use anyhow::Result;
use clap::Parser;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub use cli::Cli;

/// Initialize logging based on verbosity level
fn init_logging(verbosity: &str) {
    // Parse verbosity level from string
    let level = match verbosity.to_lowercase().as_str() {
        "off" => LevelFilter::OFF,
        "error" => LevelFilter::ERROR,
        "warn" => LevelFilter::WARN,
        "info" => LevelFilter::INFO,
        "debug" => LevelFilter::DEBUG,
        "trace" => LevelFilter::TRACE,
        _ => {
            eprintln!("Invalid verbosity level '{}', using 'info'", verbosity);
            LevelFilter::INFO
        }
    };

    // Check if RUST_LOG environment variable is set
    let env_filter = if std::env::var("RUST_LOG").is_ok() {
        // If RUST_LOG is set, use it (allows fine-grained control)
        EnvFilter::from_default_env()
    } else {
        // Otherwise use the command-line level
        EnvFilter::default()
            .add_directive(format!("p2p_core={}", level).parse().unwrap())
            .add_directive(format!("p2p_cli={}", level).parse().unwrap())
    };

    // Initialize tracing subscriber with nice formatting
    let _ = tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_target(false) // Don't show module names (cleaner output)
                .with_level(true) // Show log level [INFO], [DEBUG], etc.
                .with_ansi(true) // Use colors
                .compact(), // Compact format
        )
        .try_init();
}

pub async fn run_cli() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    init_logging(&cli.verbosity);

    match cli.command {
        cli::Commands::Send {
            path,
            session,
            transfer,
        } => {
            send::handle_send(path, session, transfer).await?;
        }
        cli::Commands::Receive {
            output,
            auto_accept,
            session,
        } => {
            receive::handle_receive(output, auto_accept, session).await?;
        }
        cli::Commands::Discover { timeout, port } => {
            discover::handle_discover(timeout, port).await?;
        }
        cli::Commands::NatTest { stun_server } => {
            nat_test::handle_nat_test(stun_server).await?;
        }
        cli::Commands::Resume {
            transfer_id,
            to,
            path,
        } => {
            resume::handle_resume(transfer_id, to, path).await?;
        }
        cli::Commands::History {
            limit,
            direction,
            completed,
            failed,
        } => {
            history::handle_history(limit, direction, completed, failed).await?;
        }
    }

    Ok(())
}
