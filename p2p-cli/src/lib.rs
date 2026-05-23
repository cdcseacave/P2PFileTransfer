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
mod rendezvous;
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

/// Entry point for CLI - checks if GUI mode should be used before entering async context
pub fn run_cli_sync() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    init_logging(&cli.verbosity);

    // Check if we should run GUI mode (no command or explicit gui command)
    // GUI must be run outside of async context to avoid nested runtime issues
    #[cfg(feature = "gui")]
    {
        match &cli.command {
            None | Some(cli::Commands::Gui) => {
                // Launch GUI in blocking mode (it has its own Tokio runtime via Iced)
                return p2p_gui::run_gui();
            }
            _ => {
                // Continue to async CLI commands
            }
        }
    }

    #[cfg(not(feature = "gui"))]
    {
        if cli.command.is_none() {
            eprintln!("GUI not available. This binary was built without GUI support.");
            eprintln!("To use GUI, rebuild with: cargo build --release --features full");
            eprintln!("\nAvailable CLI commands:");
            eprintln!("  p2p-transfer send <PATH> --peer <IP:PORT>");
            eprintln!("  p2p-transfer receive --output <DIR>");
            eprintln!("  p2p-transfer discover");
            eprintln!("  p2p-transfer --help");
            std::process::exit(1);
        }
        // Continue to async CLI commands
    }

    // Run async CLI commands in Tokio runtime
    tokio::runtime::Runtime::new()?.block_on(run_cli_async(cli))
}

async fn run_cli_async(cli: Cli) -> Result<()> {
    match cli.command {
        // GUI cases already handled in run_cli_sync
        #[cfg(feature = "gui")]
        None | Some(cli::Commands::Gui) => {
            unreachable!("GUI mode should be handled in run_cli_sync")
        }
        #[cfg(not(feature = "gui"))]
        None => {
            unreachable!("No command case should be handled in run_cli_sync")
        }
        #[cfg(not(feature = "gui"))]
        Some(cli::Commands::Gui) => {
            unreachable!("Gui command should be handled in run_cli_sync")
        }
        Some(cli::Commands::Send {
            path,
            session,
            transfer,
        }) => {
            send::handle_send(path, session, transfer).await?;
        }
        Some(cli::Commands::Receive {
            output,
            auto_accept,
            session,
        }) => {
            receive::handle_receive(output, auto_accept, session).await?;
        }
        Some(cli::Commands::Discover { timeout, port }) => {
            discover::handle_discover(timeout, port).await?;
        }
        Some(cli::Commands::NatTest { stun_server, rendezvous }) => {
            nat_test::handle_nat_test(stun_server, rendezvous).await?;
        }
        Some(cli::Commands::Resume {
            transfer_id,
            to,
            peer_fingerprint,
            path,
        }) => {
            resume::handle_resume(transfer_id, to, peer_fingerprint, path).await?;
        }
        Some(cli::Commands::History {
            limit,
            direction,
            completed,
            failed,
        }) => {
            history::handle_history(limit, direction, completed, failed).await?;
        }
    }

    Ok(())
}
