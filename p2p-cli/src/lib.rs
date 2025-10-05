//! CLI interface for P2P file transfer
//!
//! This module provides a command-line interface organized into separate submodules:
//! - `cli`: Command definitions and parsing
//! - `send`: Send operations for files and folders
//! - `receive`: Receive operations
//! - `discover`: Peer discovery functionality
//! - `resume`: Resume interrupted transfers

mod cli;
mod send;
mod receive;
mod discover;
mod resume;
mod nat_test;

use anyhow::Result;
use clap::Parser;

pub use cli::Cli;

pub async fn run_cli() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging (only if not already initialized)
    let _ = if cli.verbose {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).try_init()
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).try_init()
    };

    match cli.command {
        cli::Commands::Send {
            path,
            to,
            discover,
            compress,
            compress_level,
            chunk_size,
            window_size,
            max_speed,
            transfer_port,
        } => {
            send::handle_send(path, to, discover, compress, compress_level, chunk_size, window_size, max_speed, transfer_port).await?;
        }
        cli::Commands::Receive {
            output,
            port,
            auto_accept,
        } => {
            receive::handle_receive(output, port, auto_accept).await?;
        }
        cli::Commands::Discover { timeout } => {
            discover::handle_discover(timeout).await?;
        }
        cli::Commands::NatTest { stun_server } => {
            nat_test::handle_nat_test(stun_server).await?;
        }
        cli::Commands::Resume { transfer_id, to, path } => {
            resume::handle_resume(transfer_id, to, path).await?;
        }
    }

    Ok(())
}
