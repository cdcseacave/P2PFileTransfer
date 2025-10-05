//! Command-line interface definitions

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Parse bandwidth string into bytes per second
fn parse_bandwidth_arg(s: &str) -> Result<u64, String> {
    p2p_core::bandwidth::parse_bandwidth(s)
}

#[derive(Parser)]
#[command(name = "p2p-transfer")]
#[command(about = "P2P file transfer with compression", long_about = None)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Send files to a peer
    Send {
        /// File or folder to send
        path: PathBuf,

        /// Peer address (IP:PORT)
        #[arg(short, long)]
        to: Option<String>,

        /// Use auto-discovery
        #[arg(short, long)]
        discover: bool,

        /// Enable compression
        #[arg(long, default_value = "true")]
        compress: bool,

        /// Compression level (-7 to 22)
        #[arg(long, default_value = "3")]
        compress_level: i32,

        /// Use adaptive compression (auto-disable if data is incompressible)
        #[arg(long, default_value = "true")]
        adaptive: bool,

        /// Chunk size in KB
        #[arg(long, default_value = "64")]
        chunk_size: u32,

        /// Window size (number of chunks in-flight). Use 1 for sequential mode, 2+ for windowed mode
        #[arg(long, default_value = "16")]
        window_size: usize,

        /// Maximum transfer speed (e.g., "10M", "1G", "512K", "unlimited"). Default: unlimited
        #[arg(long, value_parser = parse_bandwidth_arg, default_value = "0")]
        max_speed: u64,

        /// Transfer port (TCP port for file transfer connections)
        #[arg(short, long, default_value = "7778")]
        transfer_port: u16,
    },

    /// Receive files from a peer
    Receive {
        /// Output directory
        #[arg(short, long, default_value = "./received")]
        output: PathBuf,

        /// Listen port
        #[arg(short, long, default_value = "7778")]
        port: u16,

        /// Auto-accept transfers without prompting
        #[arg(short = 'a', long)]
        auto_accept: bool,
    },

    /// Discover peers on the network
    Discover {
        /// Discovery timeout in seconds
        #[arg(short, long, default_value = "10")]
        timeout: u64,
    },

    /// Test NAT traversal - discover public IP and port
    NatTest {
        /// STUN server to use (default: Google's public STUN)
        #[arg(long)]
        stun_server: Option<String>,
    },

    /// Resume a previous transfer
    Resume {
        /// Transfer ID to resume (or state file path)
        transfer_id: String,
        
        /// Peer address (IP:PORT) to reconnect to
        #[arg(long)]
        to: String,
        
        /// Original folder path to resume from
        #[arg(long)]
        path: PathBuf,
    },

    /// View transfer history
    History {
        /// Show only recent N transfers
        #[arg(short = 'n', long, default_value = "10")]
        limit: usize,

        /// Filter by direction (send/receive)
        #[arg(short, long)]
        direction: Option<String>,

        /// Show only completed transfers
        #[arg(long)]
        completed: bool,

        /// Show only failed transfers
        #[arg(long)]
        failed: bool,
    },
}
