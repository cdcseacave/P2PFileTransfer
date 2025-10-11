//! Command-line interface definitions

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

/// Parse bandwidth string into bytes per second
fn parse_bandwidth_arg(s: &str) -> Result<u64, String> {
    p2p_core::bandwidth::parse_bandwidth(s)
}

/// Common session parameters for connection establishment
///
/// These parameters control how the P2P session is established and what role
/// this peer takes (client/server). After the session is established, both
/// peers are equal and can perform any operation.
#[derive(Args, Clone)]
pub struct SessionParams {
    /// Session role: 'client' (connect to peer) or 'server' (listen for peer)
    /// If not specified, defaults based on command: 'client' for send, 'server' for receive
    #[arg(long, value_parser = ["client", "server"])]
    pub role: Option<String>,

    /// Peer address (IP:PORT) - required when role is 'client'
    #[arg(long)]
    pub peer: Option<String>,

    /// Port to use - for 'client' role, this is the destination port; for 'server' role, this is the listen port
    #[arg(short = 'p', long, default_value = "14567")]
    pub port: u16,

    /// Use peer discovery to find the peer address (only for 'client' role)
    #[arg(short = 'd', long)]
    pub discover: bool,
}

impl SessionParams {
    /// Get the role, using the provided default if not specified
    pub fn get_role(&self, default: &str) -> String {
        self.role.clone().unwrap_or_else(|| default.to_string())
    }

    /// Check if this is a client role (with default fallback)
    pub fn is_client(&self, default: &str) -> bool {
        self.get_role(default) == "client"
    }

    /// Check if this is a server role (with default fallback)
    pub fn is_server(&self, default: &str) -> bool {
        self.get_role(default) == "server"
    }
}

/// Common transfer configuration parameters
///
/// These parameters control the transfer behavior (compression, windowing, etc.)
/// and apply regardless of whether this peer is acting as sender or receiver.
#[derive(Args, Clone)]
pub struct TransferParams {
    /// Enable compression (default: enabled, use --compress=false to disable)
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub compress: bool,

    /// Compression level (-7 to 22)
    #[arg(long, default_value = "3")]
    pub compress_level: i32,

    /// Auto-disable compression if data is incompressible (default: enabled, use --adaptive=false to disable)
    #[arg(long, default_value = "true", action = clap::ArgAction::Set)]
    pub adaptive: bool,

    /// Chunk size in KB
    #[arg(long, default_value = "64")]
    pub chunk_size: u32,

    /// Window size (number of chunks in-flight). Use 1 for sequential mode, 2+ for windowed mode
    #[arg(long, default_value = "16")]
    pub window_size: usize,

    /// Maximum transfer speed (e.g., "10M", "1G", "512K", "unlimited"). Default: unlimited
    #[arg(long, value_parser = parse_bandwidth_arg, default_value = "0")]
    pub max_speed: u64,

    /// Maximum reconnection attempts on network failures (0 = unlimited, 1 = no retry)
    #[arg(long, default_value = "5")]
    pub max_retries: u32,
}

#[derive(Parser)]
#[command(name = "p2p-transfer")]
#[command(about = "P2P file transfer with compression (GUI mode by default)", long_about = None)]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    /// Set logging level: off, error, warn, info, debug, trace
    #[arg(short = 'v', long = "verbosity", default_value = "info", global = true)]
    pub verbosity: String,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Send files to a peer
    ///
    /// Can operate in two modes:
    /// - Client mode (default): Connect to a peer and send files
    /// - Server mode: Listen for a peer to connect, then send files
    Send {
        /// File or folder to send
        path: PathBuf,

        #[command(flatten)]
        session: SessionParams,

        #[command(flatten)]
        transfer: TransferParams,
    },

    /// Receive files from a peer
    ///
    /// Can operate in two modes:
    /// - Server mode (default): Listen for a peer to connect and receive files
    /// - Client mode: Connect to a peer and receive files
    Receive {
        /// Output directory
        #[arg(short, long, default_value = "./received")]
        output: PathBuf,

        /// Auto-accept transfers without prompting
        #[arg(short = 'a', long)]
        auto_accept: bool,

        #[command(flatten)]
        session: SessionParams,
    },

    /// Discover peers on the network
    Discover {
        /// Discovery timeout in seconds
        #[arg(short, long, default_value = "10")]
        timeout: u64,

        /// Port to use for discovery
        #[arg(short = 'p', long, default_value = "14567")]
        port: u16,
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

    /// Launch graphical user interface
    Gui,
}
