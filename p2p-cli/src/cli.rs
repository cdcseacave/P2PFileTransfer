//! Command-line interface definitions

use clap::{Parser, Subcommand};
use std::path::PathBuf;

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

        /// Chunk size in KB
        #[arg(long, default_value = "64")]
        chunk_size: u32,

        /// Window size (number of chunks in-flight). Use 1 for sequential mode, 2+ for windowed mode
        #[arg(long, default_value = "16")]
        window_size: usize,

        /// Listen port
        #[arg(short, long, default_value = "7778")]
        port: u16,
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
}
