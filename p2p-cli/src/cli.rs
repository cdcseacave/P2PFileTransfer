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

    /// Peer address (IP:PORT) - required when role is 'client' and not using discovery
    #[arg(long)]
    pub peer: Option<String>,

    /// Hex-encoded SHA-256 fingerprint of the peer's TLS cert (64 hex chars).
    /// Required when --peer is used; populated automatically from LAN beacons
    /// when --discover is used.
    #[arg(long)]
    pub peer_fingerprint: Option<String>,

    /// Port to use - for 'client' role, this is the destination port; for 'server' role, this is the listen port
    #[arg(short = 'p', long, default_value = "14567")]
    pub port: u16,

    /// Use peer discovery to find the peer address (only for 'client' role)
    #[arg(short = 'd', long)]
    pub discover: bool,

    /// Rendezvous server (host:port) for cross-NAT pairing. When set,
    /// `--peer` and `--discover` are ignored and pairing happens via
    /// `--code` instead.
    #[arg(long)]
    pub rendezvous: Option<String>,

    /// Shared pairing code (4–32 ASCII alphanumeric). Required when
    /// `--rendezvous` is set. Both peers must use the same value: agree
    /// out-of-band, pick any conforming string, or generate one with the
    /// GUI's "Generate" button.
    #[arg(long)]
    pub code: Option<String>,

    /// Force relay mode even when STUN says the local NAT is Cone.
    /// Useful for testing the relay path; normal pairing should leave
    /// this off and let symmetric-NAT detection decide.
    #[arg(long)]
    pub force_relay: bool,
}

impl SessionParams {
    /// Decode `--peer-fingerprint` into a 32-byte array, if provided.
    pub fn parsed_fingerprint(&self) -> anyhow::Result<Option<[u8; 32]>> {
        let Some(hex_str) = self.peer_fingerprint.as_deref() else {
            return Ok(None);
        };
        if hex_str.len() != 64 {
            anyhow::bail!(
                "--peer-fingerprint must be 64 hex chars, got {} chars",
                hex_str.len()
            );
        }
        let bytes = hex::decode(hex_str)
            .map_err(|e| anyhow::anyhow!("--peer-fingerprint hex decode: {e}"))?;
        let mut out = [0u8; 32];
        out.copy_from_slice(&bytes);
        Ok(Some(out))
    }
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
    #[arg(long, default_value = "1024")]
    pub chunk_size: u32,

    /// Maximum transfer speed (e.g., "10M", "1G", "512K", "unlimited"). Default: unlimited
    #[arg(long, value_parser = parse_bandwidth_arg, default_value = "0")]
    pub max_speed: u64,

    /// Max reconnect attempts after a connection drop (0 = retry forever)
    #[arg(long, default_value = "5")]
    pub max_reconnect_attempts: u32,
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

    /// Directory holding identity.{key,cert} (default: <config_dir>/p2p-transfer)
    #[arg(long, global = true)]
    pub identity_dir: Option<PathBuf>,
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

        /// Directory to write the resume state file into. Defaults to the
        /// current working directory. Pass an absolute path here so
        /// `p2p-transfer resume <id> --state-dir <same>` works regardless
        /// of where the user runs the resume command from.
        #[arg(long)]
        state_dir: Option<PathBuf>,

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

    /// Test NAT traversal — STUN-based by default; with `--rendezvous`,
    /// runs a real self-loop punch test through a live rendezvous server.
    NatTest {
        /// STUN server to use (defaults to two of Google's public servers
        /// so symmetric-vs-cone classification is possible)
        #[arg(long)]
        stun_server: Option<String>,

        /// Rendezvous server (host[:port]) to self-loop punch against.
        /// When present, the tool spawns two local peers that pair
        /// through the rendezvous and races a QUIC handshake between
        /// them — reports `direct`, `relay`, or `failed`.
        #[arg(long)]
        rendezvous: Option<String>,
    },

    /// Resume a previous transfer
    Resume {
        /// Transfer ID to resume (or state file path)
        transfer_id: String,

        /// Peer address (IP:PORT) to reconnect to
        #[arg(long)]
        to: String,

        /// SHA-256 fingerprint (64 hex chars) of the peer's TLS cert
        #[arg(long)]
        peer_fingerprint: String,

        /// Original file or folder path to resume from
        #[arg(long)]
        path: PathBuf,

        /// Directory the resume state file lives in. Must match whatever
        /// `--state-dir` the original `send` used; defaults to the current
        /// working directory.
        #[arg(long)]
        state_dir: Option<PathBuf>,

        /// Max reconnect attempts after a connection drop (0 = retry forever)
        #[arg(long, default_value = "5")]
        max_reconnect_attempts: u32,
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
