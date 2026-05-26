//! Application state types
//!
//! This module contains all state structures for the GUI tabs and overall application.

use iced::widget::text_editor;
use p2p_core::{history::TransferHistory, session::P2PSession};
use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc, Mutex as StdMutex};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Console message icon type
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)] // Variants reserved for future use
pub enum ConsoleIcon {
    Info,
    Success,
    Warning,
    Error,
}

/// Application tabs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Connection,
    Send,
    Receive,
    Settings,
    History,
}

impl Tab {
    pub fn all() -> Vec<Tab> {
        vec![
            Tab::Connection,
            Tab::Send,
            Tab::Receive,
            Tab::Settings,
            Tab::History,
        ]
    }

    /// Get the emoji icon for the tab
    pub fn icon(&self) -> &str {
        match self {
            Tab::Connection => "🔌",
            Tab::Send => "📤",
            Tab::Receive => "📥",
            Tab::Settings => "⚙️",
            Tab::History => "📊",
        }
    }

    /// Get the text label for the tab (without emoji)
    pub fn text(&self) -> &str {
        match self {
            Tab::Connection => "Connection",
            Tab::Send => "Send",
            Tab::Receive => "Receive",
            Tab::Settings => "Settings",
            Tab::History => "History",
        }
    }
}

/// Connection state
#[derive(Default)]
pub struct ConnectionState {
    /// Connection mode
    pub mode: ConnectionMode,
    /// Peer address input (Connect mode)
    pub peer_address: String,
    /// Hex-encoded SHA-256 cert fingerprint of the peer (Connect mode).
    /// 64 hex chars; pulled from beacons in Discovery mode and from the
    /// rendezvous in Rendezvous mode.
    pub peer_fingerprint: String,
    /// Port input
    pub port: String,
    /// Device ID
    pub device_id: Option<Uuid>,
    /// Use peer discovery (Connect mode only)
    pub use_discovery: bool,
    /// Rendezvous server (host[:port]) for cross-NAT pairing
    pub rendezvous_address: String,
    /// Shared pairing code for the rendezvous
    pub code: String,
    /// Connection status message
    pub status_message: String,
    /// Is currently connecting/listening
    pub is_active: bool,
}

/// Connection mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionMode {
    #[default]
    Listen,
    Connect,
    /// Pair with another peer through a rendezvous server using a short
    /// shared code (works across NATs).
    Rendezvous,
}

impl ConnectionMode {
    pub fn all() -> Vec<ConnectionMode> {
        vec![
            ConnectionMode::Listen,
            ConnectionMode::Connect,
            ConnectionMode::Rendezvous,
        ]
    }
}

impl std::fmt::Display for ConnectionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionMode::Listen => write!(f, "Listen for connections"),
            ConnectionMode::Connect => write!(f, "Connect to peer (direct)"),
            ConnectionMode::Rendezvous => write!(f, "Pair with code (cross-NAT)"),
        }
    }
}

/// Send tab state
#[derive(Default)]
pub struct SendState {
    /// Selected path to send
    pub selected_path: Option<PathBuf>,
    /// Path input field
    pub path_input: String,
}

/// Receive tab state
#[derive(Default)]
pub struct ReceiveState {
    /// Output directory
    pub output_dir: PathBuf,
    /// Output directory input field
    pub output_input: String,
    /// Auto-accept transfers
    pub auto_accept: bool,
}

/// Application settings
pub struct AppSettings {
    /// Enable compression
    pub compression_enabled: bool,
    /// Compression level
    pub compression_level: i32,
    /// Adaptive compression
    pub adaptive_compression: bool,
    /// Chunk size in KB
    pub chunk_size_kb: u32,
    /// Bandwidth limit (0 = unlimited)
    pub bandwidth_limit: u64,
    /// Max retries
    pub max_retries: u32,
    /// Bandwidth limit input
    pub bandwidth_input: String,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            compression_enabled: true,
            compression_level: 3,
            adaptive_compression: true,
            chunk_size_kb: p2p_core::DEFAULT_CHUNK_SIZE / 1024,
            bandwidth_limit: 0,
            max_retries: 5,
            bandwidth_input: String::from("unlimited"),
        }
    }
}

impl AppSettings {
    pub fn to_config_message(&self) -> p2p_core::protocol::ConfigMessage {
        p2p_core::protocol::ConfigMessage {
            compression_enabled: self.compression_enabled,
            compression_level: self.compression_level,
            adaptive_compression: self.adaptive_compression,
            chunk_size: self.chunk_size_kb * 1024,
            bandwidth_limit: self.bandwidth_limit,
        }
    }
}

/// Transfer progress information
pub struct TransferProgress {
    /// File/folder name (used when logging completed transfers to history)
    pub name: String,
    /// Total bytes
    pub total_bytes: u64,
    /// Transferred bytes
    pub transferred_bytes: u64,
    /// Transfer speed (bytes per second)
    pub speed_bps: f64,
    /// Is sending (true) or receiving (false)
    pub is_sending: bool,
}

/// Main application state
pub struct AppState {
    /// Current active tab
    pub current_tab: Tab,
    /// Connection state
    pub connection_state: ConnectionState,
    /// Send tab state
    pub send_state: SendState,
    /// Receive tab state
    pub receive_state: ReceiveState,
    /// Settings state
    pub settings: AppSettings,
    /// Transfer history (use std::Mutex since it's only accessed in sync context)
    pub history: Arc<StdMutex<TransferHistory>>,
    /// Active session (use tokio::Mutex for async operations)
    pub session: Option<Arc<Mutex<P2PSession>>>,
    /// Current transfer progress
    pub transfer_progress: Option<TransferProgress>,
    /// Text editor content for selectable console
    pub console_content: text_editor::Content,
    /// Cancellation flag for stopping listener
    pub cancel_listener: Arc<AtomicBool>,
    /// Current transfer record being tracked
    pub current_transfer: Option<p2p_core::history::TransferRecord>,
}

impl Drop for AppState {
    fn drop(&mut self) {
        // Ensure session is dropped cleanly
        if let Some(session) = self.session.take() {
            // Drop the Arc, which will clean up the session
            drop(session);
        }
    }
}

impl AppState {
    pub fn new() -> Self {
        let history = Arc::new(StdMutex::new(TransferHistory::new()));

        let mut receive_state = ReceiveState::default();
        receive_state.output_dir = PathBuf::from("./received");
        receive_state.output_input = receive_state.output_dir.display().to_string();

        // Build initial console content
        let initial_content = format!(
            "{} ℹ️  Application started\n",
            chrono::Local::now().format("%H:%M:%S")
        );
        let console_content = text_editor::Content::with_text(&initial_content);

        Self {
            current_tab: Tab::Connection,
            connection_state: ConnectionState {
                port: String::from("14567"),
                status_message: String::from("Idle"),
                device_id: Some(Uuid::new_v4()),
                ..Default::default()
            },
            send_state: SendState::default(),
            receive_state,
            settings: AppSettings::default(),
            history,
            session: None,
            transfer_progress: None,
            console_content,
            cancel_listener: Arc::new(AtomicBool::new(false)),
            current_transfer: None,
        }
    }

    /// Add a message to the console
    pub fn add_console_message(&mut self, text: String, icon_type: ConsoleIcon) {
        let timestamp = chrono::Local::now().format("%H:%M:%S").to_string();
        let icon = match icon_type {
            ConsoleIcon::Info => "ℹ️ ",
            ConsoleIcon::Success => "✅",
            ConsoleIcon::Warning => "⚠️ ",
            ConsoleIcon::Error => "❌",
        };

        // Append to text editor content and move cursor to end for auto-scroll
        let line = format!("{} {} {}\n", timestamp, icon, text);
        let new_text = format!("{}{}", self.console_content.text(), line);
        self.console_content = text_editor::Content::with_text(&new_text);

        // Move cursor to end to ensure it scrolls to bottom
        self.console_content
            .perform(iced::widget::text_editor::Action::Move(
                iced::widget::text_editor::Motion::DocumentEnd,
            ));
    }
}
