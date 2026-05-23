//! Message types for GUI events
//!
//! This module defines all message types that can be sent within the application.

use crate::state::{ConnectionMode, Tab};
use p2p_core::session::P2PSession;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Application messages
#[derive(Clone)]
#[allow(dead_code)] // Some variants not yet implemented in operations
pub enum Message {
    // Tab switching
    TabSelected(Tab),

    // Connection tab
    ModeSelected(ConnectionMode),
    PeerAddressChanged(String),
    PortChanged(String),
    DiscoveryToggled(bool),
    StartConnection,
    StopConnection,
    ConnectionEstablished(String), // Success message (for Listen mode)
    ConnectionEstablishedWithSession(Arc<Mutex<P2PSession>>, String), // Success with session (for Connect mode)
    ConnectionFailed(String),                                         // Error message

    // Send tab
    PathInputChanged(String),
    BrowseFile,
    BrowseFolder,
    PathSelected(Option<PathBuf>),
    StartSend,
    SendComplete(String, u64), // message, bytes_transferred
    SendFailed(String),

    // Receive tab
    OutputDirChanged(String),
    BrowseOutputDir,
    OpenOutputDir,
    OutputDirSelected(Option<PathBuf>),
    AutoAcceptToggled(bool),
    StartReceive,
    ReceiveComplete(String, u64), // message, bytes_transferred
    ReceiveFailed(String),

    // Settings
    CompressionToggled(bool),
    CompressionLevelChanged(i32),
    AdaptiveCompressionToggled(bool),
    ChunkSizeChanged(u32),
    BandwidthLimitChanged(String),
    MaxRetriesChanged(u32),

    // Progress
    ProgressUpdate {
        transferred: u64,
        total: u64,
        speed: f64,
        eta: u64,
    },

    // Transfer lifecycle events
    TransferStarted(String), // Transfer initiated (e.g., "Receiving from peer...")
    TransferInProgress(String), // Transfer ongoing status
    TransferCompleted(String), // Transfer finished successfully
    TransferError(String),   // Transfer failed

    // Listener status
    ListenerWaiting,        // Waiting for incoming connection
    ListenerActive(String), // Active connection (peer ID)

    // History
    RefreshHistory,

    // Console
    ConsoleAction(iced::widget::text_editor::Action),
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::ConnectionEstablishedWithSession(_, msg) => f
                .debug_tuple("ConnectionEstablishedWithSession")
                .field(&"<Session>")
                .field(msg)
                .finish(),
            _ => {
                // For other variants, use default Debug if they had it
                write!(f, "{:?}", self as *const _)
            }
        }
    }
}
