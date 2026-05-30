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
pub enum Message {
    // Tab switching
    TabSelected(Tab),

    // Connection tab
    ModeSelected(ConnectionMode),
    PeerAddressChanged(String),
    PeerFingerprintChanged(String),
    PortChanged(String),
    DiscoveryToggled(bool),
    RendezvousAddressChanged(String),
    CodeChanged(String),
    GenerateCode,
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

    // Settings
    CompressionToggled(bool),
    CompressionLevelChanged(i32),
    ChunkSizeChanged(u32),
    BandwidthLimitChanged(String),
    MaxRetriesChanged(u32),

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
