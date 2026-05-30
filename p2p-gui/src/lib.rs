//! GUI interface for P2P file transfer
//!
//! This module provides a graphical user interface for the P2P file transfer application
//! using the Iced framework. The GUI supports all features available in the CLI, including:
//! - Starting a listener or connecting to peers
//! - Sending files and folders with progress tracking
//! - Receiving transfers with auto-accept
//! - Configuring all transfer settings (compression, window size, bandwidth, etc.)
//! - Viewing transfer history
//!
//! ## Architecture
//!
//! The GUI is organized into several modules:
//! - `app`: Main application and Iced integration
//! - `state`: Application state types and structures
//! - `message`: Message types for event handling
//! - `operations`: Message handlers and async operations
//! - `views`: View implementations for each tab
//! - `utils`: Utility functions for formatting

mod app;
mod message;
mod operations;
mod state;
mod utils;
mod views;

use anyhow::Result;
use tracing::info;

/// Run the GUI application
pub fn run_gui() -> Result<()> {
    info!("🎨 Starting GUI mode");
    app::run()
}
