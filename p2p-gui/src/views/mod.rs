//! View modules for each tab
//!
//! This module re-exports all tab view implementations.

pub mod connection;
pub mod console;
pub mod history;
pub mod receive;
pub mod send;
pub mod settings;

pub use connection::view_connection_tab;
pub use console::view_console;
pub use history::view_history_tab;
pub use receive::view_receive_tab;
pub use send::view_send_tab;
pub use settings::view_settings_tab;
