//! Unified progress tracking for file transfers
//!
//! This module provides a unified progress tracking system for both single-file
//! and folder transfers. It tracks bytes transferred and manages the progress bar display.

use indicatif::{ProgressBar, ProgressStyle};

/// Unified progress state for tracking transfer progress
pub struct ProgressState {
    /// Total bytes to transfer
    total_bytes: u64,
    /// Bytes transferred so far
    transferred_bytes: u64,
    /// Progress bar from indicatif
    progress_bar: ProgressBar,
}

impl ProgressState {
    /// Create a new progress state with a progress bar
    pub fn new(total_bytes: u64) -> Self {
        let progress_bar = ProgressBar::new(total_bytes);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes} ({bytes_per_sec}, ETA: {eta})")
                .unwrap()
                .progress_chars("=>-"),
        );

        // If total_bytes is 0, hide the progress bar until we know the actual size
        if total_bytes == 0 {
            progress_bar.set_draw_target(indicatif::ProgressDrawTarget::hidden());
        } else {
            // Enable steady tick for smooth updates (every 250ms)
            progress_bar.enable_steady_tick(std::time::Duration::from_millis(250));
        }

        Self {
            total_bytes,
            transferred_bytes: 0,
            progress_bar,
        }
    }

    /// Update progress by adding bytes transferred
    pub fn add_bytes(&mut self, bytes: u64) {
        self.transferred_bytes += bytes;
        self.progress_bar.set_position(self.transferred_bytes);
        // Force a draw/tick to ensure the bar updates immediately
        self.progress_bar.tick();
    }

    /// Set the total bytes (useful when total is initially unknown)
    pub fn set_total_bytes(&mut self, total_bytes: u64) {
        if self.total_bytes == total_bytes {
            return;
        }

        // If we're transitioning from 0 to a real value, show the progress bar now
        if self.total_bytes == 0 && total_bytes > 0 {
            // Set draw target to default (stderr) to make it visible
            self.progress_bar
                .set_draw_target(indicatif::ProgressDrawTarget::stderr());
            // Enable steady tick for smooth updates (every 250ms)
            self.progress_bar
                .enable_steady_tick(std::time::Duration::from_millis(250));
            // Reset the elapsed clock so the bytes/sec rate doesn't include
            // whatever happened before the real total was known (most
            // commonly the interactive y/N accept prompt).
            self.progress_bar.reset_elapsed();
            self.progress_bar.reset_eta();
        }

        self.total_bytes = total_bytes;
        self.progress_bar.set_length(total_bytes);
        // Force a tick to show the updated total
        self.progress_bar.tick();
    }

    /// Finish the progress bar
    pub fn finish(&self) {
        self.progress_bar.finish_with_message("Transfer complete!");
    }

    /// Get total bytes
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Get transferred bytes
    pub fn transferred_bytes(&self) -> u64 {
        self.transferred_bytes
    }

    /// Get progress percentage
    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes > 0 {
            (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Check if transfer is complete
    pub fn is_complete(&self) -> bool {
        self.transferred_bytes >= self.total_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_state() {
        let mut state = ProgressState::new(1000);

        assert_eq!(state.total_bytes(), 1000);
        assert_eq!(state.transferred_bytes(), 0);
        assert_eq!(state.progress_percent(), 0.0);
        assert!(!state.is_complete());

        state.add_bytes(250);
        assert_eq!(state.transferred_bytes(), 250);
        assert_eq!(state.progress_percent(), 25.0);
        assert!(!state.is_complete());

        state.add_bytes(750);
        assert_eq!(state.transferred_bytes(), 1000);
        assert_eq!(state.progress_percent(), 100.0);
        assert!(state.is_complete());
    }

    #[test]
    fn test_progress_updates() {
        let mut state = ProgressState::new(500);

        state.add_bytes(100);
        assert_eq!(state.transferred_bytes(), 100);
        assert_eq!(state.progress_percent(), 20.0);

        state.add_bytes(200);
        assert_eq!(state.transferred_bytes(), 300);
        assert_eq!(state.progress_percent(), 60.0);

        state.add_bytes(200);
        assert_eq!(state.transferred_bytes(), 500);
        assert_eq!(state.progress_percent(), 100.0);
        assert!(state.is_complete());
    }
}
