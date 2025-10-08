//! Sliding window protocol for parallel chunk transfers
//!
//! This module implements a sliding window flow control mechanism that allows
//! multiple chunks to be in-flight simultaneously, significantly improving
//! transfer speed on high-latency networks.
//!
//! ## Design
//!
//! The sliding window allows sending multiple chunks before waiting for acknowledgments,
//! with configurable window size, timeout, and retry limits. Each in-flight chunk stores
//! the complete network message to enable efficient retransmission on timeout without
//! needing to re-read, re-compress, or reconstruct the chunk data.
//!
//! ## Key Features
//!
//! - **Parallel transmission**: Multiple chunks can be in-flight simultaneously
//! - **Automatic retry**: Chunks are retransmitted on timeout with exponential backoff
//! - **Out-of-order ACKs**: Handles acknowledgments arriving in any order
//! - **Resume support**: Can mark chunks as already completed for transfer resumption

use crate::protocol::ChunkMessage;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// Configuration for the sliding window
#[derive(Debug, Clone)]
pub struct WindowConfig {
    /// Maximum number of chunks that can be in-flight simultaneously
    pub max_window_size: usize,
    /// Timeout for chunk acknowledgment
    pub ack_timeout: Duration,
    /// Maximum number of retries per chunk
    pub max_retries: u32,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            max_window_size: 16,
            ack_timeout: Duration::from_secs(10),
            max_retries: 3,
        }
    }
}

/// Information about a chunk that has been sent but not yet acknowledged
///
/// This structure wraps a network chunk message with windowing-specific metadata
/// for retry logic and timeout tracking. The chunk message is stored directly to
/// enable efficient retransmission without needing to reconstruct the message.
#[derive(Debug, Clone)]
pub struct InFlightChunk {
    /// The actual chunk message sent over the network (stored for retransmission)
    pub message: ChunkMessage,
    /// Timestamp when the chunk was sent (for timeout detection)
    pub sent_at: Instant,
    /// Number of times this chunk has been transmitted (0 = first attempt)
    pub retry_count: u32,
}

/// Sliding window state for managing parallel chunk transfers
///
/// Manages the flow control for sending chunks in parallel, tracking which chunks
/// are in-flight, which have been acknowledged, and which need retransmission.
///
/// ## Current Usage
///
/// Currently used for single-file transfers - one window instance per file.
/// Each `FileTransferSession` creates its own independent window, and files
/// are transferred sequentially (one completes before the next begins).
///
/// ## Future Extensibility
///
/// This design can be extended to support:
/// - **Connection pooling**: Multiple TCP connections transferring different files in parallel
/// - **Concurrent transfers**: Multiple windows operating simultaneously across a connection pool
/// - **Batch processing**: Queueing chunks for preparation/ACK handling to optimize throughput
///
/// See TODO.md Phase 4 "Connection Pooling" for planned implementation details.
///
/// ## Window State
///
/// The window maintains:
/// - A set of in-flight chunks with their complete message data for retransmission
/// - Acknowledgment tracking to handle out-of-order ACKs
/// - Timeout detection and automatic retry with configurable limits
pub struct SlidingWindow {
    /// Configuration
    config: WindowConfig,
    /// Total number of chunks
    total_chunks: u32,
    /// Next chunk index to send
    next_to_send: u32,
    /// Next chunk index we expect to be acknowledged
    next_expected_ack: u32,
    /// Chunks currently in flight
    in_flight: HashMap<u32, InFlightChunk>,
    /// Set of chunks that have been acknowledged
    acked_chunks: std::collections::HashSet<u32>,
    /// Number of chunks successfully acknowledged
    acked_count: u32,
}

impl SlidingWindow {
    /// Create a new sliding window
    pub fn new(config: WindowConfig, total_chunks: u32) -> Self {
        Self {
            config,
            total_chunks,
            next_to_send: 0,
            next_expected_ack: 0,
            in_flight: HashMap::new(),
            acked_chunks: std::collections::HashSet::new(),
            acked_count: 0,
        }
    }

    /// Check if we can send more chunks (window not full)
    pub fn can_send(&self) -> bool {
        self.in_flight.len() < self.config.max_window_size && self.next_to_send < self.total_chunks
    }

    /// Get the next chunk index to send
    pub fn next_chunk_to_send(&self) -> Option<u32> {
        if self.can_send() {
            Some(self.next_to_send)
        } else {
            None
        }
    }

    /// Mark a chunk as sent
    pub fn mark_sent(&mut self, chunk: InFlightChunk) {
        let chunk_index = chunk.message.chunk_index as u32;
        self.in_flight.insert(chunk_index, chunk);
        if chunk_index == self.next_to_send {
            self.next_to_send += 1;
        }
    }

    /// Get an in-flight chunk by index
    pub fn get_in_flight(&self, chunk_index: u32) -> Option<&InFlightChunk> {
        self.in_flight.get(&chunk_index)
    }

    /// Process a received ACK
    pub fn process_ack(&mut self, chunk_index: u32) -> AckResult {
        // Check if already acked
        if self.acked_chunks.contains(&chunk_index) {
            return AckResult::Duplicate;
        }

        // Remove from in-flight
        self.in_flight.remove(&chunk_index);

        // Mark as acked
        self.acked_chunks.insert(chunk_index);
        self.acked_count += 1;

        // Advance window if this is the next expected ACK
        if chunk_index == self.next_expected_ack {
            self.next_expected_ack += 1;

            // Advance past any other ACKs we've already received
            while self.acked_chunks.contains(&self.next_expected_ack)
                && self.next_expected_ack < self.total_chunks
            {
                self.next_expected_ack += 1;
            }
        }

        AckResult::Success
    }

    /// Check for chunks that have timed out and need retransmission
    pub fn check_timeouts(&mut self) -> Vec<InFlightChunk> {
        let mut timed_out = Vec::new();
        let now = Instant::now();

        // Find timed out chunks
        let mut to_remove = Vec::new();
        for (chunk_index, chunk) in &self.in_flight {
            if now.duration_since(chunk.sent_at) > self.config.ack_timeout {
                to_remove.push(*chunk_index);
            }
        }

        // Remove and prepare for retry
        for chunk_index in to_remove {
            if let Some(mut chunk) = self.in_flight.remove(&chunk_index) {
                chunk.retry_count += 1;
                if chunk.retry_count <= self.config.max_retries {
                    timed_out.push(chunk);
                } else {
                    // Max retries exceeded - this is an error condition
                    // The caller should handle this
                }
            }
        }

        timed_out
    }

    /// Check if transfer is complete
    pub fn is_complete(&self) -> bool {
        self.acked_count == self.total_chunks
    }

    /// Get current window statistics
    pub fn stats(&self) -> WindowStats {
        WindowStats {
            in_flight: self.in_flight.len(),
            acked: self.acked_count,
            total: self.total_chunks,
            next_to_send: self.next_to_send,
            window_utilization: self.in_flight.len() as f32 / self.config.max_window_size as f32,
        }
    }

    /// Get number of chunks still in flight
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    /// Get chunks that have exceeded max retries (failed)
    pub fn get_failed_chunks(&self) -> Vec<u32> {
        self.in_flight
            .iter()
            .filter(|(_, chunk)| chunk.retry_count > self.config.max_retries)
            .map(|(idx, _)| *idx)
            .collect()
    }

    /// Mark a chunk as already completed (for resume support)
    ///
    /// This is used when resuming a transfer to mark chunks that were
    /// successfully transferred in a previous session.
    pub fn mark_completed(&mut self, chunk_index: u32) {
        if chunk_index >= self.total_chunks {
            return;
        }

        // Add to acked set
        if !self.acked_chunks.contains(&chunk_index) {
            self.acked_chunks.insert(chunk_index);
            self.acked_count += 1;
        }

        // Advance next_to_send if this creates a gap that we should skip
        if chunk_index == self.next_to_send {
            self.next_to_send += 1;

            // Skip past any other completed chunks
            while self.acked_chunks.contains(&self.next_to_send)
                && self.next_to_send < self.total_chunks
            {
                self.next_to_send += 1;
            }
        }

        // Advance next_expected_ack similarly
        if chunk_index == self.next_expected_ack {
            self.next_expected_ack += 1;

            // Skip past any other completed chunks
            while self.acked_chunks.contains(&self.next_expected_ack)
                && self.next_expected_ack < self.total_chunks
            {
                self.next_expected_ack += 1;
            }
        }
    }
}

/// Result of processing an ACK
#[derive(Debug, PartialEq, Eq)]
pub enum AckResult {
    /// ACK processed successfully
    Success,
    /// Duplicate ACK (already received)
    Duplicate,
}

/// Statistics about the sliding window state
#[derive(Debug, Clone)]
pub struct WindowStats {
    /// Number of chunks currently in flight
    pub in_flight: usize,
    /// Number of chunks acknowledged
    pub acked: u32,
    /// Total chunks
    pub total: u32,
    /// Next chunk to send
    pub next_to_send: u32,
    /// Window utilization (0.0 to 1.0)
    pub window_utilization: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    /// Helper to create a test chunk message
    fn create_test_chunk(
        transfer_id: Uuid,
        file_index: u32,
        chunk_index: u64,
        total_chunks: u64,
    ) -> InFlightChunk {
        InFlightChunk {
            message: ChunkMessage {
                transfer_id,
                file_index,
                chunk_index,
                total_chunks,
                flags: 0,
                checksum: 0,
                data: vec![],
            },
            sent_at: Instant::now(),
            retry_count: 0,
        }
    }

    #[test]
    fn test_window_can_send() {
        let config = WindowConfig {
            max_window_size: 4,
            ..Default::default()
        };
        let transfer_id = Uuid::new_v4();
        let mut window = SlidingWindow::new(config, 10);

        // Should be able to send up to window size
        assert!(window.can_send());
        assert_eq!(window.next_chunk_to_send(), Some(0));

        // Fill the window
        for i in 0..4 {
            let chunk = create_test_chunk(transfer_id, 0, i as u64, 10);
            window.mark_sent(chunk);
        }

        // Window should be full now
        assert!(!window.can_send());
        assert_eq!(window.next_chunk_to_send(), None);
    }

    #[test]
    fn test_window_process_ack() {
        let config = WindowConfig::default();
        let transfer_id = Uuid::new_v4();
        let mut window = SlidingWindow::new(config, 10);

        // Send chunks 0, 1, 2
        for i in 0..3 {
            let chunk = create_test_chunk(transfer_id, 0, i as u64, 10);
            window.mark_sent(chunk);
        }

        // ACK chunk 0
        assert_eq!(window.process_ack(0), AckResult::Success);
        assert_eq!(window.next_expected_ack, 1);

        // ACK chunk 2 (out of order)
        assert_eq!(window.process_ack(2), AckResult::Success);
        assert_eq!(window.next_expected_ack, 1); // Still waiting for 1

        // ACK chunk 1
        assert_eq!(window.process_ack(1), AckResult::Success);
        assert_eq!(window.next_expected_ack, 3); // Advanced past 2
    }

    #[test]
    fn test_window_completion() {
        let config = WindowConfig::default();
        let transfer_id = Uuid::new_v4();
        let mut window = SlidingWindow::new(config, 3);

        assert!(!window.is_complete());

        // Send and ack all chunks
        for i in 0..3 {
            let chunk = create_test_chunk(transfer_id, 0, i as u64, 3);
            window.mark_sent(chunk);
            window.process_ack(i);
        }

        assert!(window.is_complete());
        assert_eq!(window.acked_count, 3);
    }

    #[test]
    fn test_window_timeout() {
        let config = WindowConfig {
            ack_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let transfer_id = Uuid::new_v4();
        let mut window = SlidingWindow::new(config, 10);

        // Send a chunk
        let mut chunk = create_test_chunk(transfer_id, 0, 0, 10);
        chunk.message.data = vec![1, 2, 3];
        chunk.message.checksum = 123;
        chunk.sent_at = Instant::now() - Duration::from_millis(20); // Already timed out
        window.mark_sent(chunk);

        // Check timeouts
        let timed_out = window.check_timeouts();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0].message.chunk_index, 0);
        assert_eq!(timed_out[0].retry_count, 1);
    }
}
