//! Transfer state management

use crate::protocol::FileMetadata;
use bitvec::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

/// Transfer state for persistence and resume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferState {
    /// Unique transfer identifier
    pub transfer_id: Uuid,
    /// Peer address
    pub peer_address: String,
    /// List of files in transfer
    pub files: Vec<FileState>,
    /// Timestamp of last update
    pub timestamp: u64,
    /// Total bytes to transfer
    pub total_bytes: u64,
    /// Bytes transferred so far
    pub transferred_bytes: u64,
}

/// State of a single file in transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    /// File index in transfer
    pub file_index: u32,
    /// File metadata
    pub metadata: FileMetadata,
    /// Local file path
    pub local_path: PathBuf,
    /// Total chunks in file
    pub total_chunks: u64,
    /// Bitmap of completed chunks
    pub completed_chunks: BitVec<u8, Lsb0>,
    /// File completion status
    pub status: FileStatus,
}

/// File completion status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileStatus {
    /// Not started
    Pending,
    /// In progress
    InProgress,
    /// All chunks received
    Complete,
    /// Verified successfully
    Verified,
    /// Failed
    Failed,
}

impl TransferState {
    /// Create a new transfer state
    pub fn new(
        transfer_id: Uuid,
        peer_address: String,
        files: Vec<FileMetadata>,
        chunk_size: u32,
    ) -> Self {
        let total_bytes = files.iter().map(|f| f.size).sum();
        let file_states = files
            .into_iter()
            .enumerate()
            .map(|(idx, metadata)| {
                let total_chunks = (metadata.size + chunk_size as u64 - 1) / chunk_size as u64;
                FileState {
                    file_index: idx as u32,
                    metadata,
                    local_path: PathBuf::new(),
                    total_chunks,
                    completed_chunks: bitvec![u8, Lsb0; 0; total_chunks as usize],
                    status: FileStatus::Pending,
                }
            })
            .collect();

        Self {
            transfer_id,
            peer_address,
            files: file_states,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            total_bytes,
            transferred_bytes: 0,
        }
    }

    /// Mark a chunk as completed
    pub fn mark_chunk_complete(&mut self, file_index: u32, chunk_index: u64, chunk_size: u64) {
        if let Some(file) = self.files.get_mut(file_index as usize) {
            if chunk_index < file.total_chunks {
                file.completed_chunks.set(chunk_index as usize, true);
                self.transferred_bytes += chunk_size;

                // Update file status
                if file.completed_chunks.all() && file.status == FileStatus::InProgress {
                    file.status = FileStatus::Complete;
                } else if file.status == FileStatus::Pending {
                    file.status = FileStatus::InProgress;
                }
            }
        }
    }

    /// Check if transfer is complete
    pub fn is_complete(&self) -> bool {
        self.files.iter().all(|f| f.status == FileStatus::Verified)
    }

    /// Get progress percentage (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        if self.total_bytes == 0 {
            return 1.0;
        }
        self.transferred_bytes as f64 / self.total_bytes as f64
    }

    /// Get next chunk to send/receive
    pub fn next_chunk(&self) -> Option<(u32, u64)> {
        for file in &self.files {
            if file.status == FileStatus::Pending || file.status == FileStatus::InProgress {
                if let Some(chunk_idx) = file.completed_chunks.iter().position(|b| !*b) {
                    return Some((file.file_index, chunk_idx as u64));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_state_creation() {
        let metadata = FileMetadata {
            path: "test.txt".to_string(),
            size: 1000,
            modified: 0,
            checksum: [0; 32],
        };

        let state =
            TransferState::new(Uuid::new_v4(), "127.0.0.1".to_string(), vec![metadata], 100);

        assert_eq!(state.files.len(), 1);
        assert_eq!(state.files[0].total_chunks, 10);
        assert_eq!(state.progress(), 0.0);
    }

    #[test]
    fn test_chunk_completion() {
        let metadata = FileMetadata {
            path: "test.txt".to_string(),
            size: 300,
            modified: 0,
            checksum: [0; 32],
        };

        let mut state =
            TransferState::new(Uuid::new_v4(), "127.0.0.1".to_string(), vec![metadata], 100);

        state.mark_chunk_complete(0, 0, 100);
        assert_eq!(state.progress(), 100.0 / 300.0);

        state.mark_chunk_complete(0, 1, 100);
        state.mark_chunk_complete(0, 2, 100);
        assert_eq!(state.progress(), 1.0);
        assert_eq!(state.files[0].status, FileStatus::Complete);
    }

    #[test]
    fn test_next_chunk() {
        let metadata = FileMetadata {
            path: "test.txt".to_string(),
            size: 300,
            modified: 0,
            checksum: [0; 32],
        };

        let mut state =
            TransferState::new(Uuid::new_v4(), "127.0.0.1".to_string(), vec![metadata], 100);

        assert_eq!(state.next_chunk(), Some((0, 0)));

        state.mark_chunk_complete(0, 0, 100);
        assert_eq!(state.next_chunk(), Some((0, 1)));

        state.mark_chunk_complete(0, 1, 100);
        state.mark_chunk_complete(0, 2, 100);
        assert_eq!(state.next_chunk(), None);
    }
}
