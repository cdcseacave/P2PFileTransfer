//! Transfer history tracking
//!
//! This module provides functionality to track and query past transfers.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Direction of a transfer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferDirection {
    /// Sending files
    Send,
    /// Receiving files
    Receive,
}

/// Status of a completed transfer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    /// Transfer completed successfully
    Completed,
    /// Transfer was interrupted
    Interrupted,
    /// Transfer failed with error
    Failed,
}

/// A single transfer record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferRecord {
    /// Unique transfer ID
    pub transfer_id: Uuid,
    /// Timestamp when transfer started (Unix timestamp)
    pub start_time: u64,
    /// Timestamp when transfer ended (Unix timestamp)
    pub end_time: u64,
    /// Direction (send or receive)
    pub direction: TransferDirection,
    /// Peer address
    pub peer_address: String,
    /// List of files transferred (paths)
    pub files: Vec<String>,
    /// Total bytes transferred
    pub bytes_transferred: u64,
    /// Duration in seconds
    pub duration_secs: u64,
    /// Final status
    pub status: TransferStatus,
}

impl TransferRecord {
    /// Create a new transfer record
    pub fn new(transfer_id: Uuid, direction: TransferDirection, peer_address: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            transfer_id,
            start_time: now,
            end_time: now,
            direction,
            peer_address,
            files: Vec::new(),
            bytes_transferred: 0,
            duration_secs: 0,
            status: TransferStatus::Interrupted,
        }
    }

    /// Mark transfer as completed
    pub fn complete(&mut self, files: Vec<String>, bytes_transferred: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.end_time = now;
        self.duration_secs = now.saturating_sub(self.start_time);
        self.files = files;
        self.bytes_transferred = bytes_transferred;
        self.status = TransferStatus::Completed;
    }

    /// Mark transfer as interrupted
    pub fn interrupt(&mut self, files: Vec<String>, bytes_transferred: u64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.end_time = now;
        self.duration_secs = now.saturating_sub(self.start_time);
        self.files = files;
        self.bytes_transferred = bytes_transferred;
        self.status = TransferStatus::Interrupted;
    }

    /// Mark transfer as failed
    pub fn fail(&mut self, error: String) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.end_time = now;
        self.duration_secs = now.saturating_sub(self.start_time);
        self.status = TransferStatus::Failed;

        // Store error in files list for now (could add dedicated error field)
        self.files.push(format!("Error: {}", error));
    }
}

/// Transfer history manager
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransferHistory {
    /// List of transfer records
    records: Vec<TransferRecord>,
}

impl TransferHistory {
    /// Create a new empty transfer history
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    /// Add a transfer record
    pub fn add_record(&mut self, record: TransferRecord) {
        self.records.push(record);
    }

    /// Get all transfer records
    pub fn records(&self) -> &[TransferRecord] {
        &self.records
    }

    /// Get records filtered by direction
    pub fn filter_by_direction(&self, direction: TransferDirection) -> Vec<&TransferRecord> {
        self.records
            .iter()
            .filter(|r| r.direction == direction)
            .collect()
    }

    /// Get records filtered by status
    pub fn filter_by_status(&self, status: TransferStatus) -> Vec<&TransferRecord> {
        self.records.iter().filter(|r| r.status == status).collect()
    }

    /// Get a specific transfer by ID
    pub fn get_by_id(&self, transfer_id: Uuid) -> Option<&TransferRecord> {
        self.records.iter().find(|r| r.transfer_id == transfer_id)
    }

    /// Get most recent transfers (up to limit)
    pub fn recent(&self, limit: usize) -> Vec<&TransferRecord> {
        let mut records: Vec<&TransferRecord> = self.records.iter().collect();
        records.sort_by_key(|r| std::cmp::Reverse(r.start_time));
        records.into_iter().take(limit).collect()
    }

    /// Load history from file
    pub async fn load_from_file(path: &Path) -> Result<Self> {
        let data = tokio::fs::read(path).await?;

        serde_json::from_slice(&data)
            .map_err(|e| Error::Protocol(format!("Failed to deserialize history: {}", e)))
    }

    /// Save history to file
    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let data = serde_json::to_vec_pretty(self)
            .map_err(|e| Error::Protocol(format!("Failed to serialize history: {}", e)))?;

        tokio::fs::write(path, data).await?;
        Ok(())
    }

    /// Get default history file path
    pub fn default_path() -> PathBuf {
        if let Some(home) = dirs::home_dir() {
            home.join(".p2p-transfer").join("history.json")
        } else {
            PathBuf::from("transfer_history.json")
        }
    }
}

/// Append a finalized [`TransferRecord`] to the on-disk history at
/// `history_path` (or [`TransferHistory::default_path`] when `None`).
///
/// Concurrency: the file is opened with an OS-level exclusive lock for the
/// duration of the read-modify-write so co-located CLI processes (e.g. a
/// sender and a receiver on the same machine) cannot clobber each other.
/// A missing file is treated as empty history; a corrupt file is overwritten.
pub async fn record_transfer(record: TransferRecord, history_path: Option<&Path>) -> Result<()> {
    let path: PathBuf = match history_path {
        Some(p) => p.to_path_buf(),
        None => TransferHistory::default_path(),
    };

    tokio::task::spawn_blocking(move || append_record_locked(&path, record))
        .await
        .map_err(|e| Error::Protocol(format!("history task join: {e}")))?
}

fn append_record_locked(path: &Path, record: TransferRecord) -> Result<()> {
    use fs2::FileExt;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(Error::Network)?;
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(Error::Network)?;

    file.lock_exclusive().map_err(Error::Network)?;

    let mut buf = Vec::new();
    file.read_to_end(&mut buf).map_err(Error::Network)?;

    let mut history: TransferHistory = if buf.is_empty() {
        TransferHistory::default()
    } else {
        serde_json::from_slice(&buf).unwrap_or_default()
    };
    history.add_record(record);

    let data = serde_json::to_vec_pretty(&history)
        .map_err(|e| Error::Protocol(format!("Failed to serialize history: {}", e)))?;
    file.seek(SeekFrom::Start(0)).map_err(Error::Network)?;
    file.set_len(0).map_err(Error::Network)?;
    file.write_all(&data).map_err(Error::Network)?;

    fs2::FileExt::unlock(&file).map_err(Error::Network)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_record_creation() {
        let record = TransferRecord::new(
            Uuid::new_v4(),
            TransferDirection::Send,
            "127.0.0.1".to_string(),
        );

        assert_eq!(record.direction, TransferDirection::Send);
        assert_eq!(record.status, TransferStatus::Interrupted);
        assert_eq!(record.bytes_transferred, 0);
    }

    #[test]
    fn test_transfer_completion() {
        let mut record = TransferRecord::new(
            Uuid::new_v4(),
            TransferDirection::Send,
            "127.0.0.1".to_string(),
        );

        let files = vec!["file1.txt".to_string(), "file2.txt".to_string()];
        record.complete(files.clone(), 1024);

        assert_eq!(record.status, TransferStatus::Completed);
        assert_eq!(record.bytes_transferred, 1024);
        assert_eq!(record.files, files);
        // duration_secs should be positive (end_time >= start_time)
        assert!(record.duration_secs > 0 || record.end_time == record.start_time);
    }

    #[test]
    fn test_history_management() {
        let mut history = TransferHistory::new();

        let record1 = TransferRecord::new(
            Uuid::new_v4(),
            TransferDirection::Send,
            "127.0.0.1".to_string(),
        );

        let record2 = TransferRecord::new(
            Uuid::new_v4(),
            TransferDirection::Receive,
            "127.0.0.1".to_string(),
        );

        history.add_record(record1.clone());
        history.add_record(record2.clone());

        assert_eq!(history.records().len(), 2);
        assert_eq!(
            history.filter_by_direction(TransferDirection::Send).len(),
            1
        );
        assert_eq!(
            history
                .filter_by_direction(TransferDirection::Receive)
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn record_transfer_concurrent_appends_dont_clobber() {
        let tmp = tempfile::tempdir().unwrap();
        let path = std::sync::Arc::new(tmp.path().join("history.json"));

        let n = 16usize;
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let path = path.clone();
            handles.push(tokio::spawn(async move {
                let mut r = TransferRecord::new(
                    Uuid::new_v4(),
                    TransferDirection::Send,
                    format!("10.0.0.{}", i),
                );
                r.complete(vec![format!("file-{}.bin", i)], i as u64 * 100);
                record_transfer(r, Some(&path)).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let loaded = TransferHistory::load_from_file(&path).await.unwrap();
        assert_eq!(
            loaded.records().len(),
            n,
            "concurrent record_transfer calls must not clobber each other"
        );
    }

    #[tokio::test]
    async fn record_transfer_appends_and_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("history.json");

        let mut a = TransferRecord::new(Uuid::new_v4(), TransferDirection::Send, "1.1.1.1:1".into());
        a.complete(vec!["a.bin".into()], 100);
        record_transfer(a, Some(&path)).await.unwrap();

        let mut b = TransferRecord::new(Uuid::new_v4(), TransferDirection::Receive, "2.2.2.2:2".into());
        b.fail("boom".into());
        record_transfer(b, Some(&path)).await.unwrap();

        let loaded = TransferHistory::load_from_file(&path).await.unwrap();
        assert_eq!(loaded.records().len(), 2);
        assert_eq!(loaded.records()[0].status, TransferStatus::Completed);
        assert_eq!(loaded.records()[1].status, TransferStatus::Failed);
    }

    #[tokio::test]
    async fn test_history_persistence() {
        let temp_dir = tempfile::tempdir().unwrap();
        let history_path = temp_dir.path().join("history.json");

        let mut history = TransferHistory::new();
        let record = TransferRecord::new(
            Uuid::new_v4(),
            TransferDirection::Send,
            "127.0.0.1".to_string(),
        );
        history.add_record(record);

        // Save and load
        history.save_to_file(&history_path).await.unwrap();
        let loaded = TransferHistory::load_from_file(&history_path)
            .await
            .unwrap();

        assert_eq!(loaded.records().len(), 1);
        assert_eq!(loaded.records()[0].direction, TransferDirection::Send);
    }
}
