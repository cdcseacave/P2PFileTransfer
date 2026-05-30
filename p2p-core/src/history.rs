//! Transfer history tracking
//!
//! This module provides functionality to track and query past transfers.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tracing::warn;
use uuid::Uuid;

/// Seconds since the Unix epoch, or 0 if the system clock is set before
/// 1970 (RTC battery dead, container with bogus time). Using `unwrap()` here
/// previously crashed the receive loop on bad-clock hosts even though the
/// transfer itself was fine — see review finding 5.1.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

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
        let now = now_secs();
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
        let now = now_secs();
        self.end_time = now;
        self.duration_secs = now.saturating_sub(self.start_time);
        self.files = files;
        self.bytes_transferred = bytes_transferred;
        self.status = TransferStatus::Completed;
    }

    /// Mark transfer as interrupted
    pub fn interrupt(&mut self, files: Vec<String>, bytes_transferred: u64) {
        let now = now_secs();
        self.end_time = now;
        self.duration_secs = now.saturating_sub(self.start_time);
        self.files = files;
        self.bytes_transferred = bytes_transferred;
        self.status = TransferStatus::Interrupted;
    }

    /// Mark transfer as failed
    pub fn fail(&mut self, error: String) {
        let now = now_secs();
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
/// Concurrency: an OS-level exclusive lock is held over a sibling `.lock`
/// file for the duration of the read-modify-write so co-located CLI
/// processes (e.g. a sender and a receiver on the same machine) cannot
/// clobber each other.
///
/// Durability: the new history is written to `<path>.tmp`, fsynced, and
/// then atomically renamed over `<path>`. A crash mid-write cannot
/// produce an empty `history.json`.
///
/// Corruption recovery: if the existing `history.json` fails to parse,
/// the corrupt bytes are renamed to `history.json.corrupt-<unix_secs>`
/// (so the user can recover them out-of-band) and a fresh history
/// containing only the new record is written.
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
    use std::io::Write;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(Error::Network)?;
    }

    // Lock a sibling `.lock` file rather than `history.json` itself —
    // we never truncate or rename the lock target, so the lock identity
    // is stable across the atomic rename of the real history file.
    let lock_path = sibling_path(path, ".lock");
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(Error::Network)?;
    lock_file.lock_exclusive().map_err(Error::Network)?;

    let mut history = load_or_quarantine(path)?;
    history.add_record(record);

    let data = serde_json::to_vec_pretty(&history)
        .map_err(|e| Error::Protocol(format!("Failed to serialize history: {}", e)))?;

    let tmp_path = sibling_path(path, ".tmp");
    {
        let mut tmp = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(Error::Network)?;
        tmp.write_all(&data).map_err(Error::Network)?;
        tmp.sync_all().map_err(Error::Network)?;
    }
    std::fs::rename(&tmp_path, path).map_err(Error::Network)?;

    if let Err(e) = fs2::FileExt::unlock(&lock_file) {
        // The bytes are already durable on disk; failing the whole call
        // would mis-report success as failure. Drop the handle below — the
        // OS releases the lock either way.
        warn!("history lock unlock failed (record is persisted): {e}");
    }
    Ok(())
}

/// Read `history.json` if it exists; on parse failure rename the corrupt
/// bytes aside and start fresh, so a single bad byte never wipes the user's
/// audit log silently.
fn load_or_quarantine(path: &Path) -> Result<TransferHistory> {
    let buf = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(TransferHistory::default()),
        Err(e) => return Err(Error::Network(e)),
    };
    if buf.is_empty() {
        return Ok(TransferHistory::default());
    }
    match serde_json::from_slice::<TransferHistory>(&buf) {
        Ok(h) => Ok(h),
        Err(e) => {
            let quarantine = sibling_path(path, &format!(".corrupt-{}", now_secs()));
            warn!(
                "history.json failed to parse ({e}); preserving original bytes at {}",
                quarantine.display()
            );
            // Rename rather than copy — preserves inode and never loses data
            // even if the disk fills up between read and write.
            std::fs::rename(path, &quarantine).map_err(Error::Network)?;
            Ok(TransferHistory::default())
        }
    }
}

/// Build a sibling path like `<file>.<suffix>` (e.g. `history.json.tmp`).
fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
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

        let mut a =
            TransferRecord::new(Uuid::new_v4(), TransferDirection::Send, "1.1.1.1:1".into());
        a.complete(vec!["a.bin".into()], 100);
        record_transfer(a, Some(&path)).await.unwrap();

        let mut b = TransferRecord::new(
            Uuid::new_v4(),
            TransferDirection::Receive,
            "2.2.2.2:2".into(),
        );
        b.fail("boom".into());
        record_transfer(b, Some(&path)).await.unwrap();

        let loaded = TransferHistory::load_from_file(&path).await.unwrap();
        assert_eq!(loaded.records().len(), 2);
        assert_eq!(loaded.records()[0].status, TransferStatus::Completed);
        assert_eq!(loaded.records()[1].status, TransferStatus::Failed);
    }

    /// Finding 1.4: a corrupt history.json must NOT be silently overwritten.
    /// `unwrap_or_default()` on parse failure threw away the user's entire
    /// audit log; we instead quarantine the corrupt bytes to a side file so
    /// the user can recover, then start a fresh history with the new record.
    #[tokio::test]
    async fn record_transfer_quarantines_corrupt_file_instead_of_overwriting() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("history.json");

        let garbage: &[u8] = b"\xff\xfe\xfdNOT JSON {{{ broken";
        tokio::fs::write(&path, garbage).await.unwrap();

        let mut rec =
            TransferRecord::new(Uuid::new_v4(), TransferDirection::Send, "1.1.1.1:1".into());
        rec.complete(vec!["after-corruption.bin".into()], 42);
        record_transfer(rec, Some(&path)).await.unwrap();

        let loaded = TransferHistory::load_from_file(&path).await.unwrap();
        assert_eq!(loaded.records().len(), 1);
        assert_eq!(loaded.records()[0].files, vec!["after-corruption.bin"]);

        let mut quarantined: Vec<std::path::PathBuf> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("history.json.corrupt-"))
            })
            .collect();
        assert_eq!(
            quarantined.len(),
            1,
            "expected exactly one quarantined corrupt file in {:?}",
            tmp.path()
        );
        let recovered = std::fs::read(quarantined.remove(0)).unwrap();
        assert_eq!(
            recovered, garbage,
            "quarantined file must preserve the original corrupt bytes"
        );
    }

    /// Finding 5.1: a bad system clock (RTC battery dead, container with
    /// bogus time, pre-1970 instant) must not panic the receive loop. All
    /// constructors degrade to timestamp 0 instead of `.unwrap()`.
    ///
    /// This test cannot rewind the real clock; instead it exercises every
    /// constructor (which previously unwrapped) and asserts no panic.
    #[test]
    fn timestamp_helpers_never_panic() {
        let mut r = TransferRecord::new(
            Uuid::new_v4(),
            TransferDirection::Send,
            "127.0.0.1:1".into(),
        );
        r.complete(vec!["a".into()], 1);
        r.interrupt(vec!["a".into()], 1);
        r.fail("err".into());
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
