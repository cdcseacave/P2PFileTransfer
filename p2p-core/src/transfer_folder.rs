//! Folder-level transfer orchestration.
//!
//! A folder transfer is a sequence of single-file transfers reusing the
//! same QUIC connection. After all files are sent, the sender emits a
//! `Complete` control message; per-file SHA-256s are exchanged via
//! `FileChecksum` control messages so both sides agree on integrity.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use crate::bandwidth;
use crate::error::{Error, Result};
use crate::network::quic::QuicConnection;
use crate::progress::ProgressState;
use crate::protocol::{
    CompleteMessage, ConfigMessage, FileChecksumMessage, FileMetadata, Message, ResumePoint,
    TransferInfo,
};
use crate::transfer_file::FileTransferSession;

/// Statistics emitted at end of a folder transfer.
#[derive(Debug, Clone)]
pub struct TransferStats {
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub duration_secs: f64,
    pub compression_ratio: f64,
    pub compression_percent: f64,
    pub network_speed_mbps: f64,
    pub felt_speed_mbps: f64,
}

/// Callback fired after each file completes so the caller can persist state.
pub type StateCallback = std::sync::Arc<dyn Fn(&FolderTransferState) + Send + Sync>;

/// Folder transfer session — orchestrates many single-file transfers over
/// one borrowed [`QuicConnection`].
pub struct FolderTransferSession<'a> {
    connection: &'a mut QuicConnection,
    config: ConfigMessage,
    transfer_id: Uuid,
    state_callback: Option<StateCallback>,
    total_compressed_bytes: u64,
    transfer_start: Option<Instant>,
}

impl<'a> FolderTransferSession<'a> {
    pub fn new(
        connection: &'a mut QuicConnection,
        config: ConfigMessage,
        transfer_id: Uuid,
    ) -> Self {
        Self {
            connection,
            config,
            transfer_id,
            state_callback: None,
            total_compressed_bytes: 0,
            transfer_start: None,
        }
    }

    pub fn set_state_callback(&mut self, callback: StateCallback) {
        self.state_callback = Some(callback);
    }

    fn calc_compression_stats(&self, total_bytes: u64) -> (f64, f64) {
        let ratio = if total_bytes > 0 {
            total_bytes as f64 / self.total_compressed_bytes as f64
        } else {
            1.0
        };
        let percent = if total_bytes >= self.total_compressed_bytes {
            (total_bytes - self.total_compressed_bytes) as f64 / total_bytes as f64 * 100.0
        } else {
            -((self.total_compressed_bytes - total_bytes) as f64 / total_bytes as f64 * 100.0)
        };
        (ratio, percent)
    }

    fn display_transfer_stats(
        &self,
        total_files: usize,
        total_bytes: u64,
        duration_secs: f64,
        is_sender: bool,
    ) {
        info!("Transfer Statistics:");
        let action = if is_sender { "sent" } else { "received" };

        if self.config.compression_enabled && self.total_compressed_bytes > 0 {
            let (ratio, percent) = self.calc_compression_stats(total_bytes);
            let network_speed = if duration_secs > 0.0 {
                self.total_compressed_bytes as f64 / duration_secs / 1_048_576.0
            } else {
                0.0
            };
            let felt_speed = if duration_secs > 0.0 {
                total_bytes as f64 / duration_secs / 1_048_576.0
            } else {
                0.0
            };
            let direction = if is_sender { "->" } else { "<-" };
            if percent >= 0.0 {
                info!(
                    "   Data: {} {} {} ({:.1}% saved, {:.2}x compression)",
                    bandwidth::format_bandwidth(total_bytes),
                    direction,
                    bandwidth::format_bandwidth(self.total_compressed_bytes),
                    percent,
                    ratio
                );
            } else {
                info!(
                    "   Data: {} {} {} ({:.1}% overhead)",
                    bandwidth::format_bandwidth(total_bytes),
                    direction,
                    bandwidth::format_bandwidth(self.total_compressed_bytes),
                    -percent
                );
            }
            info!(
                "   Speed: {:.2} MB/s network, {:.2} MB/s throughput",
                network_speed, felt_speed
            );
            info!(
                "Folder transfer complete: {} files, {} {}",
                total_files,
                bandwidth::format_bandwidth(total_bytes),
                action
            );
        } else {
            if duration_secs > 0.0 {
                info!(
                    "   Speed: {:.2} MB/s",
                    total_bytes as f64 / duration_secs / 1_048_576.0
                );
            }
            info!(
                "Folder transfer complete: {} files, {} {}",
                total_files,
                bandwidth::format_bandwidth(total_bytes),
                action
            );
        }
    }

    /// Send a file or folder, updating `state` as chunks complete (for resume).
    pub async fn send(
        &mut self,
        path: &Path,
        state: &mut FolderTransferState,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        self.transfer_start = Some(Instant::now());
        self.total_compressed_bytes = 0;

        let resume_point = if !state.files.is_empty() {
            info!(
                "Resuming transfer: {} of {} files done",
                state.completed_files.len(),
                state.files.len()
            );
            if let Some(next_file) = state.next_file() {
                let completed = state.get_completed_chunks(next_file);
                if !completed.is_empty() {
                    Some(ResumePoint {
                        transfer_id: self.transfer_id,
                        file_index: next_file as u32,
                        completed_chunks: completed.to_vec(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            info!("Starting transfer: {:?}", path);

            let base_name = path
                .file_name()
                .ok_or_else(|| Error::Protocol("Invalid path".to_string()))?
                .to_string_lossy()
                .to_string();

            let files = if path.is_file() {
                let metadata = fs::metadata(path).await?;
                let size = metadata.len();
                let modified = metadata
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let file_name = path.file_name().unwrap().to_string_lossy().to_string();
                vec![(
                    PathBuf::from(file_name.clone()),
                    FileMetadata {
                        path: file_name,
                        size,
                        modified,
                        checksum: [0u8; 32],
                    },
                )]
            } else if path.is_dir() {
                let files = self.scan_folder(path).await?;
                if files.is_empty() {
                    return Err(Error::Protocol("Folder is empty".to_string()));
                }
                files
            } else {
                return Err(Error::Protocol(
                    "Path is neither a file nor a directory".to_string(),
                ));
            };

            let file_list: Vec<FileMetadata> = files.iter().map(|(_, m)| m.clone()).collect();
            *state = FolderTransferState::new(self.transfer_id, base_name, file_list);
            None
        };

        let total_files = state.files.len();
        let total_bytes = state.total_bytes;
        if let Some(ref mut p) = progress {
            p.set_total_bytes(total_bytes);
        }

        let is_resuming = resume_point.is_some();
        let transfer_info = TransferInfo {
            transfer_id: self.transfer_id,
            items: state.files.clone(),
            resume_from: resume_point,
        };
        self.connection
            .send_message(&Message::TransferInfo(transfer_info))
            .await?;

        match self.connection.recv_message().await? {
            Message::Ready => {}
            msg => return Err(Error::Protocol(format!("Expected Ready, got {:?}", msg))),
        }
        debug!(
            "Receiver ready, {}",
            if is_resuming { "resuming" } else { "starting" }
        );

        let base_path = if path.is_file() {
            path.parent()
                .ok_or_else(|| Error::Protocol("File has no parent directory".to_string()))?
        } else {
            path.parent().unwrap_or(path)
        };

        for file_index in 0..state.files.len() {
            if state.completed_files.contains(&file_index) {
                continue;
            }
            let file_meta = &state.files[file_index];
            let relative_path = PathBuf::from(&file_meta.path);
            let full_path = base_path.join(&relative_path);
            let completed_chunks = state.get_completed_chunks(file_index).to_vec();

            let chunk_callback = |chunk_index: u64| {
                state.mark_chunk_complete(file_index, chunk_index);
            };

            self.send_single_file(
                &full_path,
                file_index as u32,
                &completed_chunks,
                progress.as_deref_mut(),
                Some(chunk_callback),
            )
            .await?;

            state.mark_file_complete(file_index);
            state.current_file = state.next_file();
            if let Some(cb) = &self.state_callback {
                cb(state);
            }
            trace!("File {} complete", relative_path.display());
        }

        let duration = self.transfer_start.map(|s| s.elapsed()).unwrap_or_default();
        let complete = CompleteMessage {
            transfer_id: self.transfer_id,
            total_bytes,
            duration_ms: duration.as_millis() as u64,
        };
        self.connection
            .send_message(&Message::Complete(complete))
            .await?;

        if let Some(ref mut p) = progress {
            p.finish();
        }

        self.display_transfer_stats(total_files, total_bytes, duration.as_secs_f64(), true);
        Ok(())
    }

    /// Receive a folder from the peer.
    pub async fn receive_folder(
        &mut self,
        output_dir: &Path,
        state_path: Option<&Path>,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        let transfer_info = match self.connection.recv_message().await? {
            Message::TransferInfo(info) => info,
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected TransferInfo, got {:?}",
                    msg
                )))
            }
        };
        if transfer_info.items.is_empty() {
            return Err(Error::Protocol("No files in transfer".to_string()));
        }

        info!("Starting receive to: {:?}", output_dir);
        let is_resume = transfer_info.resume_from.is_some();

        if let Some(state_file) = state_path {
            if state_file.exists() {
                match FolderTransferState::load_from_file(state_file).await {
                    Ok(existing) if existing.transfer_id == transfer_info.transfer_id => {
                        info!(
                            "Detected existing transfer {}, resuming automatically",
                            transfer_info.transfer_id
                        );
                    }
                    Ok(_) => {}
                    Err(e) => warn!("Failed to load existing state: {}", e),
                }
            }
        }

        self.transfer_id = transfer_info.transfer_id;
        self.transfer_start = Some(Instant::now());
        self.total_compressed_bytes = 0;

        let total_bytes: u64 = transfer_info.items.iter().map(|f| f.size).sum();

        let mut already_transferred = 0u64;
        if let Some(ref resume_point) = transfer_info.resume_from {
            let file_index = resume_point.file_index as usize;
            for i in 0..file_index.min(transfer_info.items.len()) {
                already_transferred += transfer_info.items[i].size;
            }
            if file_index < transfer_info.items.len() {
                let chunk_size = self.config.chunk_size as u64;
                let current_size = transfer_info.items[file_index].size;
                let total_chunks = (current_size + chunk_size - 1) / chunk_size;
                let completed_chunks = resume_point.completed_chunks.len() as u64;
                let added = if completed_chunks < total_chunks {
                    completed_chunks * chunk_size
                } else {
                    current_size
                };
                already_transferred += added;
            }
            info!(
                "Resume: {} bytes already transferred ({:.1}%)",
                already_transferred,
                (already_transferred as f64 / total_bytes as f64) * 100.0
            );
        }

        if let Some(ref mut p) = progress {
            p.set_total_bytes(total_bytes);
            if already_transferred > 0 {
                p.add_bytes(already_transferred);
            }
        }

        fs::create_dir_all(output_dir).await?;
        self.connection.send_message(&Message::Ready).await?;

        let total_files = transfer_info.items.len();
        for (file_index, file_meta) in transfer_info.items.iter().enumerate() {
            let relative_path = PathBuf::from(&file_meta.path);
            let full_path = output_dir.join(&relative_path);
            info!(
                "Receiving file {}/{}: {}",
                file_index + 1,
                total_files,
                relative_path.display()
            );
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent).await?;
            }

            let expected_chunks = (file_meta.size + self.config.chunk_size as u64 - 1)
                / self.config.chunk_size as u64;

            self.receive_single_file(
                &full_path,
                file_index as u32,
                expected_chunks,
                progress.as_deref_mut(),
            )
            .await?;
            trace!("File {} complete", relative_path.display());
        }

        match self.connection.recv_message().await? {
            Message::Complete(_) => {}
            msg => warn!("Expected Complete message, got {:?}", msg),
        }

        if let Some(ref mut p) = progress {
            p.finish();
        }
        let duration = self.transfer_start.map(|s| s.elapsed()).unwrap_or_default();
        self.display_transfer_stats(total_files, total_bytes, duration.as_secs_f64(), false);

        let _ = is_resume;
        Ok(())
    }

    async fn send_single_file<F>(
        &mut self,
        path: &Path,
        file_index: u32,
        completed_chunks: &[u64],
        progress: Option<&mut ProgressState>,
        chunk_complete_callback: Option<F>,
    ) -> Result<()>
    where
        F: FnMut(u64),
    {
        let mut file_session = FileTransferSession::new(
            self.connection,
            self.config.clone(),
            self.transfer_id,
            file_index,
        );

        let sender_checksum = file_session
            .send_file(path, completed_chunks, chunk_complete_callback, progress)
            .await?;
        self.total_compressed_bytes += file_session.compressed_bytes_sent;

        let checksum_msg = FileChecksumMessage {
            transfer_id: self.transfer_id,
            file_index,
            checksum: sender_checksum,
        };
        self.connection
            .send_message(&Message::FileChecksum(checksum_msg))
            .await?;

        match self.connection.recv_message().await? {
            Message::FileChecksum(peer_msg) => {
                if peer_msg.checksum != sender_checksum {
                    return Err(Error::Verification(format!(
                        "File checksum mismatch for file {}: sender={:02x?}, receiver={:02x?}",
                        file_index,
                        &sender_checksum[..8],
                        &peer_msg.checksum[..8]
                    )));
                }
                debug!(
                    "File {} checksum verified: {:02x?}",
                    file_index,
                    &sender_checksum[..8]
                );
            }
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected FileChecksum, got {:?}",
                    msg
                )))
            }
        }
        Ok(())
    }

    async fn receive_single_file(
        &mut self,
        path: &Path,
        file_index: u32,
        expected_chunks: u64,
        progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        let mut file_session = FileTransferSession::new(
            self.connection,
            self.config.clone(),
            self.transfer_id,
            file_index,
        );

        let receiver_checksum = file_session
            .receive_file(path, expected_chunks, None::<fn(u64)>, progress)
            .await?;

        let our_msg = FileChecksumMessage {
            transfer_id: self.transfer_id,
            file_index,
            checksum: receiver_checksum,
        };
        self.connection
            .send_message(&Message::FileChecksum(our_msg))
            .await?;

        let sender_checksum = match self.connection.recv_message().await? {
            Message::FileChecksum(peer_msg) => {
                if peer_msg.file_index != file_index {
                    return Err(Error::Protocol(format!(
                        "File index mismatch: expected {}, got {}",
                        file_index, peer_msg.file_index
                    )));
                }
                peer_msg.checksum
            }
            msg => {
                return Err(Error::Protocol(format!(
                    "Expected FileChecksum, got {:?}",
                    msg
                )))
            }
        };

        if sender_checksum != receiver_checksum {
            warn!(
                "File {} checksum mismatch: sender={:02x?}, receiver={:02x?}",
                file_index,
                &sender_checksum[..8],
                &receiver_checksum[..8]
            );
        }
        Ok(())
    }

    async fn scan_folder(&self, folder_path: &Path) -> Result<Vec<(PathBuf, FileMetadata)>> {
        let mut files = Vec::new();
        let base_path = folder_path.parent().unwrap_or(folder_path);
        Self::scan_folder_recursive(base_path, folder_path, &mut files).await?;
        Ok(files)
    }

    fn scan_folder_recursive<'b>(
        base_path: &'b Path,
        current_path: &'b Path,
        files: &'b mut Vec<(PathBuf, FileMetadata)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'b>> {
        Box::pin(async move {
            let mut entries = fs::read_dir(current_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let metadata = entry.metadata().await?;
                if metadata.is_file() {
                    let relative_path = path
                        .strip_prefix(base_path)
                        .map_err(|e| Error::Protocol(format!("Invalid path: {}", e)))?
                        .to_path_buf();
                    let size = metadata.len();
                    let modified = metadata
                        .modified()
                        .unwrap_or(SystemTime::UNIX_EPOCH)
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    files.push((
                        relative_path.clone(),
                        FileMetadata {
                            path: relative_path.to_string_lossy().to_string(),
                            size,
                            modified,
                            checksum: [0u8; 32],
                        },
                    ));
                    trace!("Found file: {} ({} bytes)", path.display(), size);
                } else if metadata.is_dir() {
                    Self::scan_folder_recursive(base_path, &path, files).await?;
                }
            }
            Ok(())
        })
    }
}

/// On-disk state for chunk-level resume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderTransferState {
    pub transfer_id: Uuid,
    pub folder_name: String,
    pub files: Vec<FileMetadata>,
    pub completed_files: Vec<usize>,
    pub current_file: Option<usize>,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub file_chunks: HashMap<usize, Vec<u64>>,
    pub chunk_size: u32,
}

impl FolderTransferState {
    pub fn new(transfer_id: Uuid, folder_name: String, files: Vec<FileMetadata>) -> Self {
        let total_bytes = files.iter().map(|f| f.size).sum();
        Self {
            transfer_id,
            folder_name,
            files,
            completed_files: Vec::new(),
            current_file: None,
            total_bytes,
            transferred_bytes: 0,
            file_chunks: HashMap::new(),
            chunk_size: 65536,
        }
    }

    pub fn mark_chunk_complete(&mut self, file_index: usize, chunk_index: u64) {
        self.file_chunks
            .entry(file_index)
            .or_default()
            .push(chunk_index);
    }

    pub fn get_completed_chunks(&self, file_index: usize) -> &[u64] {
        self.file_chunks
            .get(&file_index)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn mark_file_complete(&mut self, file_index: usize) {
        if !self.completed_files.contains(&file_index) {
            self.completed_files.push(file_index);
            if file_index < self.files.len() {
                self.transferred_bytes += self.files[file_index].size;
            }
        }
    }

    pub fn next_file(&self) -> Option<usize> {
        (0..self.files.len()).find(|i| !self.completed_files.contains(i))
    }

    pub fn is_complete(&self) -> bool {
        self.completed_files.len() == self.files.len()
    }

    pub fn progress_percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }

    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| Error::Protocol(format!("Failed to serialize state: {}", e)))?;
        fs::write(path, json).await?;
        Ok(())
    }

    pub async fn load_from_file(path: &Path) -> Result<Self> {
        let json = fs::read_to_string(path).await?;
        serde_json::from_str(&json)
            .map_err(|e| Error::Protocol(format!("Failed to deserialize state: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn folder_transfer_state_tracks_files() {
        let files = vec![
            FileMetadata {
                path: "a.txt".to_string(),
                size: 100,
                modified: 0,
                checksum: [0u8; 32],
            },
            FileMetadata {
                path: "b.txt".to_string(),
                size: 200,
                modified: 0,
                checksum: [0u8; 32],
            },
        ];
        let mut state = FolderTransferState::new(Uuid::new_v4(), "x".to_string(), files);
        assert_eq!(state.total_bytes, 300);
        assert_eq!(state.next_file(), Some(0));

        state.mark_file_complete(0);
        assert_eq!(state.transferred_bytes, 100);
        assert_eq!(state.next_file(), Some(1));

        state.mark_chunk_complete(1, 7);
        assert_eq!(state.get_completed_chunks(1), &[7u64]);

        state.mark_file_complete(1);
        assert!(state.is_complete());
        assert_eq!(state.progress_percentage(), 100.0);
    }
}
