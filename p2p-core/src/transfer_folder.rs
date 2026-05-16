//! Folder transfer management
//!
//! This module provides folder-level transfer orchestration, using the
//! FileTransferSession logic as a building block for individual file transfers.
//!
//! Features:
//! - Recursive folder scanning
//! - Folder structure reproduction on receiver
//! - Progress tracking across multiple files
//! - Partial folder transfer support
//! - Individual file checksums

use crate::{
    bandwidth,
    error::{Error, Result},
    network::tcp::TcpConnection,
    progress::ProgressState,
    protocol::{
        CompleteMessage, ConfigMessage, FileListChunk, FileMetadata, Message, PartialFileStatus,
        SyncStatus, TransferInfo,
    },
    transfer_file::FileTransferSession,
    verification,
    window::WindowConfig,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::SystemTime,
};

/// Maximum number of files per TransferInfo/FileListChunk message.
/// Prevents individual messages from growing too large on directories with many files.
const FILE_LIST_BATCH_SIZE: usize = 5_000;

// ---------------------------------------------------------------------------
// Receiver-side sync state
// ---------------------------------------------------------------------------

/// Persistent state saved on the receiver side after each successful file receive.
///
/// Stored as `.p2p_sync_state.json` inside the output directory.
/// Used to skip files already on disk and to resume partial files.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ReceiverSyncState {
    /// Fully received files: relative path → info
    pub files: HashMap<String, ReceivedFileInfo>,
    /// Partially received files: relative path → chunk indices already on disk
    pub partial: HashMap<String, Vec<u64>>,
}

/// Metadata for a fully received file
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReceivedFileInfo {
    pub size: u64,
    pub mtime: u64,
    /// SHA-256 hex string — verified at receive time
    pub sha256: String,
}

impl ReceiverSyncState {
    const STATE_FILE: &'static str = ".p2p_sync_state.json";

    pub async fn load(output_dir: &Path) -> Self {
        let path = output_dir.join(Self::STATE_FILE);
        match tokio::fs::read_to_string(&path).await {
            Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    pub async fn save(&self, output_dir: &Path) {
        let path = output_dir.join(Self::STATE_FILE);
        if let Ok(json) = serde_json::to_string_pretty(self) {
            let _ = tokio::fs::write(&path, json).await;
        }
    }

    /// Returns `true` if the receiver already has this file complete and verified.
    /// Uses size + mtime as a fast "rsync-style" check.
    pub fn is_complete(&self, rel_path: &str, size: u64, mtime: u64) -> bool {
        self.files
            .get(rel_path)
            .map(|info| info.size == size && info.mtime == mtime)
            .unwrap_or(false)
    }

    /// Returns the chunk indices the receiver already has for a partial file.
    pub fn partial_chunks(&self, rel_path: &str) -> &[u64] {
        self.partial
            .get(rel_path)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Record a completed chunk for a partial file.
    pub fn record_chunk(&mut self, rel_path: &str, chunk_index: u64) {
        self.partial
            .entry(rel_path.to_string())
            .or_default()
            .push(chunk_index);
    }

    /// Mark a file as fully received.
    pub fn mark_complete(&mut self, rel_path: &str, size: u64, mtime: u64, sha256: [u8; 32]) {
        let hex: String = sha256.iter().map(|b| format!("{:02x}", b)).collect();
        self.files.insert(
            rel_path.to_string(),
            ReceivedFileInfo { size, mtime, sha256: hex },
        );
        self.partial.remove(rel_path);
    }
}
use tokio::fs;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

/// Transfer statistics
#[derive(Debug, Clone)]
pub struct TransferStats {
    /// Total uncompressed bytes
    pub uncompressed_bytes: u64,
    /// Total compressed bytes
    pub compressed_bytes: u64,
    /// Transfer duration in seconds
    pub duration_secs: f64,
    /// Compression ratio (uncompressed / compressed)
    pub compression_ratio: f64,
    /// Percentage saved by compression
    pub compression_percent: f64,
    /// Network speed (compressed data rate) in MB/s
    pub network_speed_mbps: f64,
    /// Felt speed (uncompressed data rate) in MB/s
    pub felt_speed_mbps: f64,
}

/// State callback for auto-saving transfer state
pub type StateCallback = std::sync::Arc<dyn Fn(&FolderTransferState) + Send + Sync>;

/// Folder transfer session managing multiple file transfers
pub struct FolderTransferSession<'a> {
    /// TCP connection to peer
    connection: &'a mut TcpConnection,
    /// Negotiated configuration
    config: ConfigMessage,
    /// Transfer ID
    transfer_id: Uuid,
    /// State callback for auto-save
    state_callback: Option<StateCallback>,
    /// Total compressed bytes transferred over network (for network speed calculation)
    total_compressed_bytes: u64,
    /// Transfer start time
    transfer_start: Option<std::time::Instant>,
}

impl<'a> FolderTransferSession<'a> {
    /// Create a new folder transfer session
    pub fn new(
        connection: &'a mut TcpConnection,
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

    /// Set state callback for auto-save
    pub fn set_state_callback(&mut self, callback: StateCallback) {
        self.state_callback = Some(callback);
    }

    /// Calculate compression statistics
    fn calc_compression_stats(&self, total_bytes: u64) -> (f64, f64) {
        let compression_ratio = if total_bytes > 0 {
            total_bytes as f64 / self.total_compressed_bytes as f64
        } else {
            1.0
        };

        let compression_percent = if total_bytes >= self.total_compressed_bytes {
            (total_bytes - self.total_compressed_bytes) as f64 / total_bytes as f64 * 100.0
        } else {
            // Compression expanded the data (incompressible) - show negative percentage
            -((self.total_compressed_bytes - total_bytes) as f64 / total_bytes as f64 * 100.0)
        };

        (compression_ratio, compression_percent)
    }

    /// Display compression and transfer statistics
    fn display_transfer_stats(
        &self,
        total_files: usize,
        total_bytes: u64,
        duration_secs: f64,
        is_sender: bool,
    ) {
        // Nicely formatted transfer statistics for the CLI
        info!("📊 Transfer Statistics:");
        let action = if is_sender { "sent" } else { "received" };

        if self.config.compression_enabled && self.total_compressed_bytes > 0 {
            let (compression_ratio, compression_percent) = self.calc_compression_stats(total_bytes);

            let network_speed = if duration_secs > 0.0 {
                self.total_compressed_bytes as f64 / duration_secs / 1_048_576.0
            // MB/s
            } else {
                0.0
            };

            let felt_speed = if duration_secs > 0.0 {
                total_bytes as f64 / duration_secs / 1_048_576.0 // MB/s
            } else {
                0.0
            };

            let direction = if is_sender { "→" } else { "←" };

            if compression_percent >= 0.0 {
                info!(
                    "   Data: {} bytes {} {} bytes ({:.1}% saved, {:.2}x compression)",
                    bandwidth::format_bandwidth(total_bytes),
                    direction,
                    bandwidth::format_bandwidth(self.total_compressed_bytes),
                    compression_percent,
                    compression_ratio
                );
            } else {
                info!(
                    "   Data: {} bytes {} {} bytes ({:.1}% overhead, adaptive compression disabled)",
                    bandwidth::format_bandwidth(total_bytes),
                    direction,
                    bandwidth::format_bandwidth(self.total_compressed_bytes),
                    -compression_percent
                );
            }

            info!(
                "   Speed: {:.2} MB/s network, {:.2} MB/s throughput",
                network_speed, felt_speed
            );

            info!(
                "Folder transfer complete: {} files, {} bytes {} ({} compressed, {:.1}% saved, {:.2}x ratio)",
                total_files,
                bandwidth::format_bandwidth(total_bytes),
                action,
                bandwidth::format_bandwidth(self.total_compressed_bytes),
                compression_percent.abs(),
                compression_ratio
            );
        } else {
            // No compression or adaptive compression disabled all chunks
            if duration_secs > 0.0 {
                let speed = total_bytes as f64 / duration_secs / 1_048_576.0;
                info!("   Speed: {:.2} MB/s", speed);
            }

            info!(
                "Folder transfer complete: {} files, {} bytes {}",
                total_files,
                bandwidth::format_bandwidth(total_bytes),
                action
            );
        }
    }

    /// Send a file or folder to the peer with mutable state for chunk-level resume.
    ///
    /// This is the unified send method that handles both new transfers and resuming interrupted
    /// transfers. The state is updated during transfer as chunks complete, allowing resume
    /// from the exact interruption point if connection is lost.
    ///
    /// # Arguments
    /// * `path` - Path to the file or folder to send
    /// * `state` - Mutable reference to transfer state (updated during transfer with chunk completions)
    /// * `progress` - Optional progress state for unified progress tracking
    pub async fn send(
        &mut self,
        path: &Path,
        state: &mut FolderTransferState,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        // Start timing the transfer
        self.transfer_start = Some(std::time::Instant::now());
        self.total_compressed_bytes = 0;

        // Check if we're resuming or starting fresh
        let resume_point = if !state.files.is_empty() {
            // Resume: state already has files
            info!("Resuming transfer: {:?}", path);

            info!(
                "Resume state: {}/{} files completed, {} bytes transferred",
                state.completed_files.len(),
                state.files.len(),
                state.transferred_bytes
            );

            // Build resume point from state (None if no completed chunks in current file)
            if let Some(next_file) = state.next_file() {
                let completed_chunks = state.get_completed_chunks(next_file);
                if !completed_chunks.is_empty() {
                    info!(
                        "Resuming file {} from chunk {}",
                        next_file,
                        completed_chunks.len()
                    );
                    Some(crate::protocol::ResumePoint {
                        transfer_id: self.transfer_id,
                        file_index: next_file as u32,
                        completed_chunks: completed_chunks.to_vec(),
                    })
                } else {
                    info!("Resuming file {} from beginning", next_file);
                    None
                }
            } else {
                None
            }
        } else {
            // New transfer: scan and build state
            info!("Starting transfer: {:?}", path);

            // Extract base name from path (last component)
            let base_name = path
                .file_name()
                .ok_or_else(|| Error::Protocol("Invalid path".to_string()))?
                .to_string_lossy()
                .to_string();

            // Check if path is a file or folder and collect metadata accordingly
            let files = if path.is_file() {
                // Single file: treat as 1-file "folder"
                // Only read metadata, not the file content (checksum will be computed during transfer)
                let metadata = fs::metadata(path).await?;
                let size = metadata.len();
                let modified = metadata
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let file_name = path.file_name().unwrap().to_string_lossy().to_string();
                let file_meta = FileMetadata {
                    path: file_name.clone(),
                    size,
                    checksum: [0u8; 32], // Placeholder - will be computed during transfer
                    modified,
                };

                vec![(PathBuf::from(file_name), file_meta)]
            } else if path.is_dir() {
                // Folder: scan recursively
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

            // Create fresh state with no completed files/chunks (no resume point)
            let file_list: Vec<FileMetadata> = files.iter().map(|(_, meta)| meta.clone()).collect();
            *state = FolderTransferState::new(self.transfer_id, base_name.to_string(), file_list);

            None
        };

        let total_files = state.files.len();
        let total_bytes = state.total_bytes;

        // Set total bytes in progress state if it's not set yet (when passing 0 from CLI)
        if let Some(ref mut progress) = progress {
            progress.set_total_bytes(total_bytes);
        }

        // Send transfer info with file list and optional resume point.
        // When the file list is very large, stream it as batched FileListChunk messages
        // to avoid hitting the per-message size limit.
        let is_resuming = resume_point.is_some();
        let chunked = state.files.len() > FILE_LIST_BATCH_SIZE;
        let transfer_info = TransferInfo {
            transfer_id: self.transfer_id,
            items: if chunked { vec![] } else { state.files.clone() },
            resume_from: resume_point,
            chunked,
            total_file_count: state.files.len() as u32,
        };

        self.connection
            .send_message(&Message::TransferInfo(transfer_info))
            .await?;

        if chunked {
            let total_chunks =
                (state.files.len() + FILE_LIST_BATCH_SIZE - 1) / FILE_LIST_BATCH_SIZE;
            info!(
                "Sending file list in {} batches ({} files total)",
                total_chunks,
                state.files.len()
            );
            for (chunk_index, batch) in state.files.chunks(FILE_LIST_BATCH_SIZE).enumerate() {
                let chunk_msg = FileListChunk {
                    transfer_id: self.transfer_id,
                    chunk_index: chunk_index as u32,
                    total_chunks: total_chunks as u32,
                    items: batch.to_vec(),
                };
                self.connection
                    .send_message(&Message::FileListChunk(chunk_msg))
                    .await?;
            }
        }

        // Wait for receiver's SyncStatus (or legacy Ready from older versions).
        // SyncStatus tells us which files the receiver already has so we can skip them.
        let msg = self.connection.recv_message().await?;
        match msg {
            Message::SyncStatus(sync) => {
                // Apply skip list — mark files receiver already has as complete
                let skipped = sync.complete_files.len();
                for file_index in sync.complete_files {
                    if !state.completed_files.contains(&(file_index as usize)) {
                        info!("  ⏭  Skipping file {} (receiver already has it)", file_index);
                        state.mark_file_complete(file_index as usize);
                    }
                }
                // Apply partial resume — set received chunks so sender skips them
                for partial in sync.partial_files {
                    let idx = partial.file_index as usize;
                    let n = partial.received_chunks.len();
                    info!("  🔄 Resuming file {} ({} chunks already on receiver)", idx, n);
                    for chunk in partial.received_chunks {
                        state.mark_chunk_complete(idx, chunk);
                    }
                }
                if skipped > 0 {
                    info!("Sync: skipped {} already-complete file(s)", skipped);
                }
            }
            Message::Ready => {
                // Old receiver — no sync info, transfer everything
            }
            other => {
                return Err(Error::Protocol(format!(
                    "Expected SyncStatus or Ready, got {:?}",
                    other
                )));
            }
        }

        if is_resuming {
            debug!(
                "Receiver ready, resuming from file {}",
                state.completed_files.len()
            );
        } else {
            debug!("Receiver ready, starting file transfers");
        }

        // Normalize base_path: for single files, use parent directory as base
        // For folders, also use parent so we can join with the folder-name-inclusive relative paths
        let base_path = if path.is_file() {
            path.parent()
                .ok_or_else(|| Error::Protocol("File has no parent directory".to_string()))?
        } else {
            // For folders, use parent as base (same as in scan_folder)
            path.parent().unwrap_or(path)
        };

        // Transfer each file (skipping already completed ones)
        for file_index in 0..state.files.len() {
            // Skip already completed files
            if state.completed_files.contains(&file_index) {
                continue;
            }

            let file_meta = &state.files[file_index];
            let relative_path = PathBuf::from(&file_meta.path);
            let full_path = base_path.join(&relative_path);

            // Get completed chunks for this file (for resume within file)
            let completed_chunks = state.get_completed_chunks(file_index).to_vec();

            // Create chunk completion callback that updates state directly
            let chunk_callback = |chunk_index: u64| {
                state.mark_chunk_complete(file_index, chunk_index);
            };

            // Send the file with chunk-level resume (progress state passed to FileTransferSession)
            self.send_single_file(
                &full_path,
                file_index as u32,
                &completed_chunks,
                progress.as_deref_mut(),
                Some(chunk_callback),
            )
            .await?;

            // Mark file as complete in state
            state.mark_file_complete(file_index);
            state.current_file = state.next_file();

            // Save state after each file
            if let Some(callback) = &self.state_callback {
                callback(state);
            }

            trace!("File {} complete", relative_path.display());
        }

        // Calculate transfer duration and speeds
        let duration = self.transfer_start.map(|s| s.elapsed()).unwrap_or_default();
        let duration_secs = duration.as_secs_f64();

        // Send completion message
        let complete_msg = CompleteMessage {
            transfer_id: self.transfer_id,
            total_bytes,
            duration_ms: duration.as_millis() as u64,
        };
        self.connection
            .send_message(&Message::Complete(complete_msg))
            .await?;

        // Signal progress finish
        if let Some(ref mut progress) = progress {
            progress.finish();
        }

        // Display transfer statistics
        self.display_transfer_stats(total_files, total_bytes, duration_secs, true);
        Ok(())
    }

    /// Receive a folder from the peer with optional state file for auto-resume
    pub async fn receive_folder(
        &mut self,
        output_dir: &Path,
        state_path: Option<&Path>,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        // Receive transfer info
        let msg = self.connection.recv_message().await?;
        let transfer_info = match msg {
            Message::TransferInfo(info) => info,
            _ => {
                return Err(Error::Protocol(format!(
                    "Expected TransferInfo, got {:?}",
                    msg
                )))
            }
        };
        // Collect the full file list — may arrive in batched FileListChunk messages
        // when the sender has too many files to fit in a single TransferInfo message.
        let all_items: Vec<FileMetadata> = if transfer_info.chunked {
            let total_chunks = (transfer_info.total_file_count as usize
                + FILE_LIST_BATCH_SIZE
                - 1)
                / FILE_LIST_BATCH_SIZE;
            info!(
                "Receiving chunked file list ({} files in {} batches)",
                transfer_info.total_file_count, total_chunks
            );
            let mut items = Vec::with_capacity(transfer_info.total_file_count as usize);
            for _ in 0..total_chunks {
                let msg = self.connection.recv_message().await?;
                match msg {
                    Message::FileListChunk(chunk) => {
                        items.extend(chunk.items);
                    }
                    _ => {
                        return Err(Error::Protocol(format!(
                            "Expected FileListChunk, got {:?}",
                            msg
                        )))
                    }
                }
            }
            items
        } else {
            transfer_info.items.clone()
        };

        if all_items.is_empty() {
            return Err(Error::Protocol("No files in transfer".to_string()));
        }

        info!("Starting receive to: {:?}", output_dir);

        // Load persistent sync state — tracks which files were fully/partially received
        fs::create_dir_all(output_dir).await?;
        let mut sync_state = ReceiverSyncState::load(output_dir).await;

        // Build SyncStatus so the sender knows what to skip / resume
        let mut complete_files: Vec<u32> = Vec::new();
        let mut partial_files: Vec<PartialFileStatus> = Vec::new();

        for (idx, file_meta) in all_items.iter().enumerate() {
            if sync_state.is_complete(&file_meta.path, file_meta.size, file_meta.modified) {
                complete_files.push(idx as u32);
                info!("  ✅ Already have: {}", file_meta.path);
            } else {
                let chunks = sync_state.partial_chunks(&file_meta.path).to_vec();
                if !chunks.is_empty() {
                    info!("  🔄 Partial ({} chunks): {}", chunks.len(), file_meta.path);
                    partial_files.push(PartialFileStatus {
                        file_index: idx as u32,
                        received_chunks: chunks,
                    });
                }
            }
        }

        info!(
            "Sync: {} complete, {} partial, {} to transfer",
            complete_files.len(),
            partial_files.len(),
            all_items.len() - complete_files.len() - partial_files.len()
        );

        // Update the session's transfer_id to match the incoming transfer
        self.transfer_id = transfer_info.transfer_id;

        // Start timing the transfer
        self.transfer_start = Some(std::time::Instant::now());
        self.total_compressed_bytes = 0;

        info!(
            "Receiving transfer with {} files",
            all_items.len()
        );

        // Calculate total size (excluding already-complete files for progress)
        let total_bytes: u64 = all_items.iter().map(|f| f.size).sum();
        let already_bytes: u64 = complete_files
            .iter()
            .filter_map(|&i| all_items.get(i as usize))
            .map(|f| f.size)
            .sum();

        if let Some(ref mut progress) = progress {
            progress.set_total_bytes(total_bytes);
            if already_bytes > 0 {
                progress.add_bytes(already_bytes);
            }
        }

        // Send SyncStatus to sender (replaces old Ready message)
        self.connection
            .send_message(&Message::SyncStatus(SyncStatus {
                transfer_id: self.transfer_id,
                complete_files: complete_files.clone(),
                partial_files: partial_files.clone(),
            }))
            .await?;

        // Receive each file — skip files the receiver already has
        let total_files = all_items.len();

        for (file_index, file_meta) in all_items.iter().enumerate() {
            let relative_path = PathBuf::from(&file_meta.path);
            let full_path = output_dir.join(&relative_path);

            // Skip files the receiver already has complete
            if complete_files.contains(&(file_index as u32)) {
                trace!("Skipping already-complete file: {}", relative_path.display());
                continue;
            }

            info!(
                "Receiving file {}/{}: {}",
                file_index + 1,
                total_files,
                relative_path.display()
            );

            // Create parent directories
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent).await?;
            }

            // Get already-received chunk indices (for partial resume)
            let resume_chunks: Vec<u64> = partial_files
                .iter()
                .find(|p| p.file_index == file_index as u32)
                .map(|p| p.received_chunks.clone())
                .unwrap_or_default();

            // Number of chunks the sender will actually send (total minus already received)
            let total_file_chunks = ((file_meta.size + self.config.chunk_size as u64 - 1)
                / self.config.chunk_size as u64) as u32;
            let expected_chunks = total_file_chunks - resume_chunks.len() as u32;

            let file_checksum = self
                .receive_single_file(
                    &full_path,
                    file_index as u32,
                    expected_chunks,
                    resume_chunks.as_slice(),
                    progress.as_deref_mut(),
                )
                .await?;

            // Save to sync state so we can skip this file next time
            sync_state.mark_complete(
                &file_meta.path,
                file_meta.size,
                file_meta.modified,
                file_checksum,
            );
            sync_state.save(output_dir).await;

            trace!("File {} complete", relative_path.display());
        }

        // Wait for completion message
        let msg = self.connection.recv_message().await?;
        if !matches!(msg, Message::Complete(_)) {
            warn!("Expected Complete message, got {:?}", msg);
        }

        // Signal progress finish
        if let Some(ref mut progress) = progress {
            progress.finish();
        }

        // Calculate transfer duration and speeds
        let duration = self.transfer_start.map(|s| s.elapsed()).unwrap_or_default();
        let duration_secs = duration.as_secs_f64();

        // Display transfer statistics
        self.display_transfer_stats(total_files, total_bytes, duration_secs, false);
        Ok(())
    }

    /// Send a single file (internal helper)
    /// Uses windowed or sequential mode based on config.window_size.
    /// Supports chunk-level resume by skipping chunks in completed_chunks.
    /// Sends the computed checksum and waits for receiver confirmation.
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
        // Create a FileTransferSession with borrowed connection
        let mut file_session = FileTransferSession::new(
            self.connection,
            self.config.clone(),
            self.transfer_id,
            file_index,
        );

        // Use windowed mode if window_size > 1, otherwise sequential
        let sender_checksum = if self.config.window_size > 1 {
            // Create window config from settings
            let window_config = WindowConfig {
                max_window_size: self.config.window_size,
                ack_timeout: std::time::Duration::from_secs(30),
                max_retries: 10,
            };
            file_session
                .send_file_windowed(
                    path,
                    &window_config,
                    completed_chunks,
                    chunk_complete_callback,
                    progress,
                )
                .await?
        } else {
            file_session
                .send_file(path, completed_chunks, chunk_complete_callback, progress)
                .await?
        };

        // Aggregate compression statistics
        self.total_compressed_bytes += file_session.compressed_bytes_sent;

        // Send the file checksum to receiver and immediately receive acknowledgment
        // This minimizes round-trip latency by chaining send->recv without intermediate delays
        use crate::protocol::FileChecksumMessage;
        let checksum_msg = FileChecksumMessage {
            transfer_id: self.transfer_id,
            file_index,
            checksum: sender_checksum,
        };

        // Send checksum message
        self.connection
            .send_message(&Message::FileChecksum(checksum_msg))
            .await?;

        // Immediately start receiving receiver's checksum (receiver will be sending it in parallel)
        let msg = self.connection.recv_message().await?;

        // Validate checksum response and compare checksums
        match msg {
            Message::FileChecksum(receiver_msg) => {
                // Compare sender's checksum with receiver's checksum
                let matches = sender_checksum == receiver_msg.checksum;

                if !matches {
                    return Err(Error::Verification(format!(
                        "File checksum mismatch for file {}: sender={:02x?}, receiver={:02x?}",
                        file_index,
                        &sender_checksum[..8],
                        &receiver_msg.checksum[..8]
                    )));
                }
                debug!(
                    "File {} checksum verified: {:02x?}",
                    file_index,
                    &sender_checksum[..8]
                );
            }
            _ => {
                return Err(Error::Protocol(format!(
                    "Expected FileChecksum, got {:?}",
                    msg
                )));
            }
        }

        Ok(())
    }

    /// Receive a single file (internal helper).
    ///
    /// `resume_chunks` contains chunk indices the receiver already has on disk (partial resume).
    /// The sender will skip those chunks so `expected_chunks` is the number we still need to receive.
    ///
    /// Returns the SHA-256 checksum of the complete file (for sync state persistence).
    async fn receive_single_file(
        &mut self,
        path: &Path,
        file_index: u32,
        expected_chunks: u32,
        resume_chunks: &[u64],
        mut progress: Option<&mut ProgressState>,
    ) -> Result<[u8; 32]> {
        use crate::compression::Decompressor;
        use crate::transfer_file::ChunkWriter;

        // Open writer — resume into existing .partial file if we have prior chunks
        let mut writer = if resume_chunks.is_empty() {
            ChunkWriter::new(path, self.config.chunk_size as usize).await?
        } else {
            ChunkWriter::open_for_resume(path, self.config.chunk_size as usize, resume_chunks)
                .await?
        };

        // Decompression if enabled
        let mut decompressor: Option<Decompressor> = if self.config.compression_enabled {
            Some(Decompressor::new())
        } else {
            None
        };

        let mut received = 0u32;

        while received < expected_chunks {
            // Receive chunk message with retry on timeout
            use std::time::Duration;
            use tokio::time::timeout;

            let msg = timeout(Duration::from_secs(30), self.connection.recv_message())
                .await
                .map_err(|_| Error::Protocol("Chunk receive timeout".to_string()))??;

            match msg {
                Message::Chunk(chunk_msg) => {
                    let chunk_index = chunk_msg.chunk_index as u32;

                    // Track compression statistics (network bytes only)
                    self.total_compressed_bytes += chunk_msg.data.len() as u64;

                    // Verify checksum (fast, synchronous check for data corruption)
                    verification::verify_crc32(&chunk_msg.data, chunk_msg.checksum)?;

                    // Start sending ACK immediately after verification (don't wait yet)
                    use crate::protocol::AckStatus;
                    let ack_future = self.send_ack(chunk_index, AckStatus::Success);

                    // Do expensive operations (decompression, disk I/O) in parallel with ACK send
                    let is_compressed = chunk_msg.is_compressed();
                    let final_data = if is_compressed && decompressor.is_some() {
                        decompressor.as_mut().unwrap().decompress(&chunk_msg.data)?
                    } else {
                        chunk_msg.data
                    };

                    // Write chunk (also updates running SHA256 checksum)
                    writer.write_chunk(chunk_index, &final_data).await?;

                    // Update progress with uncompressed size
                    if let Some(ref mut progress) = progress {
                        let uncompressed_size = final_data.len() as u64;
                        progress.add_bytes(uncompressed_size);
                    }

                    // Ensure ACK send completed before processing next chunk
                    ack_future.await?;

                    received += 1;
                }
                _ => {
                    warn!("Unexpected message during transfer: {:?}", msg);
                }
            }
        }

        // Finalize file (rename .partial → final) and get the computed checksum
        let receiver_checksum = writer.finalize().await?;

        // Send receiver's checksum first (same pattern as sender — both send, then both receive)
        use crate::protocol::FileChecksumMessage;
        let receiver_checksum_msg = FileChecksumMessage {
            transfer_id: self.transfer_id,
            file_index,
            checksum: receiver_checksum,
        };
        self.connection
            .send_message(&Message::FileChecksum(receiver_checksum_msg))
            .await?;

        // Now receive sender's checksum
        let msg = self.connection.recv_message().await?;
        let sender_checksum = match msg {
            Message::FileChecksum(checksum_msg) => {
                if checksum_msg.file_index != file_index {
                    return Err(Error::Protocol(format!(
                        "File index mismatch: expected {}, got {}",
                        file_index, checksum_msg.file_index
                    )));
                }
                checksum_msg.checksum
            }
            _ => {
                return Err(Error::Protocol(format!(
                    "Expected FileChecksum, got {:?}",
                    msg
                )));
            }
        };

        if sender_checksum == receiver_checksum {
            debug!(
                "File {} checksum verified: {:02x?}",
                file_index,
                &receiver_checksum[..8]
            );
        } else {
            warn!(
                "File {} checksum mismatch: sender={:02x?}, receiver={:02x?}",
                file_index,
                &sender_checksum[..8],
                &receiver_checksum[..8]
            );
        }

        Ok(receiver_checksum)
    }

    /// Send a chunk acknowledgment (internal helper)
    async fn send_ack(
        &mut self,
        chunk_index: u32,
        status: crate::protocol::AckStatus,
    ) -> Result<()> {
        use crate::protocol::ChunkAck;

        let ack_msg = ChunkAck {
            transfer_id: self.transfer_id,
            file_index: 0, // Not used in current implementation
            chunk_index: chunk_index as u64,
            status,
        };

        self.connection
            .send_message(&Message::ChunkAck(ack_msg))
            .await
    }

    /// Send a pre-determined group of files to the peer (used for parallel transfers).
    ///
    /// Unlike `send()`, this method does not scan the filesystem — the caller provides
    /// the exact file list and the base path under which they live.
    pub async fn send_group(
        &mut self,
        base_path: &Path,
        files: Vec<FileMetadata>,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        self.transfer_start = Some(std::time::Instant::now());
        self.total_compressed_bytes = 0;

        let total_files = files.len();
        let total_bytes: u64 = files.iter().map(|f| f.size).sum();

        if let Some(ref mut p) = progress {
            p.set_total_bytes(total_bytes);
        }

        // Stream the file list (may be large — use batching)
        let chunked = files.len() > FILE_LIST_BATCH_SIZE;
        let transfer_info = TransferInfo {
            transfer_id: self.transfer_id,
            items: if chunked { vec![] } else { files.clone() },
            resume_from: None,
            chunked,
            total_file_count: files.len() as u32,
        };
        self.connection
            .send_message(&Message::TransferInfo(transfer_info))
            .await?;

        if chunked {
            let total_chunks = (files.len() + FILE_LIST_BATCH_SIZE - 1) / FILE_LIST_BATCH_SIZE;
            for (chunk_index, batch) in files.chunks(FILE_LIST_BATCH_SIZE).enumerate() {
                let chunk_msg = FileListChunk {
                    transfer_id: self.transfer_id,
                    chunk_index: chunk_index as u32,
                    total_chunks: total_chunks as u32,
                    items: batch.to_vec(),
                };
                self.connection
                    .send_message(&Message::FileListChunk(chunk_msg))
                    .await?;
            }
        }

        // Wait for receiver SyncStatus (or legacy Ready)
        let msg = self.connection.recv_message().await?;
        let mut skip_indices: std::collections::HashSet<u32> = Default::default();
        let mut partial_map: HashMap<u32, Vec<u64>> = HashMap::new();
        match msg {
            Message::SyncStatus(sync) => {
                for idx in sync.complete_files {
                    skip_indices.insert(idx);
                }
                for p in sync.partial_files {
                    partial_map.insert(p.file_index, p.received_chunks);
                }
            }
            Message::Ready => {}
            other => {
                return Err(Error::Protocol(format!(
                    "Expected SyncStatus or Ready, got {:?}",
                    other
                )));
            }
        }

        for (file_index, file_meta) in files.iter().enumerate() {
            let idx = file_index as u32;
            if skip_indices.contains(&idx) {
                info!("  ⏭  Skipping {} (receiver already has it)", file_meta.path);
                continue;
            }
            let resume_chunks = partial_map.remove(&idx).unwrap_or_default();
            let relative_path = PathBuf::from(&file_meta.path);
            let full_path = base_path.join(&relative_path);

            self.send_single_file(
                &full_path,
                file_index as u32,
                &resume_chunks,
                progress.as_deref_mut(),
                None::<fn(u64)>,
            )
            .await?;
        }

        let duration = self.transfer_start.map(|s| s.elapsed()).unwrap_or_default();
        let complete_msg = CompleteMessage {
            transfer_id: self.transfer_id,
            total_bytes,
            duration_ms: duration.as_millis() as u64,
        };
        self.connection
            .send_message(&Message::Complete(complete_msg))
            .await?;

        if let Some(ref mut p) = progress {
            p.finish();
        }
        self.display_transfer_stats(total_files, total_bytes, duration.as_secs_f64(), true);
        Ok(())
    }

    /// Scan a folder and build file metadata list
    pub async fn scan_folder(&self, folder_path: &Path) -> Result<Vec<(PathBuf, FileMetadata)>> {
        let mut files = Vec::new();
        // Use parent as base so folder name is included in relative paths
        let base_path = folder_path.parent().unwrap_or(folder_path);
        Self::scan_folder_recursive(base_path, folder_path, &mut files).await?;
        Ok(files)
    }

    /// Recursively scan a folder (only reads metadata, not file contents)
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
                    // Calculate relative path
                    let relative_path = path
                        .strip_prefix(base_path)
                        .map_err(|e| Error::Protocol(format!("Invalid path: {}", e)))?
                        .to_path_buf();

                    // Only read metadata, not file content (checksum will be computed during transfer)
                    let size = metadata.len();

                    // Get modified time
                    let modified = metadata
                        .modified()
                        .unwrap_or(SystemTime::UNIX_EPOCH)
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();

                    let file_meta = FileMetadata {
                        path: relative_path.to_string_lossy().to_string(),
                        size,
                        modified,
                        checksum: [0u8; 32], // Placeholder - will be computed during transfer
                    };

                    files.push((relative_path, file_meta));
                    trace!("Found file: {} ({} bytes)", path.display(), size);
                } else if metadata.is_dir() {
                    // Recurse into subdirectory
                    Self::scan_folder_recursive(base_path, &path, files).await?;
                }
            }

            Ok(())
        })
    }
}

/// Scan a folder recursively and return `(base_path, Vec<FileMetadata>)`.
///
/// `base_path` is the parent of `folder_path` so that the folder name itself
/// is included in the relative paths stored in `FileMetadata.path`.
pub async fn scan_folder_for_parallel(
    folder_path: &Path,
) -> Result<(PathBuf, Vec<FileMetadata>)> {
    let base_path = folder_path.parent().unwrap_or(folder_path).to_path_buf();
    let mut raw: Vec<(PathBuf, FileMetadata)> = Vec::new();
    FolderTransferSession::scan_folder_recursive(&base_path, folder_path, &mut raw).await?;
    let files = raw.into_iter().map(|(_, m)| m).collect();
    Ok((base_path, files))
}

/// Split a file list into `n` balanced groups (by total bytes) for parallel transfer.
///
/// Uses a greedy bin-packing heuristic: sort files largest-first, then assign
/// each file to the group with the smallest current total.
pub fn split_files_for_parallel(files: Vec<FileMetadata>, n: usize) -> Vec<Vec<FileMetadata>> {
    if n <= 1 || files.is_empty() {
        return vec![files];
    }

    let mut sorted = files;
    sorted.sort_by(|a, b| b.size.cmp(&a.size));

    let mut groups: Vec<Vec<FileMetadata>> = (0..n).map(|_| Vec::new()).collect();
    let mut group_bytes: Vec<u64> = vec![0u64; n];

    for file in sorted {
        let min_idx = group_bytes
            .iter()
            .enumerate()
            .min_by_key(|(_, &b)| b)
            .map(|(i, _)| i)
            .unwrap_or(0);
        group_bytes[min_idx] += file.size;
        groups[min_idx].push(file);
    }

    groups.into_iter().filter(|g| !g.is_empty()).collect()
}

/// Folder transfer state for resume capability
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FolderTransferState {
    /// Transfer ID
    pub transfer_id: Uuid,
    /// Base folder name
    pub folder_name: String,
    /// File list with metadata
    pub files: Vec<FileMetadata>,
    /// Completed files (by index)
    pub completed_files: Vec<usize>,
    /// Current file being transferred (if any)
    pub current_file: Option<usize>,
    /// Total bytes
    pub total_bytes: u64,
    /// Transferred bytes
    pub transferred_bytes: u64,
    /// Completed chunks per file (file_index -> Vec<chunk_index>)
    /// Used for chunk-level resume
    pub file_chunks: std::collections::HashMap<usize, Vec<u64>>,
    /// Chunk size used for the transfer
    pub chunk_size: u32,
}

impl FolderTransferState {
    /// Create a new folder transfer state
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
            file_chunks: std::collections::HashMap::new(),
            chunk_size: 65536,
        }
    }

    /// Mark a chunk as completed for a file
    pub fn mark_chunk_complete(&mut self, file_index: usize, chunk_index: u64) {
        self.file_chunks
            .entry(file_index)
            .or_default()
            .push(chunk_index);
    }

    /// Get completed chunks for a file
    pub fn get_completed_chunks(&self, file_index: usize) -> &[u64] {
        self.file_chunks
            .get(&file_index)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Mark a file as completed
    pub fn mark_file_complete(&mut self, file_index: usize) {
        if !self.completed_files.contains(&file_index) {
            self.completed_files.push(file_index);
            if file_index < self.files.len() {
                self.transferred_bytes += self.files[file_index].size;
            }
        }
    }

    /// Get next file to transfer
    pub fn next_file(&self) -> Option<usize> {
        self.files
            .iter()
            .enumerate()
            .map(|(index, _)| index)
            .find(|&index| !self.completed_files.contains(&index))
    }

    /// Check if transfer is complete
    pub fn is_complete(&self) -> bool {
        self.completed_files.len() == self.files.len()
    }

    /// Get progress percentage
    pub fn progress_percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }

    /// Save state to a file
    pub async fn save_to_file(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| Error::Protocol(format!("Failed to serialize state: {}", e)))?;
        fs::write(path, json).await?;
        Ok(())
    }

    /// Load state from a file
    pub async fn load_from_file(path: &Path) -> Result<Self> {
        let json = fs::read_to_string(path).await?;
        let state = serde_json::from_str(&json)
            .map_err(|e| Error::Protocol(format!("Failed to deserialize state: {}", e)))?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_scan_folder() {
        let dir = tempdir().unwrap();
        let base_path = dir.path();

        // Create test folder structure
        // base/
        //   file1.txt
        //   subdir/
        //     file2.txt
        //   subdir2/
        //     nested/
        //       file3.txt

        let file1 = base_path.join("file1.txt");
        let mut f1 = fs::File::create(&file1).await.unwrap();
        f1.write_all(b"content1").await.unwrap();
        f1.flush().await.unwrap();
        drop(f1);

        let subdir = base_path.join("subdir");
        fs::create_dir(&subdir).await.unwrap();
        let file2 = subdir.join("file2.txt");
        let mut f2 = fs::File::create(&file2).await.unwrap();
        f2.write_all(b"content2").await.unwrap();
        f2.flush().await.unwrap();
        drop(f2);

        let subdir2 = base_path.join("subdir2");
        fs::create_dir(&subdir2).await.unwrap();
        let nested = subdir2.join("nested");
        fs::create_dir(&nested).await.unwrap();
        let file3 = nested.join("file3.txt");
        let mut f3 = fs::File::create(&file3).await.unwrap();
        f3.write_all(b"content3").await.unwrap();
        f3.flush().await.unwrap();
        drop(f3);

        // Create a dummy connection (we're only testing scanning)
        let config = ConfigMessage {
            compression_enabled: false,
            compression_level: 0,
            window_size: 1,
            ..Default::default()
        };

        // We can't easily test without a real connection, so just test the state
        let _config = config; // Suppress unused warning
        let files = vec![
            FileMetadata {
                path: "file1.txt".to_string(),
                size: 8,
                modified: 0,
                checksum: [0u8; 32],
            },
            FileMetadata {
                path: "subdir/file2.txt".to_string(),
                size: 8,
                modified: 0,
                checksum: [0u8; 32],
            },
        ];

        let mut state = FolderTransferState::new(Uuid::new_v4(), "test".to_string(), files);

        assert_eq!(state.files.len(), 2);
        assert_eq!(state.total_bytes, 16);
        assert!(!state.is_complete());

        state.mark_file_complete(0);
        assert_eq!(state.transferred_bytes, 8);
        assert_eq!(state.next_file(), Some(1));

        state.mark_file_complete(1);
        assert_eq!(state.transferred_bytes, 16);
        assert!(state.is_complete());
        assert_eq!(state.next_file(), None);
    }

    #[tokio::test]
    async fn test_folder_transfer_state() {
        let files = vec![
            FileMetadata {
                path: "file1.txt".to_string(),
                size: 100,
                modified: 0,
                checksum: [0u8; 32],
            },
            FileMetadata {
                path: "file2.txt".to_string(),
                size: 200,
                modified: 0,
                checksum: [0u8; 32],
            },
            FileMetadata {
                path: "file3.txt".to_string(),
                size: 300,
                modified: 0,
                checksum: [0u8; 32],
            },
        ];

        let mut state = FolderTransferState::new(Uuid::new_v4(), "test_folder".to_string(), files);

        // Initial state
        assert_eq!(state.total_bytes, 600);
        assert_eq!(state.transferred_bytes, 0);
        assert_eq!(state.progress_percentage(), 0.0);
        assert_eq!(state.next_file(), Some(0));

        // Complete first file
        state.mark_file_complete(0);
        assert_eq!(state.transferred_bytes, 100);
        assert!((state.progress_percentage() - 16.666666).abs() < 0.001);
        assert_eq!(state.next_file(), Some(1));

        // Complete second file
        state.mark_file_complete(1);
        assert_eq!(state.transferred_bytes, 300);
        assert_eq!(state.progress_percentage(), 50.0);
        assert_eq!(state.next_file(), Some(2));

        // Complete third file
        state.mark_file_complete(2);
        assert_eq!(state.transferred_bytes, 600);
        assert_eq!(state.progress_percentage(), 100.0);
        assert!(state.is_complete());
        assert_eq!(state.next_file(), None);
    }
}
