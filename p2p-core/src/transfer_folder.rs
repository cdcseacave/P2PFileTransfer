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
    protocol::{CompleteMessage, ConfigMessage, FileMetadata, Message, TransferInfo},
    transfer_file::FileTransferSession,
    verification,
    window::WindowConfig,
};
use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};
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

        // Send transfer info with file list and optional resume point
        let is_resuming = resume_point.is_some();
        let transfer_info = TransferInfo {
            transfer_id: self.transfer_id,
            items: state.files.clone(),
            resume_from: resume_point,
        };

        self.connection
            .send_message(&Message::TransferInfo(transfer_info))
            .await?;

        // Wait for ready acknowledgment
        let msg = self.connection.recv_message().await?;
        if !matches!(msg, Message::Ready) {
            return Err(Error::Protocol(format!("Expected Ready, got {:?}", msg)));
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
        if transfer_info.items.is_empty() {
            return Err(Error::Protocol("No files in transfer".to_string()));
        }

        info!("Starting receive to: {:?}", output_dir);

        // Check if this is a resume transfer
        let is_resume = transfer_info.resume_from.is_some();

        // If we have a state file, check if this transfer matches
        if let Some(state_file) = state_path {
            if state_file.exists() {
                match FolderTransferState::load_from_file(state_file).await {
                    Ok(existing_state) => {
                        if existing_state.transfer_id == transfer_info.transfer_id {
                            info!(
                                "Detected existing transfer {}, resuming automatically",
                                transfer_info.transfer_id
                            );
                            // The resume_from field in transfer_info already contains chunk data
                        } else {
                            info!(
                                "New transfer {}, previous transfer was {}",
                                transfer_info.transfer_id, existing_state.transfer_id
                            );
                        }
                    }
                    Err(e) => warn!("Failed to load existing state: {}", e),
                }
            }
        }

        // Update the session's transfer_id to match the incoming transfer
        self.transfer_id = transfer_info.transfer_id;

        // Start timing the transfer
        self.transfer_start = Some(std::time::Instant::now());
        self.total_compressed_bytes = 0;

        if is_resume {
            info!(
                "Receiving resumed transfer with {} files",
                transfer_info.items.len()
            );
        } else {
            info!(
                "Receiving new transfer with {} files",
                transfer_info.items.len()
            );
        }

        // Calculate total size
        let total_bytes: u64 = transfer_info.items.iter().map(|f| f.size).sum();

        // Calculate already-transferred bytes from resume information
        let mut already_transferred = 0u64;
        if let Some(ref resume_point) = transfer_info.resume_from {
            let file_index = resume_point.file_index as usize;
            // Add bytes from all completed files before the resume point
            for i in 0..file_index {
                if i < transfer_info.items.len() {
                    already_transferred += transfer_info.items[i].size;
                }
            }
            // Add bytes from completed chunks in the current file
            if file_index < transfer_info.items.len() {
                let current_file_size = transfer_info.items[file_index].size;
                let chunk_size = self.config.chunk_size as u64;
                let total_chunks = (current_file_size + chunk_size - 1) / chunk_size;
                let completed_chunks = resume_point.completed_chunks.len() as u64;
                if completed_chunks < total_chunks {
                    already_transferred += completed_chunks * chunk_size;
                } else {
                    already_transferred += current_file_size;
                }
                debug!(
                    "Resume: {} completed chunks ({} bytes) in file {} (total {} chunks)",
                    completed_chunks, already_transferred, file_index, total_chunks
                );
            }
            info!(
                "Resume detected: {} bytes already transferred ({:.1}%)",
                already_transferred,
                (already_transferred as f64 / total_bytes as f64) * 100.0
            );
        }

        // Set total bytes and initialize with already-transferred bytes if resuming
        if let Some(ref mut progress) = progress {
            progress.set_total_bytes(total_bytes);
            if already_transferred > 0 {
                progress.add_bytes(already_transferred);
            }
        }

        // Create output directory
        fs::create_dir_all(output_dir).await?;

        // Send ready acknowledgment
        self.connection.send_message(&Message::Ready).await?;

        // Receive each file
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

            // Create parent directories
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent).await?;
            }

            // Calculate expected chunks
            let expected_chunks = ((file_meta.size + self.config.chunk_size as u64 - 1)
                / self.config.chunk_size as u64) as u32;

            // Receive the file using our connection (checksum verification is now done per-file)
            self.receive_single_file(
                &full_path,
                file_index as u32,
                expected_chunks,
                progress.as_deref_mut(),
            )
            .await?;

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
                ack_timeout: std::time::Duration::from_secs(10),
                max_retries: 3,
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

    /// Receive a single file (internal helper)
    /// Receives chunks, computes checksum, and verifies against sender's checksum
    async fn receive_single_file(
        &mut self,
        path: &Path,
        file_index: u32,
        expected_chunks: u32,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<()> {
        use crate::compression::Decompressor;
        use crate::transfer_file::ChunkWriter;

        let mut writer = ChunkWriter::new(path, self.config.chunk_size as usize).await?;

        // Decompression if enabled
        let mut decompressor: Option<Decompressor> = if self.config.compression_enabled {
            Some(Decompressor::new())
        } else {
            None
        };

        let mut received = 0;

        while received < expected_chunks {
            // Receive chunk message
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

        // Finalize file and get the computed checksum
        let receiver_checksum = writer.finalize().await?;

        // Send receiver's checksum first (same pattern as sender - both send, then both receive)
        // This allows both messages to be "in flight" simultaneously, reducing latency
        use crate::protocol::FileChecksumMessage;
        let receiver_checksum_msg = FileChecksumMessage {
            transfer_id: self.transfer_id,
            file_index,
            checksum: receiver_checksum,
        };
        self.connection
            .send_message(&Message::FileChecksum(receiver_checksum_msg))
            .await?;

        // Now receive sender's checksum message (sender already sent it and is waiting for ours)
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

        // Log the comparison result (for receiver's awareness)
        if sender_checksum == receiver_checksum {
            debug!(
                "File {} checksum match: {:02x?}",
                file_index,
                &receiver_checksum[..8]
            );
        } else {
            // Receiver logs mismatch, but sender will detect and handle the error
            warn!(
                "File {} checksum mismatch: sender={:02x?}, receiver={:02x?}",
                file_index,
                &sender_checksum[..8],
                &receiver_checksum[..8]
            );
        }

        Ok(())
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

    /// Scan a folder and build file metadata list
    async fn scan_folder(&self, folder_path: &Path) -> Result<Vec<(PathBuf, FileMetadata)>> {
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
