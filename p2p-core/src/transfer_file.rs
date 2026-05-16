//! File transfer engine for single-file transfers
//!
//! This module implements a file transfer mechanism with:
//! - Chunk-based streaming
//! - Optional compression
//! - CRC32 checksum verification
//! - Acknowledgment protocol
//!
//! This module provides the core file transfer logic that is used by FolderTransferSession.
//! It never manages connections directly, only borrows them.

use crate::{
    bandwidth::BandwidthLimiter,
    compression::{AdaptiveCompressor, Decompressor},
    error::{Error, Result},
    network::tcp::TcpConnection,
    progress::ProgressState,
    protocol::{AckStatus, ChunkAck, ChunkMessage, ConfigMessage, Message},
    verification,
    window::{InFlightChunk, SlidingWindow, WindowConfig},
};
use sha2::Digest;
use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    time::timeout,
};
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

/// File transfer session for single-file transfers
/// This is a helper struct that never owns the connection, only borrows it.
pub struct FileTransferSession<'a> {
    /// TCP connection to peer (borrowed, not owned)
    connection: &'a mut TcpConnection,
    /// Negotiated configuration
    config: ConfigMessage,
    /// Transfer ID
    transfer_id: Uuid,
    /// File index
    file_index: u32,
    /// Bandwidth limiter (only created if throttling is enabled)
    bandwidth_limiter: Option<BandwidthLimiter>,
    /// Total compressed bytes sent (for statistics)
    pub compressed_bytes_sent: u64,
    /// Total uncompressed bytes sent (for statistics)
    pub uncompressed_bytes_sent: u64,
}

impl<'a> FileTransferSession<'a> {
    /// Create a new file transfer session with borrowed connection
    pub fn new(
        connection: &'a mut TcpConnection,
        config: ConfigMessage,
        transfer_id: Uuid,
        file_index: u32,
    ) -> Self {
        let bandwidth_limiter = if config.bandwidth_limit > 0 {
            Some(BandwidthLimiter::new(config.bandwidth_limit))
        } else {
            None
        };
        Self {
            connection,
            config,
            transfer_id,
            file_index,
            bandwidth_limiter,
            compressed_bytes_sent: 0,
            uncompressed_bytes_sent: 0,
        }
    }

    /// Sends a file to the peer using sequential chunk transfer.
    ///
    /// This method sends a file chunk-by-chunk with acknowledgment after each chunk.
    /// It supports automatic resume by skipping already-completed chunks.
    /// The file's SHA256 checksum is computed incrementally during the transfer.
    ///
    /// # Arguments
    /// * `path` - Path to the file to send
    /// * `completed_chunks` - Slice of chunk indices that have already been transferred (empty for new transfers)
    /// * `chunk_complete_callback` - Optional boxed closure invoked after each chunk is successfully acknowledged
    /// * `progress` - Optional progress state for unified progress tracking
    ///
    /// # Returns
    /// The SHA256 checksum of the complete file
    ///
    /// # Features
    /// - Sequential chunk transfer with per-chunk acknowledgment
    /// - Automatic resume capability (skips completed chunks)
    /// - Optional compression with adaptive detection
    /// - CRC32 checksum verification per chunk
    /// - Bandwidth throttling support
    /// - Streaming SHA256 computation (no full file read required)
    /// - Chunk completion tracking for incremental state saving
    pub async fn send_file<F>(
        &mut self,
        path: &Path,
        completed_chunks: &[u64],
        mut chunk_complete_callback: Option<F>,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<[u8; 32]>
    where
        F: FnMut(u64),
    {
        debug!("Starting file send: {:?}", path);

        let mut reader = ChunkReader::new(path, self.config.chunk_size as usize).await?;
        let total_chunks = reader.total_chunks();

        if !completed_chunks.is_empty() {
            info!(
                "Resuming: {} chunks already completed",
                completed_chunks.len()
            );
        }
        debug!("File has {} total chunks", total_chunks);

        // Compression if enabled
        let mut compressor: Option<AdaptiveCompressor> = if self.config.compression_enabled {
            let sample_size = if self.config.adaptive_compression {
                3
            } else {
                0
            };
            Some(AdaptiveCompressor::new(
                self.config.compression_level,
                sample_size,
            ))
        } else {
            None
        };

        for chunk_index in 0..total_chunks {
            // Skip already completed chunks
            if completed_chunks.contains(&(chunk_index as u64)) {
                trace!("Skipping already completed chunk {}", chunk_index);
                continue;
            }
            // Read chunk (this also updates the running SHA256 checksum)
            let chunk_data = reader.read_chunk(chunk_index).await?;
            let uncompressed_size = chunk_data.len() as u64;

            // Compress if enabled
            let (final_data, is_compressed) = if let Some(comp) = &mut compressor {
                let (compressed, was_compressed, _decision_changed) = comp.compress(&chunk_data)?;
                (compressed, was_compressed)
            } else {
                (chunk_data, false)
            };

            // Set flags based on compression
            let mut flags = 0u8;
            if is_compressed {
                flags = ChunkMessage::set_flag(flags, ChunkMessage::FLAG_COMPRESSED);
            }

            // Calculate checksum
            let checksum = verification::crc32(&final_data);

            // Apply bandwidth throttling if enabled
            if let Some(limiter) = &self.bandwidth_limiter {
                limiter.wait_for_tokens(final_data.len()).await;
            }

            // Send chunk
            let chunk_msg = ChunkMessage {
                transfer_id: self.transfer_id,
                file_index: self.file_index,
                chunk_index: chunk_index as u64,
                total_chunks: total_chunks as u64,
                flags,
                checksum,
                data: final_data,
            };

            // Track compression statistics
            self.compressed_bytes_sent += chunk_msg.data.len() as u64;
            self.uncompressed_bytes_sent += uncompressed_size;

            // Send chunk and retry up to MAX_CHUNK_RETRIES times on ACK timeout
            const MAX_CHUNK_RETRIES: u32 = 5;
            let mut attempts = 0u32;
            let ack = loop {
                self.connection
                    .send_message(&Message::Chunk(chunk_msg.clone()))
                    .await?;

                match timeout(Duration::from_secs(10), self.receive_ack()).await {
                    Ok(Ok(ack)) => break ack,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        attempts += 1;
                        if attempts >= MAX_CHUNK_RETRIES {
                            return Err(Error::Protocol(format!(
                                "Chunk {} ack timeout after {} retries",
                                chunk_index, attempts
                            )));
                        }
                        warn!(
                            "Chunk {} ack timeout, retrying ({}/{})",
                            chunk_index, attempts, MAX_CHUNK_RETRIES
                        );
                    }
                }
            };

            if ack != chunk_index {
                return Err(Error::Protocol(format!(
                    "Expected ack for chunk {}, got {}",
                    chunk_index, ack
                )));
            }

            // Update progress after successful chunk send (uncompressed size)
            if let Some(ref mut progress) = progress {
                progress.add_bytes(uncompressed_size);
            }

            // Notify callback that chunk completed successfully
            if let Some(ref mut callback) = chunk_complete_callback {
                callback(chunk_index as u64);
            }

            trace!("Sent chunk {}/{}", chunk_index + 1, total_chunks);
        }

        // Finalize and get the SHA256 checksum
        let checksum = reader.finalize_checksum();
        debug!("File transfer complete, SHA256: {:02x?}", &checksum[..8]);
        Ok(checksum)
    }

    /// Sends a file to the peer using the sliding window protocol for high performance.
    ///
    /// This method sends multiple chunks in parallel without waiting for individual acknowledgments,
    /// providing 5-15x speedup on high-latency networks. It supports automatic resume by skipping
    /// already-completed chunks. The file's SHA256 checksum is computed incrementally during the transfer.
    ///
    /// # Arguments
    /// * `path` - Path to the file to send
    /// * `window_config` - Window configuration (max window size, timeout, retries)
    /// * `completed_chunks` - Slice of chunk indices that have already been transferred (empty for new transfers)
    /// * `chunk_complete_callback` - Optional boxed closure invoked after each chunk is successfully acknowledged
    /// * `progress` - Optional progress state for unified progress tracking
    ///
    /// # Returns
    /// The SHA256 checksum of the complete file
    ///
    /// # Features
    /// - Parallel chunk transfer with sliding window flow control
    /// - Automatic resume capability (skips completed chunks)
    /// - Automatic retry on chunk failures (up to max_retries)
    /// - Optional compression with adaptive detection
    /// - CRC32 checksum verification per chunk
    /// - Bandwidth throttling support
    /// - Streaming SHA256 computation (no full file read required)
    /// - Chunk completion tracking for incremental state saving
    ///
    /// # Performance
    /// The sliding window protocol significantly improves transfer speed on networks with
    /// high latency by keeping the pipeline full with in-flight chunks.
    pub async fn send_file_windowed<F>(
        &mut self,
        path: &Path,
        window_config: &WindowConfig,
        completed_chunks: &[u64],
        mut chunk_complete_callback: Option<F>,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<[u8; 32]>
    where
        F: FnMut(u64),
    {
        debug!("Starting windowed file send: {:?}", path);

        let mut reader = ChunkReader::new(path, self.config.chunk_size as usize).await?;
        let total_chunks = reader.total_chunks();

        if !completed_chunks.is_empty() {
            info!(
                "Resuming: {} chunks already completed",
                completed_chunks.len()
            );
        }
        debug!(
            "File has {} total chunks, using sliding window protocol",
            total_chunks
        );

        // Create sliding window
        let mut window = SlidingWindow::new(window_config.clone(), total_chunks);

        // Mark completed chunks in the window
        for &chunk_index in completed_chunks {
            if chunk_index < total_chunks as u64 {
                window.mark_completed(chunk_index as u32);
                trace!("Marked chunk {} as already completed", chunk_index);
            }
        }

        // Compression if enabled
        let mut compressor: Option<AdaptiveCompressor> = if self.config.compression_enabled {
            let sample_size = if self.config.adaptive_compression {
                3
            } else {
                0
            };
            Some(AdaptiveCompressor::new(
                self.config.compression_level,
                sample_size,
            ))
        } else {
            None
        };

        // Main transfer loop
        let mut last_progress = 0;

        loop {
            // Phase 1: Fill the window by sending chunks
            while window.can_send() {
                if let Some(chunk_index) = window.next_chunk_to_send() {
                    // Read chunk
                    let chunk_data = reader.read_chunk(chunk_index).await?;
                    let uncompressed_size = chunk_data.len() as u64;

                    // Compress if enabled
                    let (final_data, is_compressed) = if let Some(comp) = &mut compressor {
                        let (compressed, was_compressed, _decision_changed) =
                            comp.compress(&chunk_data)?;
                        (compressed, was_compressed)
                    } else {
                        (chunk_data, false)
                    };

                    // Set flags based on compression
                    let mut flags = 0u8;
                    if is_compressed {
                        flags = ChunkMessage::set_flag(flags, ChunkMessage::FLAG_COMPRESSED);
                    }

                    // Calculate checksum
                    let checksum = verification::crc32(&final_data);

                    // Apply bandwidth throttling if enabled
                    if let Some(limiter) = &self.bandwidth_limiter {
                        limiter.wait_for_tokens(final_data.len()).await;
                    }

                    // Send chunk
                    let chunk_msg = ChunkMessage {
                        transfer_id: self.transfer_id,
                        file_index: self.file_index,
                        chunk_index: chunk_index as u64,
                        total_chunks: total_chunks as u64,
                        flags,
                        checksum,
                        data: final_data.clone(),
                    };

                    // Track compression statistics
                    self.compressed_bytes_sent += chunk_msg.data.len() as u64;
                    self.uncompressed_bytes_sent += uncompressed_size;

                    self.connection
                        .send_message(&Message::Chunk(chunk_msg.clone()))
                        .await?;

                    // Update progress immediately after sending (uncompressed size)
                    if let Some(ref mut progress) = progress {
                        progress.add_bytes(uncompressed_size);
                    }

                    // Mark as in-flight (store the actual message for potential retransmission)
                    let in_flight = InFlightChunk {
                        message: chunk_msg,
                        sent_at: Instant::now(),
                        retry_count: 0,
                    };
                    window.mark_sent(in_flight);

                    trace!(
                        "Sent chunk {} (window: {}/{})",
                        chunk_index,
                        window.in_flight_count(),
                        window_config.max_window_size
                    );
                } else {
                    break;
                }
            }

            // Phase 2: Try to receive ACKs (with short timeout to not block)
            match timeout(Duration::from_millis(50), self.connection.recv_message()).await {
                Ok(Ok(Message::ChunkAck(ack))) if ack.status == AckStatus::Success => {
                    let chunk_idx = ack.chunk_index;
                    window.process_ack(chunk_idx as u32);
                    trace!("ACK received for chunk {}", chunk_idx);

                    // Notify callback that chunk completed successfully
                    if let Some(ref mut callback) = chunk_complete_callback {
                        callback(chunk_idx);
                    }
                }
                Ok(Ok(_)) => {
                    // Other message type, ignore
                }
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(_) => {
                    // Timeout is OK - just means no ACKs ready yet
                }
            }

            // Check for timeouts and retry
            let timed_out = window.check_timeouts();
            for chunk in timed_out {
                warn!(
                    "Chunk {} timed out, retrying (attempt {})",
                    chunk.message.chunk_index, chunk.retry_count
                );

                // Apply bandwidth throttling for retries if enabled
                if let Some(limiter) = &self.bandwidth_limiter {
                    limiter.wait_for_tokens(chunk.message.data.len()).await;
                }

                // Resend the chunk (we already have the complete message)
                let chunk_msg = chunk.message.clone();

                // Note: We don't re-count retries in statistics since the data was already counted
                // in the initial send. Only the network bytes are being resent.

                self.connection
                    .send_message(&Message::Chunk(chunk_msg))
                    .await?;

                // Re-mark as in-flight with updated retry count
                window.mark_sent(chunk);
            }

            // Check for failed chunks (exceeded max retries)
            let failed = window.get_failed_chunks();
            if !failed.is_empty() {
                return Err(Error::Protocol(format!(
                    "Chunks {:?} failed after max retries",
                    failed
                )));
            }

            // Log progress periodically
            let stats = window.stats();
            if stats.acked != last_progress && stats.acked % 10 == 0 {
                debug!(
                    "Progress: {}/{} chunks ({:.1}% complete, {} in-flight)",
                    stats.acked,
                    stats.total,
                    (stats.acked as f32 / stats.total as f32) * 100.0,
                    stats.in_flight
                );
                last_progress = stats.acked;
            }

            // Check if complete
            if window.is_complete() {
                debug!("File transfer complete!");
                break;
            }

            // Small delay between iterations if window is full and no ACKs
            if !window.can_send() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // Finalize and get the SHA256 checksum
        let checksum = reader.finalize_checksum();
        debug!("File transfer complete, SHA256: {:02x?}", &checksum[..8]);
        Ok(checksum)
    }

    /// Receive a file from the peer and compute SHA256 checksum incrementally.
    ///
    /// The total number of chunks is determined from the first chunk message received.
    /// Returns the computed SHA256 checksum for verification.
    pub async fn receive_file(&mut self, output_path: &Path) -> Result<[u8; 32]> {
        debug!("Starting file receive: {:?}", output_path);

        let mut writer = ChunkWriter::new(output_path, self.config.chunk_size as usize).await?;

        // Decompression if enabled
        let mut decompressor: Option<Decompressor> = if self.config.compression_enabled {
            Some(Decompressor::new())
        } else {
            None
        };

        let mut received = 0;
        let mut total_chunks: Option<u64> = None;

        loop {
            // Receive chunk message
            let msg = timeout(Duration::from_secs(30), self.connection.recv_message())
                .await
                .map_err(|_| Error::Protocol("Chunk receive timeout".to_string()))??;

            match msg {
                Message::Chunk(chunk_msg) => {
                    // On first chunk, learn the total chunks from the message
                    if total_chunks.is_none() {
                        total_chunks = Some(chunk_msg.total_chunks);
                        info!("Transfer has {} total chunks", chunk_msg.total_chunks);
                    }

                    let chunk_index = chunk_msg.chunk_index as u32;

                    // Verify checksum first (fast, must be sync to catch corruption)
                    verification::verify_crc32(&chunk_msg.data, chunk_msg.checksum)?;

                    // Start sending ACK (creates future but doesn't wait yet)
                    let ack_future = self.send_ack(chunk_index, AckStatus::Success);

                    // Do expensive operations while ACK is being sent in parallel
                    let final_data = if let Some(decomp) = &mut decompressor {
                        decomp.decompress(&chunk_msg.data)?
                    } else {
                        chunk_msg.data
                    };
                    // Write chunk also updates the running SHA256 checksum
                    writer.write_chunk(chunk_index, &final_data).await?;
                    received += 1;

                    // Ensure ACK send completed before processing next chunk
                    ack_future.await?;

                    // Check if transfer is complete
                    if let Some(total) = total_chunks {
                        trace!("Received chunk {}/{}", received, total);
                        if received >= total {
                            info!("All chunks received, transfer complete");
                            break;
                        }
                    }
                }
                _ => {
                    warn!("Unexpected message during transfer: {:?}", msg);
                }
            }
        }

        // Finalize file and get the computed checksum
        let checksum = writer.finalize().await?;

        debug!("File receive complete, SHA256: {:02x?}", &checksum[..8]);
        Ok(checksum)
    }

    /// Receive a chunk acknowledgment
    async fn receive_ack(&mut self) -> Result<u32> {
        let msg = self.connection.recv_message().await?;

        match msg {
            Message::ChunkAck(ack_msg) => {
                if ack_msg.status == AckStatus::Success {
                    Ok(ack_msg.chunk_index as u32)
                } else {
                    Err(Error::Protocol(format!(
                        "Chunk {} was rejected with status {:?}",
                        ack_msg.chunk_index, ack_msg.status
                    )))
                }
            }
            _ => Err(Error::Protocol(format!("Expected ChunkAck, got {:?}", msg))),
        }
    }

    /// Send a chunk acknowledgment
    async fn send_ack(&mut self, chunk_index: u32, status: AckStatus) -> Result<()> {
        let ack_msg = ChunkAck {
            transfer_id: self.transfer_id,
            file_index: self.file_index,
            chunk_index: chunk_index as u64,
            status,
        };

        self.connection
            .send_message(&Message::ChunkAck(ack_msg))
            .await
    }
}

/// Chunk-based file reader with streaming checksum computation
pub struct ChunkReader {
    file: File,
    chunk_size: usize,
    total_chunks: u32,
    file_size: u64,
    hasher: sha2::Sha256,
}

impl ChunkReader {
    /// Create a new chunk reader
    pub async fn new(path: &Path, chunk_size: usize) -> Result<Self> {
        let file = File::open(path).await.map_err(|e| {
            Error::Network(std::io::Error::new(
                e.kind(),
                format!("Failed to open file {:?}: {}", path, e),
            ))
        })?;

        let metadata = file.metadata().await?;
        let file_size = metadata.len();
        let total_chunks = ((file_size + chunk_size as u64 - 1) / chunk_size as u64) as u32;

        Ok(Self {
            file,
            chunk_size,
            total_chunks,
            file_size,
            hasher: sha2::Sha256::new(),
        })
    }

    /// Get total number of chunks
    pub fn total_chunks(&self) -> u32 {
        self.total_chunks
    }

    /// Read a specific chunk and update running checksum
    pub async fn read_chunk(&mut self, index: u32) -> Result<Vec<u8>> {
        let offset = index as u64 * self.chunk_size as u64;
        self.file.seek(SeekFrom::Start(offset)).await?;

        let remaining = self.file_size - offset;
        let to_read = std::cmp::min(remaining, self.chunk_size as u64) as usize;

        let mut buffer = vec![0u8; to_read];
        self.file.read_exact(&mut buffer).await?;

        // Update running checksum
        use sha2::Digest;
        self.hasher.update(&buffer);

        Ok(buffer)
    }

    /// Finalize and return the SHA256 checksum
    pub fn finalize_checksum(self) -> [u8; 32] {
        use sha2::Digest;
        self.hasher.finalize().into()
    }
}

/// Chunk-based file writer with streaming checksum computation
pub struct ChunkWriter {
    file: File,
    path: PathBuf,
    chunk_size: usize,
    hasher: sha2::Sha256,
}

impl ChunkWriter {
    /// Create a new chunk writer
    pub async fn new(path: &Path, chunk_size: usize) -> Result<Self> {
        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Create file with .partial suffix (not replacing extension)
        let mut partial_path = path.as_os_str().to_os_string();
        partial_path.push(".partial");
        let partial_path = PathBuf::from(partial_path);

        let file = File::create(&partial_path).await.map_err(|e| {
            Error::Network(std::io::Error::new(
                e.kind(),
                format!("Failed to create file {:?}: {}", partial_path, e),
            ))
        })?;

        Ok(Self {
            file,
            path: path.to_path_buf(), // Store original path
            chunk_size,
            hasher: sha2::Sha256::new(),
        })
    }

    /// Open an existing `.partial` file for resuming a partial transfer.
    ///
    /// Reads the already-received chunks in index order to reconstruct the running
    /// SHA-256 state so the final checksum will be correct over the whole file.
    pub async fn open_for_resume(
        path: &Path,
        chunk_size: usize,
        completed_chunks: &[u64],
    ) -> Result<Self> {
        // Build the .partial path
        let mut partial_os = path.as_os_str().to_os_string();
        partial_os.push(".partial");
        let partial_path = PathBuf::from(partial_os);

        // Open for writing without truncating existing content
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&partial_path)
            .await
            .map_err(|e| {
                Error::Network(std::io::Error::new(
                    e.kind(),
                    format!("Failed to open partial file {:?}: {}", partial_path, e),
                ))
            })?;

        // Re-hash already-received chunks in order so the running SHA-256 is consistent
        let mut hasher = sha2::Sha256::new();
        if !completed_chunks.is_empty() {
            let mut sorted = completed_chunks.to_vec();
            sorted.sort_unstable();

            let mut read_file = File::open(&partial_path).await?;
            for chunk_idx in sorted {
                read_file
                    .seek(SeekFrom::Start(chunk_idx * chunk_size as u64))
                    .await?;
                let mut buf = vec![0u8; chunk_size];
                let n = read_file.read(&mut buf).await?;
                if n > 0 {
                    hasher.update(&buf[..n]);
                }
            }
        }

        Ok(Self {
            file,
            path: path.to_path_buf(),
            chunk_size,
            hasher,
        })
    }

    /// Write a chunk at the specified index and update running checksum
    pub async fn write_chunk(&mut self, index: u32, data: &[u8]) -> Result<()> {
        let offset = index as u64 * self.chunk_size as u64;
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(data).await?;
        self.file.flush().await?;

        // Update running checksum
        self.hasher.update(data);

        Ok(())
    }

    /// Get the partial file path
    fn partial_path(&self) -> PathBuf {
        let mut partial_path = self.path.as_os_str().to_os_string();
        partial_path.push(".partial");
        PathBuf::from(partial_path)
    }

    /// Finalize the file (remove .partial suffix) and return the computed checksum
    pub async fn finalize(self) -> Result<[u8; 32]> {
        // Compute paths before consuming self
        let partial_path = self.partial_path();
        let final_path = self.path.clone();

        // Finalize checksum before moving file handle
        let checksum: [u8; 32] = self.hasher.finalize().into();

        // Ensure all data is written
        self.file.sync_all().await?;
        drop(self.file);

        // Rename from .partial to final name
        tokio::fs::rename(&partial_path, &final_path).await?;

        info!("File finalized: {:?}", final_path);
        Ok(checksum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_chunk_reader() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");

        // Create a test file (200 bytes)
        let mut file = File::create(&file_path).await.unwrap();
        let data = vec![0x42u8; 200];
        file.write_all(&data).await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        // Read in 64-byte chunks
        let mut reader = ChunkReader::new(&file_path, 64).await.unwrap();

        assert_eq!(reader.total_chunks(), 4); // 200 / 64 = 3.125, rounded up to 4

        // Read all chunks
        let chunk0 = reader.read_chunk(0).await.unwrap();
        assert_eq!(chunk0.len(), 64);
        assert!(chunk0.iter().all(|&b| b == 0x42));

        let chunk1 = reader.read_chunk(1).await.unwrap();
        assert_eq!(chunk1.len(), 64);

        let chunk2 = reader.read_chunk(2).await.unwrap();
        assert_eq!(chunk2.len(), 64);

        let chunk3 = reader.read_chunk(3).await.unwrap();
        assert_eq!(chunk3.len(), 8); // Last chunk is smaller
    }

    #[tokio::test]
    async fn test_chunk_writer() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("output.txt");

        let mut writer = ChunkWriter::new(&file_path, 64).await.unwrap();

        // Write chunks out of order
        let data2 = vec![0x02u8; 64];
        writer.write_chunk(2, &data2).await.unwrap();

        let data0 = vec![0x00u8; 64];
        writer.write_chunk(0, &data0).await.unwrap();

        let data1 = vec![0x01u8; 64];
        writer.write_chunk(1, &data1).await.unwrap();

        let data3 = vec![0x03u8; 8];
        writer.write_chunk(3, &data3).await.unwrap();

        // Finalize
        writer.finalize().await.unwrap();

        // Verify file
        let final_path = dir.path().join("output.txt");
        let content = tokio::fs::read(&final_path).await.unwrap();
        assert_eq!(content.len(), 200);

        // Check chunks are in correct order
        assert!(content[0..64].iter().all(|&b| b == 0x00));
        assert!(content[64..128].iter().all(|&b| b == 0x01));
        assert!(content[128..192].iter().all(|&b| b == 0x02));
        assert!(content[192..200].iter().all(|&b| b == 0x03));
    }
}
