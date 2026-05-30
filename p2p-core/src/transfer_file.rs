//! Single-file transfer over QUIC.
//!
//! The sender opens one unidirectional QUIC stream per chunk:
//!
//! ```text
//! [chunk_index : u64 LE | flags : u8 | payload bytes (compressed iff flags&1)]
//! ```
//!
//! The receiver loops on `connection.accept_uni()`, parses the index/flags
//! header, decompresses if needed, and writes the payload at
//! `chunk_index * chunk_size` in the destination file. QUIC's per-stream
//! flow control + packet retransmission replaces what the old sliding
//! window / per-chunk ACK / per-chunk CRC32 layer used to do; TLS 1.3 AEAD
//! authenticates every byte so a chunk-level CRC would be redundant.
//!
//! File-level integrity is still checked: the sender computes the SHA-256
//! incrementally as it reads chunks in order, and the receiver computes it
//! at the end by re-reading the finalized file (chunks land in any order).
//! The two sides exchange `FileChecksum` messages over the control stream
//! to compare.

use std::collections::HashSet;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, info, trace, warn};

use crate::bandwidth::BandwidthLimiter;
use crate::compression::{AdaptiveCompressor, Decompressor};
use crate::error::{Error, Result};
use crate::network::quic::QuicConnection;
use crate::progress::ProgressState;
use crate::protocol::ConfigMessage;

/// Maximum bytes we'll read from a single chunk stream. A safety cap; in
/// practice the wire payload is `chunk_size` (default 1 MiB).
const MAX_CHUNK_STREAM_BYTES: usize = 16 * 1024 * 1024;

/// Maximum per-file size we'll honour from a peer-supplied manifest.
/// Without this cap a hostile peer can advertise a multi-petabyte
/// `file_size`, leading the receiver to compute an enormous
/// `total_chunks` and block in `accept_uni()` forever (finding 4.1).
/// 1 TiB is large enough for any plausible single-file transfer.
pub const MAX_TRANSFER_FILE_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

/// Reject a peer-supplied per-file size that exceeds the sanity bound.
/// Called from the folder-receive path before any stream is accepted.
pub fn validate_file_size(size: u64) -> Result<()> {
    if size > MAX_TRANSFER_FILE_SIZE {
        return Err(Error::Protocol(format!(
            "peer-supplied file size {size} exceeds maximum {MAX_TRANSFER_FILE_SIZE}"
        )));
    }
    Ok(())
}

/// Ceiling-divide a file size by chunk size — number of full + final
/// chunks needed to cover `file_size` bytes when laid out under
/// `chunk_size`. Single source of truth (the sender, receiver, and
/// resume state all must agree).
pub const fn chunk_count(file_size: u64, chunk_size: u32) -> u64 {
    let cs = chunk_size as u64;
    (file_size + cs - 1) / cs
}

/// Per-chunk header: `[index: u64 LE | flags: u8]`.
const CHUNK_HEADER_BYTES: usize = 9;

/// Flag bit 0: payload is zstd-compressed.
const FLAG_COMPRESSED: u8 = 0b0000_0001;

/// File transfer session. Borrows the QUIC connection; never owns it.
pub struct FileTransferSession<'a> {
    connection: &'a mut QuicConnection,
    config: ConfigMessage,
    bandwidth_limiter: Option<BandwidthLimiter>,
    pub compressed_bytes_sent: u64,
    pub uncompressed_bytes_sent: u64,
}

impl<'a> FileTransferSession<'a> {
    pub fn new(connection: &'a mut QuicConnection, config: ConfigMessage) -> Self {
        let bandwidth_limiter = if config.bandwidth_limit > 0 {
            Some(BandwidthLimiter::new(config.bandwidth_limit))
        } else {
            None
        };
        Self {
            connection,
            config,
            bandwidth_limiter,
            compressed_bytes_sent: 0,
            uncompressed_bytes_sent: 0,
        }
    }

    /// Send a file to the peer one uni-stream per chunk, skipping any
    /// chunk indices already present in `completed_chunks` (resume).
    ///
    /// Returns the SHA-256 of the complete file (computed incrementally
    /// as chunks are read in order).
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
                "Resuming: {} of {} chunks already completed",
                completed_chunks.len(),
                total_chunks
            );
        }

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

        // O(1) lookup vs O(n) on a slice — matters once a resume bitmap
        // covers tens of thousands of chunks.
        let completed: HashSet<u64> = completed_chunks.iter().copied().collect();

        for chunk_index in 0..total_chunks {
            if completed.contains(&chunk_index) {
                trace!("Skipping already-completed chunk {}", chunk_index);
                // ChunkReader.read_chunk seeks per call, so skipping is safe;
                // but we still need to fold the chunk into the SHA-256.
                reader.fold_chunk(chunk_index).await?;
                continue;
            }

            let chunk_data = reader.read_chunk(chunk_index).await?;
            let uncompressed_size = chunk_data.len() as u64;

            let (final_data, is_compressed) = if let Some(comp) = &mut compressor {
                let (compressed, was_compressed, _decision_changed) = comp.compress(&chunk_data)?;
                (compressed, was_compressed)
            } else {
                (chunk_data, false)
            };

            if let Some(limiter) = &self.bandwidth_limiter {
                limiter.wait_for_tokens(final_data.len()).await;
            }

            self.send_chunk_stream(chunk_index, is_compressed, &final_data)
                .await?;

            self.compressed_bytes_sent += final_data.len() as u64;
            self.uncompressed_bytes_sent += uncompressed_size;

            if let Some(ref mut p) = progress {
                p.add_bytes(uncompressed_size);
            }
            if let Some(ref mut cb) = chunk_complete_callback {
                cb(chunk_index);
            }

            trace!("Sent chunk {}/{}", chunk_index + 1, total_chunks);
        }

        let checksum = reader.finalize_checksum();
        debug!("File send complete, SHA256: {:02x?}", &checksum[..8]);
        Ok(checksum)
    }

    /// Receive a file from the peer. `total_chunks` is the file's total
    /// chunk count (used as the bound for incoming `chunk_index` values
    /// AND to size the `.partial` file); `streams_to_receive` is the
    /// number of DISTINCT chunk_indices the sender will deliver —
    /// `total_chunks - already_sent` on a resume. After all chunks land,
    /// re-read the file from disk to compute its SHA-256.
    ///
    /// Duplicate streams are dropped with a warn so a buggy or hostile
    /// peer cannot satisfy the stream count while leaving a real chunk
    /// missing (finding 1.6).
    pub async fn receive_file(
        &mut self,
        output_path: &Path,
        total_chunks: u64,
        streams_to_receive: u64,
        mut chunk_complete_callback: Option<impl FnMut(u64)>,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<[u8; 32]> {
        debug!(
            "Starting file receive: {:?} ({} chunks total, {} distinct streams expected)",
            output_path, total_chunks, streams_to_receive
        );
        if streams_to_receive > total_chunks {
            return Err(Error::Protocol(format!(
                "streams_to_receive {streams_to_receive} > total_chunks {total_chunks}"
            )));
        }

        let expected_file_size = total_chunks * self.config.chunk_size as u64;
        let mut writer = ChunkWriter::new(
            output_path,
            self.config.chunk_size as usize,
            expected_file_size,
        )
        .await?;
        let mut decompressor: Option<Decompressor> = if self.config.compression_enabled {
            Some(Decompressor::new())
        } else {
            None
        };

        let mut seen: HashSet<u64> = HashSet::with_capacity(streams_to_receive as usize);
        while (seen.len() as u64) < streams_to_receive {
            let mut stream = self.connection.accept_uni().await?;
            let raw = stream
                .read_to_end(MAX_CHUNK_STREAM_BYTES)
                .await
                .map_err(|e| Error::Quic(format!("chunk stream read: {e}")))?;

            if raw.len() < CHUNK_HEADER_BYTES {
                return Err(Error::Protocol(format!(
                    "chunk stream too short: {} bytes",
                    raw.len()
                )));
            }
            let chunk_index = u64::from_le_bytes(raw[0..8].try_into().expect("8 bytes"));
            if chunk_index >= total_chunks {
                return Err(Error::Protocol(format!(
                    "chunk_index {chunk_index} >= total_chunks {total_chunks}"
                )));
            }
            if !seen.insert(chunk_index) {
                warn!(
                    "duplicate chunk_index {chunk_index} on a fresh stream; ignoring (already received)"
                );
                continue;
            }
            let flags = raw[8];
            let payload = &raw[CHUNK_HEADER_BYTES..];

            // Avoid an allocation per uncompressed chunk: write the slice
            // straight into the chunk writer. Decompression still has to
            // produce an owned Vec because zstd needs scratch space.
            let written = if flags & FLAG_COMPRESSED != 0 {
                let decomp = decompressor.as_mut().ok_or_else(|| {
                    Error::Protocol(
                        "compressed chunk but compression disabled in config".to_string(),
                    )
                })?;
                let decompressed = decomp.decompress(payload)?;
                let len = decompressed.len() as u64;
                writer.write_chunk(chunk_index, &decompressed).await?;
                len
            } else {
                writer.write_chunk(chunk_index, payload).await?;
                payload.len() as u64
            };

            if let Some(ref mut p) = progress {
                p.add_bytes(written);
            }
            if let Some(ref mut cb) = chunk_complete_callback {
                cb(chunk_index);
            }

            trace!(
                "Received chunk {} ({}/{})",
                chunk_index,
                seen.len(),
                streams_to_receive
            );
        }

        let checksum = writer.finalize().await?;
        debug!("File receive complete, SHA256: {:02x?}", &checksum[..8]);
        Ok(checksum)
    }

    async fn send_chunk_stream(
        &self,
        chunk_index: u64,
        compressed: bool,
        data: &[u8],
    ) -> Result<()> {
        let mut stream = self.connection.open_uni().await?;
        // Pack `index || flags` into one fixed-size header so the whole
        // 9-byte preamble lands in a single write_all call.
        let mut header = [0u8; CHUNK_HEADER_BYTES];
        header[..8].copy_from_slice(&chunk_index.to_le_bytes());
        header[8] = if compressed { FLAG_COMPRESSED } else { 0 };
        stream
            .write_all(&header)
            .await
            .map_err(|e| Error::Quic(format!("write header: {e}")))?;
        stream
            .write_all(data)
            .await
            .map_err(|e| Error::Quic(format!("write payload: {e}")))?;
        stream
            .finish()
            .map_err(|e| Error::Quic(format!("finish stream: {e}")))?;
        // Wait for the peer to acknowledge the whole stream before we
        // return — otherwise the connection can be torn down while the
        // last chunk is still in flight and the receiver loses it.
        stream
            .stopped()
            .await
            .map_err(|e| Error::Quic(format!("stream stopped: {e}")))?;
        Ok(())
    }
}

// ----------------------------------------------------------------------
// Chunk reader (sender side) — streams the file in order, hashes inline.
// ----------------------------------------------------------------------

pub struct ChunkReader {
    file: File,
    chunk_size: usize,
    total_chunks: u64,
    file_size: u64,
    hasher: Sha256,
}

impl ChunkReader {
    pub async fn new(path: &Path, chunk_size: usize) -> Result<Self> {
        let file = File::open(path).await.map_err(|e| {
            Error::Network(std::io::Error::new(
                e.kind(),
                format!("Failed to open file {:?}: {}", path, e),
            ))
        })?;
        let metadata = file.metadata().await?;
        let file_size = metadata.len();
        let total_chunks = chunk_count(file_size, chunk_size as u32);
        Ok(Self {
            file,
            chunk_size,
            total_chunks,
            file_size,
            hasher: Sha256::new(),
        })
    }

    pub fn total_chunks(&self) -> u64 {
        self.total_chunks
    }

    /// Read `index`-th chunk from disk, updating the running SHA-256.
    pub async fn read_chunk(&mut self, index: u64) -> Result<Vec<u8>> {
        let offset = index * self.chunk_size as u64;
        self.file.seek(SeekFrom::Start(offset)).await?;
        let remaining = self.file_size - offset;
        let to_read = remaining.min(self.chunk_size as u64) as usize;
        let mut buffer = vec![0u8; to_read];
        self.file.read_exact(&mut buffer).await?;
        self.hasher.update(&buffer);
        Ok(buffer)
    }

    /// Read `index`-th chunk and fold it into the running SHA-256 but
    /// discard the bytes. Used during resume to keep the running hash
    /// over the full file even when we don't re-send the chunk.
    pub async fn fold_chunk(&mut self, index: u64) -> Result<()> {
        let _ = self.read_chunk(index).await?;
        Ok(())
    }

    pub fn finalize_checksum(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

// ----------------------------------------------------------------------
// Chunk writer (receiver side) — writes chunks at arbitrary offsets,
// then re-reads the file from disk to compute the SHA-256.
// ----------------------------------------------------------------------

pub struct ChunkWriter {
    file: File,
    path: PathBuf,
    chunk_size: usize,
}

impl ChunkWriter {
    /// Open (or create) the `<path>.partial` file. If a leftover partial
    /// from an earlier session is longer than `expected_file_size`
    /// (e.g. the user changed `--chunk-size` between sessions, or the file
    /// was externally truncated to a larger size), truncate it back to the
    /// expected length so stale trailing bytes never survive into
    /// `finalize` (finding 1.7).
    pub async fn new(path: &Path, chunk_size: usize, expected_file_size: u64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut partial = path.as_os_str().to_os_string();
        partial.push(".partial");
        let partial = PathBuf::from(partial);

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&partial)
            .await
            .map_err(|e| {
                Error::Network(std::io::Error::new(
                    e.kind(),
                    format!("Failed to create file {:?}: {}", partial, e),
                ))
            })?;

        let current_len = file.metadata().await?.len();
        if current_len > expected_file_size {
            warn!(
                ".partial file {:?} is {} bytes; truncating to expected {} bytes",
                partial, current_len, expected_file_size
            );
            file.set_len(expected_file_size).await?;
        }

        Ok(Self {
            file,
            path: path.to_path_buf(),
            chunk_size,
        })
    }

    /// Write a chunk at its absolute offset and `sync_data` so the bytes
    /// are durable before we report the chunk complete to the resume
    /// state. Without this, a power loss between `write_chunk` returning
    /// and `finalize().sync_all()` would leave the resume state lying
    /// about which chunks the receiver has (finding 1.8).
    pub async fn write_chunk(&mut self, index: u64, data: &[u8]) -> Result<()> {
        let offset = index * self.chunk_size as u64;
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(data).await?;
        self.file.sync_data().await?;
        Ok(())
    }

    fn partial_path(&self) -> PathBuf {
        let mut p = self.path.as_os_str().to_os_string();
        p.push(".partial");
        PathBuf::from(p)
    }

    /// Sync to disk, rename `.partial` → final path, then re-read the
    /// finalized file to compute its SHA-256.
    pub async fn finalize(self) -> Result<[u8; 32]> {
        self.file.sync_all().await?;
        let partial_path = self.partial_path();
        let final_path = self.path.clone();
        drop(self.file);
        tokio::fs::rename(&partial_path, &final_path).await?;

        let mut hasher = Sha256::new();
        let mut f = File::open(&final_path).await?;
        // 1 MiB buffer amortises syscall overhead on the post-transfer
        // re-read (the limiting factor here is `read` cost, not CPU).
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        info!("File finalized: {:?}", final_path);
        Ok(hasher.finalize().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::Identity;
    use crate::network::quic::QuicEndpoint;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Single-file resume must not deadlock when the sender skips
    /// already-completed chunks: the receiver has to know to expect fewer
    /// streams than `total_chunks`.
    #[tokio::test]
    async fn resume_with_skipped_chunks_does_not_deadlock() {
        let chunk_size = 64usize;
        let total_chunks = 4u64;
        let completed = vec![0u64, 1];
        let file_bytes: Vec<u8> = (0..(chunk_size as u64 * total_chunks) as usize)
            .map(|i| (i % 251) as u8)
            .collect();

        let dir = tempdir().unwrap();
        let src = dir.path().join("src.bin");
        let dst = dir.path().join("dst.bin");
        tokio::fs::write(&src, &file_bytes).await.unwrap();
        // Pre-populate the receiver's .partial with the chunks the sender will skip,
        // so the final SHA-256 verification matches.
        let mut partial_bytes = vec![0u8; file_bytes.len()];
        for &idx in &completed {
            let off = idx as usize * chunk_size;
            partial_bytes[off..off + chunk_size]
                .copy_from_slice(&file_bytes[off..off + chunk_size]);
        }
        let partial_path = {
            let mut p = dst.clone().into_os_string();
            p.push(".partial");
            std::path::PathBuf::from(p)
        };
        tokio::fs::write(&partial_path, &partial_bytes)
            .await
            .unwrap();

        let server_id = Arc::new(Identity::generate().unwrap());
        let server_fp = server_id.fingerprint();
        let server_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            server_id.clone(),
        )
        .unwrap();
        let server_addr = server_ep.local_addr().unwrap();

        let cfg = ConfigMessage {
            compression_enabled: false,
            compression_level: 3,
            adaptive_compression: false,
            chunk_size: chunk_size as u32,
            bandwidth_limit: 0,
        };

        let dst_recv = dst.clone();
        let cfg_recv = cfg.clone();
        let streams_to_receive = total_chunks - completed.len() as u64;
        let recv_task = tokio::spawn(async move {
            let mut conn = server_ep.accept().await.unwrap();
            let _ = conn.recv_message().await.unwrap(); // drive accept_bi
            let mut session = FileTransferSession::new(&mut conn, cfg_recv);
            session
                .receive_file(
                    &dst_recv,
                    total_chunks,
                    streams_to_receive,
                    None::<fn(u64)>,
                    None,
                )
                .await
        });

        let client_id = Arc::new(Identity::generate().unwrap());
        let client_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            client_id,
        )
        .unwrap();
        let mut conn = client_ep.connect(server_addr, server_fp).await.unwrap();
        conn.send_message(&crate::protocol::Message::Ping)
            .await
            .unwrap();
        let mut session = FileTransferSession::new(&mut conn, cfg);
        let send_fut = session.send_file(&src, &completed, None::<fn(u64)>, None);

        let recv_result = tokio::time::timeout(Duration::from_secs(5), async {
            let send_checksum = send_fut.await.unwrap();
            let recv_checksum = recv_task.await.unwrap().unwrap();
            assert_eq!(send_checksum, recv_checksum, "checksums must match");
            recv_checksum
        })
        .await;

        recv_result.expect("resume must finish within 5 s — receiver expected too many streams");
    }

    /// Finding 1.7: an over-long `.partial` (e.g. chunk_size shrank
    /// between sessions, or external truncation lengthened the file) must
    /// be brought back into a consistent state on resume. Otherwise stale
    /// trailing bytes survive into `finalize` and the SHA-256 mismatches.
    #[tokio::test]
    async fn chunk_writer_truncates_oversized_partial() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("out.bin");

        let mut partial_path = p.as_os_str().to_os_string();
        partial_path.push(".partial");
        let partial_path = std::path::PathBuf::from(partial_path);
        // Pre-seed an over-long partial: 4 chunks of junk where we only
        // expect 2 chunks of payload.
        tokio::fs::write(&partial_path, vec![0xAAu8; 64 * 4])
            .await
            .unwrap();

        let expected_size = 64u64 * 2;
        let _writer = ChunkWriter::new(&p, 64, expected_size).await.unwrap();

        let meta = tokio::fs::metadata(&partial_path).await.unwrap();
        assert_eq!(
            meta.len(),
            expected_size,
            "ChunkWriter::new must truncate over-long .partial down to expected_file_size"
        );
    }

    /// Finding 1.6: the receive loop counts streams, not distinct
    /// chunk_indices. A buggy or hostile sender that re-opens the same
    /// chunk_index satisfies the count and the loop terminates one short,
    /// silently leaving a hole. With proper dedup, duplicates are ignored
    /// and the loop continues until `streams_to_receive` DISTINCT chunks
    /// have arrived.
    #[tokio::test]
    async fn receive_file_dedups_duplicate_chunk_streams() {
        use std::time::Duration;

        let chunk_size = 64usize;
        let total_chunks = 3u64;
        let payload: Vec<u8> = (0..(chunk_size as u64 * total_chunks) as usize)
            .map(|i| (i % 251) as u8)
            .collect();

        let dir = tempdir().unwrap();
        let dst = dir.path().join("out.bin");

        let server_id = Arc::new(Identity::generate().unwrap());
        let server_fp = server_id.fingerprint();
        let server_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            server_id.clone(),
        )
        .unwrap();
        let server_addr = server_ep.local_addr().unwrap();

        let cfg = ConfigMessage {
            compression_enabled: false,
            compression_level: 3,
            adaptive_compression: false,
            chunk_size: chunk_size as u32,
            bandwidth_limit: 0,
        };

        let dst_recv = dst.clone();
        let cfg_recv = cfg.clone();
        let recv_task = tokio::spawn(async move {
            let mut conn = server_ep.accept().await.unwrap();
            let _ = conn.recv_message().await.unwrap();
            let mut session = FileTransferSession::new(&mut conn, cfg_recv);
            // Pass total_chunks for both — sender opens 3 distinct streams
            // plus 1 duplicate of chunk 0.
            session
                .receive_file(&dst_recv, total_chunks, total_chunks, None::<fn(u64)>, None)
                .await
        });

        let client_id = Arc::new(Identity::generate().unwrap());
        let client_ep = QuicEndpoint::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            client_id,
        )
        .unwrap();
        let mut conn = client_ep.connect(server_addr, server_fp).await.unwrap();
        conn.send_message(&crate::protocol::Message::Ping)
            .await
            .unwrap();

        // Manually open 4 streams: [0, 0 (dup), 1, 2]. A dedup-correct
        // receiver must complete after seeing 3 distinct indices.
        let order = [0u64, 0, 1, 2];
        for &idx in &order {
            let mut stream = conn.open_uni().await.unwrap();
            stream.write_all(&idx.to_le_bytes()).await.unwrap();
            stream.write_all(&[0u8]).await.unwrap(); // flags = 0 (uncompressed)
            let off = (idx as usize) * chunk_size;
            stream
                .write_all(&payload[off..off + chunk_size])
                .await
                .unwrap();
            stream.finish().unwrap();
            stream.stopped().await.unwrap();
        }

        let recv_result = tokio::time::timeout(Duration::from_secs(5), recv_task)
            .await
            .expect("receiver must complete within 5 s — duplicates should not stall the loop")
            .unwrap()
            .unwrap();

        // SHA-256 of the assembled payload should match the original.
        let expected = {
            let mut h = Sha256::new();
            h.update(&payload);
            let r: [u8; 32] = h.finalize().into();
            r
        };
        assert_eq!(recv_result, expected);
    }

    /// Finding 4.1: peer-supplied file sizes must be sanity-bounded.
    /// Without a cap, a hostile peer can advertise a multi-petabyte
    /// file_size, leading the receiver to compute an enormous
    /// total_chunks and block in accept_uni() forever.
    #[test]
    fn validate_file_size_rejects_absurd_values() {
        assert!(super::validate_file_size(0).is_ok());
        assert!(super::validate_file_size(MAX_TRANSFER_FILE_SIZE).is_ok());
        assert!(super::validate_file_size(MAX_TRANSFER_FILE_SIZE + 1).is_err());
        assert!(super::validate_file_size(u64::MAX).is_err());
    }

    #[tokio::test]
    async fn chunk_reader_reads_and_hashes() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("test.bin");
        let data = vec![0x42u8; 200];
        tokio::fs::write(&p, &data).await.unwrap();

        let mut reader = ChunkReader::new(&p, 64).await.unwrap();
        assert_eq!(reader.total_chunks(), 4u64);

        for i in 0..reader.total_chunks() {
            let _ = reader.read_chunk(i).await.unwrap();
        }
        let sha = reader.finalize_checksum();

        let expected = {
            let mut h = Sha256::new();
            h.update(&data);
            let r: [u8; 32] = h.finalize().into();
            r
        };
        assert_eq!(sha, expected);
    }

    #[tokio::test]
    async fn chunk_writer_assembles_out_of_order() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("out.bin");
        let mut writer = ChunkWriter::new(&p, 64, 200).await.unwrap();

        writer.write_chunk(2u64, &[0x02u8; 64]).await.unwrap();
        writer.write_chunk(0u64, &[0x00u8; 64]).await.unwrap();
        writer.write_chunk(1u64, &[0x01u8; 64]).await.unwrap();
        writer.write_chunk(3u64, &[0x03u8; 8]).await.unwrap();

        let sha = writer.finalize().await.unwrap();
        let bytes = tokio::fs::read(&p).await.unwrap();
        assert_eq!(bytes.len(), 200);

        let expected = {
            let mut h = Sha256::new();
            h.update(&bytes);
            let r: [u8; 32] = h.finalize().into();
            r
        };
        assert_eq!(sha, expected);
    }
}
