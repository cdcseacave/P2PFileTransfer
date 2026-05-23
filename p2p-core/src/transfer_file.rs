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

use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, info, trace};
use uuid::Uuid;

use crate::bandwidth::BandwidthLimiter;
use crate::compression::{AdaptiveCompressor, Decompressor};
use crate::error::{Error, Result};
use crate::network::quic::QuicConnection;
use crate::progress::ProgressState;
use crate::protocol::ConfigMessage;

/// Maximum bytes we'll read from a single chunk stream. A safety cap; in
/// practice the wire payload is `chunk_size` (default 64 KiB).
const MAX_CHUNK_STREAM_BYTES: usize = 16 * 1024 * 1024;

/// Per-chunk header: `[index: u64 LE | flags: u8]`.
const CHUNK_HEADER_BYTES: usize = 9;

/// Flag bit 0: payload is zstd-compressed.
const FLAG_COMPRESSED: u8 = 0b0000_0001;

/// File transfer session. Borrows the QUIC connection; never owns it.
pub struct FileTransferSession<'a> {
    connection: &'a mut QuicConnection,
    config: ConfigMessage,
    #[allow(dead_code)]
    transfer_id: Uuid,
    #[allow(dead_code)]
    file_index: u32,
    bandwidth_limiter: Option<BandwidthLimiter>,
    pub compressed_bytes_sent: u64,
    pub uncompressed_bytes_sent: u64,
}

impl<'a> FileTransferSession<'a> {
    pub fn new(
        connection: &'a mut QuicConnection,
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
            let sample_size = if self.config.adaptive_compression { 3 } else { 0 };
            Some(AdaptiveCompressor::new(
                self.config.compression_level,
                sample_size,
            ))
        } else {
            None
        };

        for chunk_index in 0..total_chunks {
            if completed_chunks.contains(&chunk_index) {
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

    /// Receive a file from the peer. `total_chunks` comes from the
    /// preceding `TransferInfo` message; we read exactly that many uni
    /// streams. After all chunks land, re-read the file from disk to
    /// compute its SHA-256.
    pub async fn receive_file(
        &mut self,
        output_path: &Path,
        total_chunks: u64,
        mut chunk_complete_callback: Option<impl FnMut(u64)>,
        mut progress: Option<&mut ProgressState>,
    ) -> Result<[u8; 32]> {
        debug!(
            "Starting file receive: {:?} ({} chunks expected)",
            output_path, total_chunks
        );

        let mut writer = ChunkWriter::new(output_path, self.config.chunk_size as usize).await?;
        let mut decompressor: Option<Decompressor> = if self.config.compression_enabled {
            Some(Decompressor::new())
        } else {
            None
        };

        let mut received: u64 = 0;
        while received < total_chunks {
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
            let flags = raw[8];
            let payload = &raw[CHUNK_HEADER_BYTES..];

            let final_data = if flags & FLAG_COMPRESSED != 0 {
                let decomp = decompressor.as_mut().ok_or_else(|| {
                    Error::Protocol(
                        "compressed chunk but compression disabled in config".to_string(),
                    )
                })?;
                decomp.decompress(payload)?
            } else {
                payload.to_vec()
            };

            let written = final_data.len() as u64;
            writer.write_chunk(chunk_index, &final_data).await?;
            received += 1;

            if let Some(ref mut p) = progress {
                p.add_bytes(written);
            }
            if let Some(ref mut cb) = chunk_complete_callback {
                cb(chunk_index);
            }

            trace!("Received chunk {} ({}/{})", chunk_index, received, total_chunks);
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
        stream
            .write_all(&chunk_index.to_le_bytes())
            .await
            .map_err(|e| Error::Quic(format!("write index: {e}")))?;
        let flags: u8 = if compressed { FLAG_COMPRESSED } else { 0 };
        stream
            .write_all(&[flags])
            .await
            .map_err(|e| Error::Quic(format!("write flags: {e}")))?;
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
        let total_chunks = (file_size + chunk_size as u64 - 1) / chunk_size as u64;
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

    pub fn file_size(&self) -> u64 {
        self.file_size
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
    pub async fn new(path: &Path, chunk_size: usize) -> Result<Self> {
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

        Ok(Self {
            file,
            path: path.to_path_buf(),
            chunk_size,
        })
    }

    pub async fn write_chunk(&mut self, index: u64, data: &[u8]) -> Result<()> {
        let offset = index * self.chunk_size as u64;
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(data).await?;
        self.file.flush().await?;
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
        let mut buf = vec![0u8; 64 * 1024];
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
    use tempfile::tempdir;

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
        let mut writer = ChunkWriter::new(&p, 64).await.unwrap();

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
