//! Compression utilities using Zstandard

use crate::error::{Error, Result};
use tracing::info;

/// Compress data using Zstandard
pub fn compress(data: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::encode_all(data, level).map_err(|e| Error::Compression(e.to_string()))
}

/// Decompress data using Zstandard
pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| Error::Decompression(e.to_string()))
}

/// Streaming compressor
pub struct Compressor {
    level: i32,
}

impl Compressor {
    /// Create a new compressor with the given level
    pub fn new(level: i32) -> Self {
        Self { level }
    }

    /// Compress a chunk of data
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        compress(data, self.level)
    }
}

/// Adaptive compressor that automatically disables compression for pre-compressed files
///
/// Samples the first few chunks to determine if compression is beneficial.
/// If the compression ratio is below a threshold, compression is disabled.
pub struct AdaptiveCompressor {
    compressor: Compressor,
    sample_size: usize,
    threshold: f64,
    chunks_sampled: usize,
    total_original_size: usize,
    total_compressed_size: usize,
    compression_enabled: bool,
    compression_decided: bool,
}

impl AdaptiveCompressor {
    /// Create a new adaptive compressor
    ///
    /// # Arguments
    /// * `level` - Compression level (1-22)
    /// * `sample_size` - Number of chunks to sample before deciding (0 = always compress, no adaptation)
    ///
    /// When `sample_size > 0` (adaptive mode):
    /// - Samples first `sample_size` chunks
    /// - Uses default threshold (1.05) compression ratio to decide
    /// - Automatically disables compression if data is already compressed
    ///
    /// When `sample_size == 0` (always compress mode):
    /// - Never adapts, always compresses every chunk
    /// - Behaves like a standard Compressor
    pub fn new(level: i32, sample_size: usize) -> Self {
        Self {
            compressor: Compressor::new(level),
            sample_size,
            compression_decided: sample_size == 0,
            ..Default::default()
        }
    }

    /// Compress a chunk with adaptive logic
    ///
    /// Returns (compressed_data, compression_was_used, compression_decision_changed)
    pub fn compress(&mut self, data: &[u8]) -> Result<(Vec<u8>, bool, bool)> {
        let original_size = data.len();

        // If compression is already decided and disabled, return original data
        if self.compression_decided && !self.compression_enabled {
            return Ok((data.to_vec(), false, false));
        }

        // Try compressing
        let compressed = self.compressor.compress(data)?;
        let compressed_size = compressed.len();

        // If still in sampling phase, collect statistics
        if !self.compression_decided {
            self.chunks_sampled += 1;
            self.total_original_size += original_size;
            self.total_compressed_size += compressed_size;

            // After sampling enough chunks, make a decision
            if self.chunks_sampled >= self.sample_size {
                self.compression_decided = true;

                // Calculate compression ratio: original / compressed
                let ratio = self.total_original_size as f64 / self.total_compressed_size as f64;

                let decision_changed = if ratio < self.threshold {
                    // Compression not beneficial, disable it
                    self.compression_enabled = false;
                    info!(
                        "Adaptive compression: DISABLED (ratio: {:.2}, threshold: {:.2})",
                        ratio, self.threshold
                    );
                    true
                } else {
                    // Compression is beneficial, keep it enabled
                    info!(
                        "Adaptive compression: ENABLED (ratio: {:.2}, threshold: {:.2})",
                        ratio, self.threshold
                    );
                    false
                };

                // Note: Current chunk is always compressed during sampling phase
                return Ok((compressed, true, decision_changed));
            }
        }

        // Return compressed data
        Ok((compressed, true, false))
    }

    /// Check if compression is currently enabled
    pub fn is_compression_enabled(&self) -> bool {
        self.compression_enabled
    }

    /// Check if compression decision has been made
    pub fn is_decided(&self) -> bool {
        self.compression_decided
    }

    /// Get compression statistics
    pub fn stats(&self) -> CompressionStats {
        let ratio = if self.total_compressed_size > 0 {
            self.total_original_size as f64 / self.total_compressed_size as f64
        } else {
            1.0
        };

        CompressionStats {
            chunks_sampled: self.chunks_sampled,
            original_size: self.total_original_size,
            compressed_size: self.total_compressed_size,
            ratio,
            enabled: self.compression_enabled,
            decided: self.compression_decided,
        }
    }
}

impl Default for AdaptiveCompressor {
    fn default() -> Self {
        Self {
            compressor: Compressor::new(3),
            sample_size: 3,
            threshold: 1.05,
            chunks_sampled: 0,
            total_original_size: 0,
            total_compressed_size: 0,
            compression_enabled: true,
            compression_decided: false,
        }
    }
}

/// Compression statistics
#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub chunks_sampled: usize,
    pub original_size: usize,
    pub compressed_size: usize,
    pub ratio: f64,
    pub enabled: bool,
    pub decided: bool,
}

/// Streaming decompressor
pub struct Decompressor;

impl Decompressor {
    /// Create a new decompressor
    pub fn new() -> Self {
        Self
    }

    /// Decompress a chunk of data
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        decompress(data)
    }
}

impl Default for Decompressor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress() {
        // Use larger, more compressible data
        let data = b"Hello, World! This is a test of zstd compression. ".repeat(100);
        let compressed = compress(&data, 3).unwrap();
        // With repetitive data, compression should work
        assert!(compressed.len() < data.len());

        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_compressor() {
        let compressor = Compressor::new(5);
        let data = b"Test data for compression";
        let compressed = compressor.compress(data).unwrap();

        let decompressor = Decompressor::new();
        let decompressed = decompressor.decompress(&compressed).unwrap();
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_adaptive_compression_compressible_data() {
        // Highly compressible data (repetitive)
        let chunk1 = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".repeat(100);
        let chunk2 = b"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".repeat(100);
        let chunk3 = b"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC".repeat(100);

        let mut compressor = AdaptiveCompressor::default();

        // First chunk - still sampling
        let (compressed1, used1, changed1) = compressor.compress(&chunk1).unwrap();
        assert!(used1, "Should compress during sampling");
        assert!(!changed1, "Should not change decision yet");
        assert!(!compressor.is_decided(), "Should not be decided yet");

        // Second chunk - still sampling
        let (compressed2, used2, changed2) = compressor.compress(&chunk2).unwrap();
        assert!(used2, "Should compress during sampling");
        assert!(!changed2, "Should not change decision yet");

        // Third chunk - decision made
        let (compressed3, used3, changed3) = compressor.compress(&chunk3).unwrap();
        assert!(used3, "Should compress this chunk");
        assert!(!changed3, "Should keep compression enabled");
        assert!(compressor.is_decided(), "Should be decided now");
        assert!(
            compressor.is_compression_enabled(),
            "Compression should stay enabled"
        );

        // Verify compression was effective
        assert!(compressed1.len() < chunk1.len());
        assert!(compressed2.len() < chunk2.len());
        assert!(compressed3.len() < chunk3.len());

        // Fourth chunk - uses decided setting
        let chunk4 = b"DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD".repeat(100);
        let (_compressed4, used4, changed4) = compressor.compress(&chunk4).unwrap();
        assert!(used4, "Should still compress");
        assert!(!changed4, "Decision already made");
    }

    #[test]
    fn test_adaptive_compression_incompressible_data() {
        // Simulate already-compressed data by actually compressing random data first
        // This creates high-entropy data that won't compress further
        let random_data = b"Random text that will be pre-compressed ".repeat(100);
        let pre_compressed = compress(&random_data, 3).unwrap();

        // Split into 3 chunks for sampling
        let chunk_size = pre_compressed.len() / 3;
        let chunk1 = &pre_compressed[0..chunk_size];
        let chunk2 = &pre_compressed[chunk_size..chunk_size * 2];
        let chunk3 = &pre_compressed[chunk_size * 2..];

        let mut compressor = AdaptiveCompressor::default();

        // Compress sampling chunks (these are already compressed, so won't compress well)
        compressor.compress(chunk1).unwrap();
        compressor.compress(chunk2).unwrap();
        let (_, _, changed) = compressor.compress(chunk3).unwrap();

        // Should decide to disable compression (already compressed data doesn't compress further)
        assert!(changed, "Should change decision to disable");
        assert!(compressor.is_decided(), "Should be decided");
        assert!(
            !compressor.is_compression_enabled(),
            "Should disable compression"
        );

        // Subsequent chunks should not be compressed
        let chunk4 = &pre_compressed[0..chunk_size];
        let (result, used, _) = compressor.compress(chunk4).unwrap();
        assert!(!used, "Should not compress");
        assert_eq!(result, chunk4, "Should return original data");
    }

    #[test]
    fn test_adaptive_compression_stats() {
        let chunk = b"Test data ".repeat(100);
        let mut compressor = AdaptiveCompressor::default();

        compressor.compress(&chunk).unwrap();
        compressor.compress(&chunk).unwrap();
        compressor.compress(&chunk).unwrap();

        let stats = compressor.stats();
        assert_eq!(stats.chunks_sampled, 3);
        assert!(stats.decided);
        assert_eq!(stats.original_size, chunk.len() * 3);
        assert!(stats.ratio > 1.0, "Should have positive compression ratio");
    }
}
