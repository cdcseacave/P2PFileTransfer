//! Compression utilities using Zstandard

use crate::error::{Error, Result};

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
}
