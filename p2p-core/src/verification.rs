//! File-level SHA-256 verification.
//!
//! Per-chunk CRC32 is gone in the QUIC rewrite: TLS 1.3 AEAD already
//! authenticates every byte that lands on the wire, so an additional
//! CRC would only catch local memory corruption — not a meaningful
//! threat model for this app. SHA-256 over the whole file remains as a
//! single end-to-end integrity check both sides exchange after the
//! transfer.

use crate::error::{Error, Result};
use sha2::{Digest, Sha256};

/// Hash a byte slice with SHA-256.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Verify a byte slice matches an expected SHA-256.
pub fn verify_sha256(data: &[u8], expected: &[u8; 32]) -> Result<()> {
    let actual = sha256(data);
    if &actual == expected {
        Ok(())
    } else {
        Err(Error::Verification(format!(
            "SHA256 mismatch: expected {}, got {}",
            hex::encode(expected),
            hex::encode(actual),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_roundtrip() {
        let data = b"Hello, World!";
        let checksum = sha256(data);
        assert!(verify_sha256(data, &checksum).is_ok());

        let mut wrong = checksum;
        wrong[0] ^= 1;
        assert!(verify_sha256(data, &wrong).is_err());
    }
}
