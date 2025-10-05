//! Checksum verification

use crate::error::{Error, Result};
use sha2::{Digest, Sha256};

/// Calculate CRC32 checksum
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Calculate SHA256 checksum
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Verify CRC32 checksum
pub fn verify_crc32(data: &[u8], expected: u32) -> Result<()> {
    let actual = crc32(data);
    if actual == expected {
        Ok(())
    } else {
        Err(Error::Verification(format!(
            "CRC32 mismatch: expected {}, got {}",
            expected, actual
        )))
    }
}

/// Verify SHA256 checksum
pub fn verify_sha256(data: &[u8], expected: &[u8; 32]) -> Result<()> {
    let actual = sha256(data);
    if &actual == expected {
        Ok(())
    } else {
        Err(Error::Verification(format!(
            "SHA256 mismatch: expected {:?}, got {:?}",
            expected, actual
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32() {
        let data = b"Hello, World!";
        let checksum = crc32(data);
        assert!(verify_crc32(data, checksum).is_ok());
        assert!(verify_crc32(data, checksum + 1).is_err());
    }

    #[test]
    fn test_sha256() {
        let data = b"Hello, World!";
        let checksum = sha256(data);
        assert!(verify_sha256(data, &checksum).is_ok());

        let mut wrong = checksum;
        wrong[0] ^= 1;
        assert!(verify_sha256(data, &wrong).is_err());
    }
}
