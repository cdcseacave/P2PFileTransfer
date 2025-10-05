//! Bandwidth throttling using token bucket algorithm
//!
//! This module provides rate limiting for network transfers to prevent
//! congestion and allow fair bandwidth sharing.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;

/// Bandwidth limiter using token bucket algorithm
///
/// The token bucket algorithm allows for burst traffic while maintaining
/// an average rate. Tokens are added at a fixed rate, and each byte sent
/// consumes one token.
#[derive(Clone)]
pub struct BandwidthLimiter {
    /// Maximum bytes per second (0 = unlimited)
    max_bytes_per_sec: u64,
    /// Token bucket state
    bucket: Arc<Mutex<TokenBucket>>,
}

/// Token bucket state
struct TokenBucket {
    /// Current number of tokens available
    tokens: f64,
    /// Maximum bucket capacity (allows bursts)
    capacity: f64,
    /// Rate at which tokens are added (bytes per second)
    refill_rate: f64,
    /// Last refill timestamp
    last_refill: Instant,
}

impl BandwidthLimiter {
    /// Create a new bandwidth limiter
    ///
    /// # Arguments
    /// * `max_bytes_per_sec` - Maximum transfer rate in bytes per second (0 = unlimited)
    ///
    /// # Examples
    /// ```
    /// use p2p_core::bandwidth::BandwidthLimiter;
    ///
    /// // Limit to 10 MB/s
    /// let limiter = BandwidthLimiter::new(10 * 1024 * 1024);
    ///
    /// // Unlimited
    /// let unlimited = BandwidthLimiter::new(0);
    /// ```
    pub fn new(max_bytes_per_sec: u64) -> Self {
        let capacity = if max_bytes_per_sec > 0 {
            // Allow burst of 2 seconds worth of data
            (max_bytes_per_sec * 2) as f64
        } else {
            0.0
        };

        Self {
            max_bytes_per_sec,
            bucket: Arc::new(Mutex::new(TokenBucket {
                tokens: capacity,
                capacity,
                refill_rate: max_bytes_per_sec as f64,
                last_refill: Instant::now(),
            })),
        }
    }

    /// Create an unlimited bandwidth limiter (no throttling)
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Check if throttling is enabled
    pub fn is_enabled(&self) -> bool {
        self.max_bytes_per_sec > 0
    }

    /// Get the configured max bytes per second
    pub fn max_bytes_per_sec(&self) -> u64 {
        self.max_bytes_per_sec
    }

    /// Wait for enough tokens to send the specified number of bytes
    ///
    /// This method will sleep until enough tokens are available.
    /// If throttling is disabled (max_bytes_per_sec = 0), returns immediately.
    ///
    /// # Arguments
    /// * `bytes` - Number of bytes to send
    pub async fn wait_for_tokens(&self, bytes: usize) {
        if !self.is_enabled() {
            return;
        }

        let bytes = bytes as f64;
        let mut bucket = self.bucket.lock().await;

        loop {
            // Refill tokens based on elapsed time
            let now = Instant::now();
            let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
            let new_tokens = elapsed * bucket.refill_rate;
            
            bucket.tokens = (bucket.tokens + new_tokens).min(bucket.capacity);
            bucket.last_refill = now;

            // Check if we have enough tokens
            if bucket.tokens >= bytes {
                bucket.tokens -= bytes;
                break;
            }

            // Calculate how long to wait for enough tokens
            let tokens_needed = bytes - bucket.tokens;
            let wait_time = tokens_needed / bucket.refill_rate;
            let wait_duration = Duration::from_secs_f64(wait_time);

            // Release lock while sleeping
            drop(bucket);
            sleep(wait_duration).await;
            bucket = self.bucket.lock().await;
        }
    }

    /// Get current statistics
    pub async fn stats(&self) -> BandwidthStats {
        if !self.is_enabled() {
            return BandwidthStats {
                max_bytes_per_sec: 0,
                available_tokens: 0.0,
                capacity: 0.0,
            };
        }

        let bucket = self.bucket.lock().await;
        BandwidthStats {
            max_bytes_per_sec: self.max_bytes_per_sec,
            available_tokens: bucket.tokens,
            capacity: bucket.capacity,
        }
    }
}

/// Bandwidth limiter statistics
#[derive(Debug, Clone)]
pub struct BandwidthStats {
    /// Maximum bytes per second
    pub max_bytes_per_sec: u64,
    /// Currently available tokens
    pub available_tokens: f64,
    /// Maximum bucket capacity
    pub capacity: f64,
}

/// Parse a human-readable bandwidth string into bytes per second
///
/// # Supported formats:
/// - `"10M"` or `"10MB"` = 10 megabytes/sec
/// - `"1G"` or `"1GB"` = 1 gigabyte/sec
/// - `"512K"` or `"512KB"` = 512 kilobytes/sec
/// - `"1000000"` = 1000000 bytes/sec
/// - `"unlimited"` or `"0"` = no limit
///
/// # Examples
/// ```
/// use p2p_core::bandwidth::parse_bandwidth;
///
/// assert_eq!(parse_bandwidth("10M").unwrap(), 10 * 1024 * 1024);
/// assert_eq!(parse_bandwidth("1G").unwrap(), 1024 * 1024 * 1024);
/// assert_eq!(parse_bandwidth("512K").unwrap(), 512 * 1024);
/// assert_eq!(parse_bandwidth("unlimited").unwrap(), 0);
/// ```
pub fn parse_bandwidth(s: &str) -> Result<u64, String> {
    let s = s.trim().to_lowercase();

    if s == "unlimited" || s == "0" {
        return Ok(0);
    }

    // Try to parse as plain number first
    if let Ok(bytes) = s.parse::<u64>() {
        return Ok(bytes);
    }

    // Parse with unit suffix
    let (num_str, multiplier) = if s.ends_with("gb") || s.ends_with('g') {
        let num_str = s.trim_end_matches("gb").trim_end_matches('g');
        (num_str, 1024u64 * 1024 * 1024)
    } else if s.ends_with("mb") || s.ends_with('m') {
        let num_str = s.trim_end_matches("mb").trim_end_matches('m');
        (num_str, 1024u64 * 1024)
    } else if s.ends_with("kb") || s.ends_with('k') {
        let num_str = s.trim_end_matches("kb").trim_end_matches('k');
        (num_str, 1024u64)
    } else {
        return Err(format!("Invalid bandwidth format: {}. Use K, M, or G suffix (e.g., '10M', '1G')", s));
    };

    let num = num_str
        .parse::<f64>()
        .map_err(|_| format!("Invalid number in bandwidth: {}", s))?;

    if num < 0.0 {
        return Err("Bandwidth cannot be negative".to_string());
    }

    Ok((num * multiplier as f64) as u64)
}

/// Format bytes per second as human-readable string
///
/// # Examples
/// ```
/// use p2p_core::bandwidth::format_bandwidth;
///
/// assert_eq!(format_bandwidth(10 * 1024 * 1024), "10.00 MB/s");
/// assert_eq!(format_bandwidth(1024 * 1024 * 1024), "1.00 GB/s");
/// assert_eq!(format_bandwidth(512 * 1024), "512.00 KB/s");
/// assert_eq!(format_bandwidth(0), "unlimited");
/// ```
pub fn format_bandwidth(bytes_per_sec: u64) -> String {
    if bytes_per_sec == 0 {
        return "unlimited".to_string();
    }

    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    const KB: u64 = 1024;

    if bytes_per_sec >= GB {
        format!("{:.2} GB/s", bytes_per_sec as f64 / GB as f64)
    } else if bytes_per_sec >= MB {
        format!("{:.2} MB/s", bytes_per_sec as f64 / MB as f64)
    } else if bytes_per_sec >= KB {
        format!("{:.2} KB/s", bytes_per_sec as f64 / KB as f64)
    } else {
        format!("{} B/s", bytes_per_sec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[test]
    fn test_parse_bandwidth() {
        assert_eq!(parse_bandwidth("10M").unwrap(), 10 * 1024 * 1024);
        assert_eq!(parse_bandwidth("10MB").unwrap(), 10 * 1024 * 1024);
        assert_eq!(parse_bandwidth("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bandwidth("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bandwidth("512K").unwrap(), 512 * 1024);
        assert_eq!(parse_bandwidth("512KB").unwrap(), 512 * 1024);
        assert_eq!(parse_bandwidth("1000000").unwrap(), 1000000);
        assert_eq!(parse_bandwidth("unlimited").unwrap(), 0);
        assert_eq!(parse_bandwidth("0").unwrap(), 0);
    }

    #[test]
    fn test_format_bandwidth() {
        assert_eq!(format_bandwidth(10 * 1024 * 1024), "10.00 MB/s");
        assert_eq!(format_bandwidth(1024 * 1024 * 1024), "1.00 GB/s");
        assert_eq!(format_bandwidth(512 * 1024), "512.00 KB/s");
        assert_eq!(format_bandwidth(1000), "1000 B/s");
        assert_eq!(format_bandwidth(0), "unlimited");
    }

    #[tokio::test]
    async fn test_unlimited_limiter() {
        let limiter = BandwidthLimiter::unlimited();
        assert!(!limiter.is_enabled());

        // Should return immediately
        let start = Instant::now();
        limiter.wait_for_tokens(1024 * 1024).await;
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() < 10);
    }

    #[tokio::test]
    async fn test_bandwidth_limiting() {
        // Limit to 1 MB/s
        let limiter = BandwidthLimiter::new(1024 * 1024);
        assert!(limiter.is_enabled());

        let start = Instant::now();
        
        // First 2 MB should be fast (burst capacity allows it)
        limiter.wait_for_tokens(1024 * 1024).await;
        limiter.wait_for_tokens(1024 * 1024).await;
        
        let burst_time = start.elapsed();
        // Burst should be fast (within 200ms due to test overhead)
        assert!(burst_time.as_millis() < 200, "Burst was too slow: {:?}", burst_time);

        // Next transfer after burst should wait
        let wait_start = Instant::now();
        limiter.wait_for_tokens(1024 * 1024).await;
        let wait_time = wait_start.elapsed();

        // Should have waited at least 800ms (with tolerance for test overhead)
        assert!(wait_time.as_millis() >= 800, "Didn't wait long enough: {:?}", wait_time);
    }

    #[tokio::test]
    async fn test_burst_allowance() {
        // Limit to 1 MB/s with 2 second burst
        let limiter = BandwidthLimiter::new(1024 * 1024);

        let start = Instant::now();
        
        // First burst should be immediate (up to 2 MB)
        limiter.wait_for_tokens(1024 * 1024).await;
        limiter.wait_for_tokens(1024 * 1024).await;
        
        let burst_time = start.elapsed();

        // Burst should be fast
        assert!(burst_time.as_millis() < 100, "Burst was too slow: {:?}", burst_time);

        // Next transfer should wait
        let wait_start = Instant::now();
        limiter.wait_for_tokens(512 * 1024).await;
        let wait_time = wait_start.elapsed();

        // Should have waited
        assert!(wait_time.as_millis() >= 400, "Didn't wait enough: {:?}", wait_time);
    }
}
