//! Auto-reconnect functionality with exponential backoff

use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

/// Configuration for auto-reconnect behavior
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Maximum number of reconnection attempts (0 = infinite)
    pub max_attempts: u32,
    /// Initial backoff delay in seconds
    pub initial_backoff_secs: u64,
    /// Maximum backoff delay in seconds
    pub max_backoff_secs: u64,
    /// Whether to use exponential backoff
    pub exponential: bool,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff_secs: 3,
            max_backoff_secs: 180,
            exponential: true,
        }
    }
}

impl ReconnectConfig {
    /// Create config with unlimited retries
    pub fn unlimited() -> Self {
        Self {
            max_attempts: 0,
            ..Default::default()
        }
    }

    /// Create config with specific max attempts
    pub fn with_max_attempts(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            ..Default::default()
        }
    }

    /// Calculate backoff delay for given attempt number
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        let delay_secs = if self.exponential {
            // Exponential: 3, 6, 12, 24, 48, 60, 60, ...
            let exp_delay = self.initial_backoff_secs * 2u64.pow(attempt);
            exp_delay.min(self.max_backoff_secs)
        } else {
            // Linear: 3, 3, 3, 3, ...
            self.initial_backoff_secs
        };

        Duration::from_secs(delay_secs)
    }

    /// Check if should retry after the given attempt number.
    ///
    /// Note: `attempt` is 0-indexed and represents the attempt that just failed.
    /// This is called BEFORE incrementing to check if we should try again.
    ///
    /// `max_attempts` determines the maximum number of retries (not total attempts):
    /// - 0 = unlimited retries
    /// - 1 = allow 1 retry (2 total attempts)
    /// - 2 = allow 2 retries (3 total attempts)
    pub fn should_retry(&self, attempt: u32) -> bool {
        self.max_attempts == 0 || (attempt + 1) < self.max_attempts
    }
}

/// Result of a reconnection attempt
#[derive(Debug)]
pub enum ReconnectResult<T> {
    /// Operation succeeded
    Success(T),
    /// Operation failed but can retry
    Retry { attempt: u32, error: String },
    /// Operation failed permanently
    Failed { attempts: u32, last_error: String },
}

/// Retry a fallible async operation with automatic reconnection
pub async fn retry_with_backoff<F, Fut, T, E>(
    operation: F,
    config: &ReconnectConfig,
    operation_name: &str,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;

    loop {
        match operation().await {
            Ok(result) => {
                if attempt > 0 {
                    info!(
                        "{} succeeded after {} attempts",
                        operation_name,
                        attempt + 1
                    );
                }
                return Ok(result);
            }
            Err(e) => {
                // Check if we can retry BEFORE incrementing attempt
                if !config.should_retry(attempt) {
                    error!(
                        "{} failed after {} attempts: {}",
                        operation_name,
                        attempt + 1,
                        e
                    );
                    return Err(e);
                }

                let delay = config.backoff_delay(attempt);
                attempt += 1;

                warn!(
                    "{} failed (attempt {}/{}): {}. Retrying in {:?}...",
                    operation_name,
                    attempt,
                    if config.max_attempts == 0 {
                        "∞".to_string()
                    } else {
                        config.max_attempts.to_string()
                    },
                    e,
                    delay
                );

                sleep(delay).await;
            }
        }
    }
}

// `is_transient_error` removed: callers should use `Error::is_recoverable()`,
// which now covers all the QUIC-era transport error variants in one place.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_delay_exponential() {
        let config = ReconnectConfig::default();

        // Default initial_backoff_secs = 3, max_backoff_secs = 180
        assert_eq!(config.backoff_delay(0), Duration::from_secs(3)); // 3 * 2^0 = 3
        assert_eq!(config.backoff_delay(1), Duration::from_secs(6)); // 3 * 2^1 = 6
        assert_eq!(config.backoff_delay(2), Duration::from_secs(12)); // 3 * 2^2 = 12
        assert_eq!(config.backoff_delay(3), Duration::from_secs(24)); // 3 * 2^3 = 24
        assert_eq!(config.backoff_delay(4), Duration::from_secs(48)); // 3 * 2^4 = 48
        assert_eq!(config.backoff_delay(5), Duration::from_secs(96)); // 3 * 2^5 = 96
        assert_eq!(config.backoff_delay(6), Duration::from_secs(180)); // 3 * 2^6 = 192, capped at 180
        assert_eq!(config.backoff_delay(10), Duration::from_secs(180)); // Still capped
    }

    #[test]
    fn test_backoff_delay_linear() {
        let config = ReconnectConfig {
            exponential: false,
            ..Default::default()
        };

        // Default initial_backoff_secs = 3 (constant for linear)
        assert_eq!(config.backoff_delay(0), Duration::from_secs(3));
        assert_eq!(config.backoff_delay(1), Duration::from_secs(3));
        assert_eq!(config.backoff_delay(5), Duration::from_secs(3));
    }

    #[test]
    fn test_should_retry() {
        // max_attempts = 3 means allow up to 2 retries (3 total attempts)
        let config = ReconnectConfig::with_max_attempts(3);

        // attempt 0: first try fails, should_retry(0) -> (0+1) < 3 = true (retry #1)
        // attempt 1: second try fails, should_retry(1) -> (1+1) < 3 = true (retry #2)
        // attempt 2: third try fails, should_retry(2) -> (2+1) < 3 = false (no more retries)
        assert!(config.should_retry(0));
        assert!(config.should_retry(1));
        assert!(!config.should_retry(2));
        assert!(!config.should_retry(3));
        assert!(!config.should_retry(10));
    }

    #[test]
    fn test_should_retry_unlimited() {
        let config = ReconnectConfig::unlimited();

        assert!(config.should_retry(0));
        assert!(config.should_retry(100));
        assert!(config.should_retry(1000));
    }

    #[test]
    fn test_default_caps_at_5_attempts() {
        let config = ReconnectConfig::default();
        assert_eq!(config.max_attempts, 5);
        assert!(config.should_retry(0));
        assert!(config.should_retry(3));
        assert!(!config.should_retry(4));
        assert!(!config.should_retry(10));
    }

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let config = ReconnectConfig::with_max_attempts(3);
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(
            || async {
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<i32, String>(42)
            },
            &config,
            "test_operation",
        )
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let config = ReconnectConfig {
            initial_backoff_secs: 0, // No delay for test
            max_attempts: 5,
            ..Default::default()
        };
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(
            || async {
                let count = counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count < 2 {
                    Err("Simulated failure".to_string())
                } else {
                    Ok::<i32, String>(42)
                }
            },
            &config,
            "test_operation",
        )
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        // max_attempts = 2 means 1 original attempt + 1 retry = 2 total attempts
        let config = ReconnectConfig {
            initial_backoff_secs: 0, // No delay for test
            max_attempts: 2,
            ..Default::default()
        };
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry_with_backoff(
            || async {
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err::<i32, String>("Persistent failure".to_string())
            },
            &config,
            "test_operation",
        )
        .await;

        assert!(result.is_err());
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
