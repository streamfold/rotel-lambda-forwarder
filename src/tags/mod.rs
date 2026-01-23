mod cache;
mod cloudwatch;

pub use cache::{CacheEntry, TagCache};
pub use cloudwatch::{CloudWatchError, CloudWatchTagFetcher};

use crate::s3_cache::{S3Cache, S3CacheError};
use aws_sdk_cloudwatchlogs::Client as CloudWatchLogsClient;
use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, error, info, warn};

/// S3 cache key for storing tags
const CACHE_KEY: &str = "rotel-lambda-forwarder/cache/log-groups/tags.json.gz";

/// Serializable format for the cache file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheFile {
    /// Version of the cache file format
    pub version: u32,
    /// Map of log group names to their cache entries (with timestamps)
    pub log_groups: HashMap<String, CacheEntry>,
}

impl CacheFile {
    /// Create a new cache file
    pub fn new(log_groups: HashMap<String, CacheEntry>) -> Self {
        Self {
            version: 1,
            log_groups,
        }
    }
}

/// Errors that can occur during tag operations
#[derive(Debug, Error)]
pub enum TagError {
    #[error("CloudWatch error: {0}")]
    CloudWatch(#[from] CloudWatchError),

    #[error("S3 error: {0}")]
    S3(#[from] S3CacheError),

    #[error("No S3 bucket configured")]
    NoS3Bucket,
}

/// Main tag manager that coordinates cache, S3, and CloudWatch operations
///
/// The `TagManager` provides automatic caching and persistence of CloudWatch log group tags.
/// It fetches tags from CloudWatch Logs API on first access and caches them in memory with
/// a 15-minute TTL. Optionally, tags can be persisted to S3 for durability across Lambda
/// cold starts.
///
pub struct TagManager {
    /// In-memory cache
    cache: TagCache,
    /// S3 persistence layer (optional)
    s3_cache: Option<S3Cache<CacheFile>>,
    /// CloudWatch client for fetching tags
    cw_fetcher: CloudWatchTagFetcher,
    /// Whether to persist to S3
    persist_enabled: bool,
    /// Circuit breaker: timestamp when tag fetching was disabled due to AccessDenied
    tag_fetch_disabled_until: Option<Instant>,
    /// Circuit breaker cooldown period (30 minutes)
    cooldown_duration: Duration,
}

impl TagManager {
    pub fn new(
        cw_client: CloudWatchLogsClient,
        s3_client: Option<S3Client>,
        s3_bucket: Option<String>,
    ) -> Self {
        let persist_enabled = s3_client.is_some() && s3_bucket.is_some();

        let s3_cache = match (s3_client, s3_bucket) {
            (Some(client), Some(bucket)) => {
                Some(S3Cache::new(client, bucket, CACHE_KEY.to_string()))
            }
            _ => None,
        };

        Self {
            cache: TagCache::new(),
            s3_cache,
            cw_fetcher: CloudWatchTagFetcher::new(cw_client),
            persist_enabled,
            tag_fetch_disabled_until: None,
            cooldown_duration: Duration::from_secs(30 * 60), // 30 minutes
        }
    }

    /// Initialize the tag manager by loading the cache from S3
    ///
    /// This should be called once during startup to restore the cache from S3.
    /// If no cache exists in S3 or S3 is not configured, starts with an empty cache.
    /// Errors loading the cache are logged but do not fail initialization.
    ///
    pub async fn initialize(&mut self) -> Result<(), TagError> {
        if let Some(s3_cache) = &mut self.s3_cache {
            match s3_cache.load().await {
                Ok(Some(cache_file)) => {
                    info!(
                        entry_count = cache_file.log_groups.len(),
                        "Loaded cache from S3"
                    );
                    self.cache.load_snapshot(cache_file.log_groups);
                }
                Ok(None) => {
                    warn!("No existing cache found in S3");
                }
                Err(e) => {
                    error!(error = %e, "Failed to load cache from S3, starting with empty cache");
                    // Don't fail initialization, just start with empty cache
                }
            }
        }
        Ok(())
    }

    /// Get tags for a log group
    ///
    /// First checks the in-memory cache. If not found or expired, fetches from CloudWatch
    /// Logs API and updates both the in-memory cache and S3 (if enabled).
    ///
    pub async fn get_tags(
        &mut self,
        log_group_name: &str,
        region: &str,
        account_id: &str,
    ) -> Result<HashMap<String, String>, TagError> {
        // Check cache first
        if let Some(tags) = self.cache.get(log_group_name) {
            debug!(log_group = %log_group_name, "Tags found in cache");
            return Ok(tags);
        }

        // Check if tag fetching is currently disabled due to AccessDenied
        if let Some(disabled_until) = self.tag_fetch_disabled_until {
            let now = Instant::now();
            if now < disabled_until {
                let remaining = disabled_until.duration_since(now);
                warn!(
                    log_group = %log_group_name,
                    remaining_seconds = remaining.as_secs(),
                    "Tag fetching is disabled due to AccessDenied, returning empty tags"
                );
                return Ok(HashMap::new());
            } else {
                // Cooldown period has elapsed, re-enable tag fetching
                info!(
                    log_group = %log_group_name,
                    "Cooldown period elapsed, attempting to fetch tags again"
                );
                self.tag_fetch_disabled_until = None;
            }
        }

        // Not in cache, fetch from CloudWatch
        debug!(log_group = %log_group_name, "Tags not in cache, fetching from CloudWatch");

        let log_group_arn =
            CloudWatchTagFetcher::build_log_group_arn(region, account_id, log_group_name);

        let tags = match self.cw_fetcher.fetch_tags(&log_group_arn).await {
            Ok(tags) => tags,
            Err(CloudWatchError::AccessDenied(_)) => {
                // Disable tag fetching for the next 30 minutes
                let disabled_until = Instant::now() + self.cooldown_duration;
                self.tag_fetch_disabled_until = Some(disabled_until);

                warn!(
                    log_group = %log_group_name,
                    cooldown_minutes = self.cooldown_duration.as_secs() / 60,
                    "AccessDenied error, disabling tag fetching for all log groups"
                );

                // Return empty tags instead of propagating the error
                return Ok(HashMap::new());
            }
            Err(e) => {
                // For other errors, propagate them normally
                return Err(TagError::CloudWatch(e));
            }
        };

        // Update cache
        self.cache.insert(log_group_name.to_string(), tags.clone());

        // Persist to S3 if enabled
        if self.persist_enabled {
            if let Err(e) = self.persist_cache().await {
                error!(error = %e, "Failed to persist cache to S3 after fetching tags");
                // Don't fail the request, just log the error
            }
        }

        Ok(tags)
    }

    /// Persist the current cache to S3
    async fn persist_cache(&mut self) -> Result<(), TagError> {
        if let Some(s3_cache) = &mut self.s3_cache {
            let snapshot = self.cache.get_snapshot();
            let cache_file = CacheFile::new(snapshot);

            match s3_cache.save(cache_file).await {
                Ok(_) => {
                    debug!("Successfully persisted cache to S3");
                    Ok(())
                }
                Err(S3CacheError::ConditionalWriteFailed) => {
                    warn!("Conditional write failed, reloading and merging cache");

                    // Reload from S3 and merge with our current snapshot
                    let local_snapshot = self.cache.get_snapshot();
                    let local_cache_file = CacheFile::new(local_snapshot);

                    let merged_cache_file = s3_cache
                        .reload_and_merge(local_cache_file, |from_s3, local| {
                            // Merge the log groups, keeping the most recent entry for each
                            let mut merged = from_s3.log_groups;
                            for (log_group, new_entry) in local.log_groups {
                                let should_update = if let Some(existing_entry) = merged.get(&log_group) {
                                    // Keep the entry with the most recent last_seen time
                                    new_entry.last_seen_secs > existing_entry.last_seen_secs
                                } else {
                                    // New entry, always insert
                                    true
                                };

                                if should_update {
                                    debug!(log_group = %log_group, "Updating merged cache with more recent entry");
                                    merged.insert(log_group, new_entry);
                                }
                            }
                            CacheFile::new(merged)
                        })
                        .await?;

                    // Merge the result back into our in-memory cache
                    // This keeps the most recent entry for each log group
                    self.cache
                        .merge_snapshot(merged_cache_file.log_groups.clone());

                    // Try to save again with the merged data
                    s3_cache.save(merged_cache_file).await?;

                    info!("Successfully merged and persisted cache after conflict");
                    Ok(())
                }
                Err(e) => Err(TagError::S3(e)),
            }
        } else {
            Ok(())
        }
    }

    /// Check if tag fetching is currently disabled due to the circuit breaker
    pub fn is_tag_fetch_disabled(&self) -> bool {
        self.tag_fetch_disabled_until
            .is_some_and(|disabled_until| Instant::now() < disabled_until)
    }

    /// Get the remaining cooldown time if tag fetching is disabled
    ///
    /// Returns `Some(Duration)` with the remaining time if the circuit breaker is active,
    /// or `None` if tag fetching is currently enabled.
    pub fn remaining_cooldown(&self) -> Option<Duration> {
        self.tag_fetch_disabled_until.and_then(|disabled_until| {
            let now = Instant::now();
            if now < disabled_until {
                Some(disabled_until.duration_since(now))
            } else {
                None
            }
        })
    }

    /// Get cache statistics
    ///
    /// Returns information about the current state of the cache including
    /// the number of cached entries, whether S3 persistence is enabled, and
    /// whether tag fetching is currently disabled due to the circuit breaker.
    pub async fn cache_stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.cache.len(),
            persist_enabled: self.persist_enabled,
            tag_fetch_disabled: self.is_tag_fetch_disabled(),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of entries currently in the cache
    pub entry_count: usize,
    /// Whether S3 persistence is enabled
    pub persist_enabled: bool,
    /// Whether tag fetching is currently disabled due to AccessDenied circuit breaker
    pub tag_fetch_disabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;
    use std::time::Duration;

    #[tokio::test]
    async fn test_tag_manager_creation_without_s3() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let cw_client = CloudWatchLogsClient::new(&config);

        let manager = TagManager::new(cw_client, None, None);
        assert!(!manager.persist_enabled);

        let stats = manager.cache_stats().await;
        assert_eq!(stats.entry_count, 0);
        assert!(!stats.persist_enabled);
        assert!(!stats.tag_fetch_disabled);
    }

    #[tokio::test]
    async fn test_tag_manager_creation_with_s3() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let cw_client = CloudWatchLogsClient::new(&config);
        let s3_client = S3Client::new(&config);

        let manager = TagManager::new(cw_client, Some(s3_client), Some("test-bucket".to_string()));
        assert!(manager.persist_enabled);
    }

    #[tokio::test]
    async fn test_circuit_breaker_cooldown_duration() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let cw_client = CloudWatchLogsClient::new(&config);

        let mut manager = TagManager::new(cw_client, None, None);

        // Set a short cooldown for testing
        manager.cooldown_duration = Duration::from_millis(100);

        // Simulate AccessDenied by setting the disabled time
        manager.tag_fetch_disabled_until = Some(Instant::now() + manager.cooldown_duration);

        // Initially should be disabled
        let stats = manager.cache_stats().await;
        assert!(stats.tag_fetch_disabled);

        // Wait for cooldown to elapse
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should now be enabled again
        let stats = manager.cache_stats().await;
        assert!(!stats.tag_fetch_disabled);
    }
}
