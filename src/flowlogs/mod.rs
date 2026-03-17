//! EC2 Flow Log Manager Module
//!
//! This module provides comprehensive management of EC2 Flow Log configurations, including:
//! - Fetching flow log configurations from EC2 API (DescribeFlowLogs)
//! - Extracting flow log format strings for dynamic parsing
//! - Extracting and applying flow log tags to resource attributes
//! - Caching configurations in-memory with 30-minute TTL
//! - Persisting configurations to S3 for durability across Lambda cold starts
//!
//! Flow logs are indexed by destination type:
//! - CloudWatch Logs destinations: looked up by log group name via `get_config`
//! - S3 destinations: looked up by bucket name + object key via `get_config_by_bucket`
//!
//! ## Thread Safety
//!
//! `FlowLogManager` is cheaply cloneable and safe to share across concurrent tokio tasks.
//! All mutable state lives behind an `Arc<tokio::sync::Mutex<FlowLogManagerInner>>`, so
//! callers can clone the handle and call `get_config` / `get_config_by_bucket` from
//! multiple tasks simultaneously without any external synchronization.

mod cache;
mod ec2;

pub use cache::{
    CacheSnapshot, FlowLogCache, FlowLogConfig, ParsedField, ParsedFieldType, ParsedFields,
    get_field_type,
};
pub use ec2::{Ec2Error, Ec2FlowLogFetcher};

use aws_sdk_ec2::Client as Ec2Client;
use aws_sdk_s3::Client as S3Client;
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::s3_cache::{S3Cache, S3CacheError};

/// S3 cache key for storing flow log configurations
const FLOW_LOG_CACHE_KEY: &str = "rotel-lambda-forwarder/cache/flow-logs/configs.json.gz";

/// Errors that can occur during flow log operations
#[derive(Debug, Error)]
pub enum FlowLogError {
    #[error("EC2 error: {0}")]
    Ec2(#[from] Ec2Error),

    #[error("S3 error: {0}")]
    S3(#[from] S3CacheError),

    #[error("No S3 bucket configured")]
    NoS3Bucket,
}

/// Serializable format for the cache file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowLogCacheFile {
    pub version: u32,
    pub snapshot: CacheSnapshot,
}

impl FlowLogCacheFile {
    pub fn new(snapshot: CacheSnapshot) -> Self {
        Self {
            version: 1,
            snapshot,
        }
    }
}

// ---------------------------------------------------------------------------
// Inner state (lives behind the shared mutex)
// ---------------------------------------------------------------------------

/// All mutable state owned by `FlowLogManager`.
///
/// Callers never access this type directly; it is always accessed through the
/// `Arc<Mutex<FlowLogManagerInner>>` held by `FlowLogManager`.
struct FlowLogManagerInner {
    cache: FlowLogCache,
    s3_cache: Option<S3Cache<FlowLogCacheFile>>,
    ec2_fetcher: Ec2FlowLogFetcher,
    persist_enabled: bool,
    fetch_disabled_until: Option<Instant>,
    cooldown_duration: Duration,
}

impl FlowLogManagerInner {
    fn new(ec2_client: Ec2Client, s3_client: Option<S3Client>, s3_bucket: Option<String>) -> Self {
        let persist_enabled = s3_client.is_some() && s3_bucket.is_some();

        let s3_cache = match (s3_client, s3_bucket) {
            (Some(client), Some(bucket)) => {
                Some(S3Cache::new(client, bucket, FLOW_LOG_CACHE_KEY.to_string()))
            }
            _ => None,
        };

        Self {
            cache: FlowLogCache::new(),
            s3_cache,
            ec2_fetcher: Ec2FlowLogFetcher::new(ec2_client),
            persist_enabled,
            fetch_disabled_until: None,
            cooldown_duration: Duration::from_secs(30 * 60), // 30 minutes
        }
    }

    // -----------------------------------------------------------------------
    // Reload / fetch helpers
    // -----------------------------------------------------------------------

    /// Reload cache from S3 or EC2 if needed.
    ///
    /// First attempts to load a valid (non-expired) cache from S3.
    /// If S3 cache doesn't exist or is expired, fetches from EC2.
    async fn reload_cache_if_needed(&mut self) -> Result<(), Ec2Error> {
        // Check if fetching is currently disabled due to AccessDenied
        if let Some(disabled_until) = self.fetch_disabled_until {
            let now = Instant::now();
            if now < disabled_until {
                let remaining = disabled_until.duration_since(now);
                debug!(
                    remaining_seconds = remaining.as_secs(),
                    "Flow log fetching is disabled due to AccessDenied"
                );
                return Ok(());
            } else {
                // Cooldown period has elapsed, re-enable fetching
                info!("Cooldown period elapsed, attempting to fetch flow logs again");
                self.fetch_disabled_until = None;
            }
        }

        // Load from S3 if available
        if let Some(s3_cache) = &mut self.s3_cache {
            match s3_cache.load().await {
                Ok(Some(cache_file)) => {
                    let is_expired = cache_file.snapshot.is_expired();
                    info!(
                        log_group_count = cache_file.snapshot.by_log_group.len(),
                        bucket_count = cache_file.snapshot.by_bucket.len(),
                        expired = is_expired,
                        "Loaded flow log cache from S3"
                    );

                    if !is_expired {
                        // Cache is still valid, use it and skip EC2 API call
                        self.cache.load_snapshot(cache_file.snapshot);
                        debug!("Using valid cached flow log configurations, skipping EC2 API call");
                        return Ok(());
                    } else {
                        debug!("Cached flow log configurations are expired, will fetch from EC2");
                    }
                }
                Ok(None) => {
                    debug!("No existing flow log cache found in S3");
                }
                Err(e) => {
                    error!(error = %e, "Failed to load flow log cache from S3, will refresh");
                    // Don't fail, just continue without S3 cache
                }
            }
        }

        // Only fetch from EC2 if we don't have a valid cache, will persist to S3 if loaded
        match self.fetch_and_update_all().await {
            Ok(_) => {
                info!("Successfully fetched and cached flow log configurations from EC2");
                Ok(())
            }
            Err(Ec2Error::AccessDenied(e)) => {
                warn!(error = ?e, "Access denied when fetching flow logs from EC2");
                // Disable fetching for the next 30 minutes
                let disabled_until = Instant::now() + self.cooldown_duration;
                self.fetch_disabled_until = Some(disabled_until);

                warn!(
                    cooldown_minutes = self.cooldown_duration.as_secs() / 60,
                    "AccessDenied error, disabling flow log fetching"
                );

                Ok(()) // Don't fail, just use cached data (if any)
            }
            Err(e) => Err(e),
        }
    }

    /// Fetch all flow logs from EC2 and update both cache maps.
    async fn fetch_and_update_all(&mut self) -> Result<(), Ec2Error> {
        debug!("Fetching flow logs from EC2");

        let fetched = self.ec2_fetcher.fetch_all_flow_logs().await?;

        // Update CloudWatch cache entries
        for (log_group, config) in fetched.by_log_group {
            self.cache.insert_by_log_group(log_group, config);
        }

        // Update S3 cache entries
        for (bucket, configs) in fetched.by_bucket {
            for config in configs {
                self.cache.insert_by_bucket(bucket.clone(), config);
            }
        }

        // Mark the cache as refreshed
        self.cache.mark_refreshed();

        // Persist to S3 if enabled
        if self.persist_enabled {
            if let Err(e) = self.persist_cache().await {
                error!(error = %e, "Failed to persist flow log cache to S3");
                // Don't fail the request, just log the error
            }
        }

        Ok(())
    }

    /// Persist the current cache to S3.
    async fn persist_cache(&mut self) -> Result<(), FlowLogError> {
        if let Some(s3_cache) = &mut self.s3_cache {
            let snapshot = self.cache.get_snapshot();
            let cache_file = FlowLogCacheFile::new(snapshot);

            match s3_cache.save(cache_file).await {
                Ok(_) => {
                    debug!("Successfully persisted flow log cache to S3");
                    Ok(())
                }
                Err(S3CacheError::ConditionalWriteFailed) => {
                    warn!("Conditional write failed, reloading and merging flow log cache");

                    // Reload from S3 and merge with our current snapshot
                    let local_snapshot = self.cache.get_snapshot();
                    let local_cache_file = FlowLogCacheFile::new(local_snapshot);

                    let merged = s3_cache
                        .reload_and_merge(local_cache_file, |from_s3, local| {
                            // Merge the snapshots, keeping the most recent one
                            let merged_snapshot = if local.snapshot.last_refreshed_secs
                                > from_s3.snapshot.last_refreshed_secs
                            {
                                debug!("Local snapshot is more recent, using it");
                                local.snapshot
                            } else {
                                debug!("S3 snapshot is more recent, using it");
                                from_s3.snapshot
                            };

                            FlowLogCacheFile::new(merged_snapshot)
                        })
                        .await?;

                    // Replace our local cache with the most recent snapshot
                    self.cache.load_snapshot(merged.snapshot.clone());

                    // Try to save again with the merged data. It is possible this
                    // could fail again, but we ignore it in the caller.
                    s3_cache.save(merged).await?;

                    info!("Successfully merged and persisted flow log cache after conflict");
                    Ok(())
                }
                Err(e) => Err(FlowLogError::S3(e)),
            }
        } else {
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // Cache freshness helpers
    // -----------------------------------------------------------------------

    /// Ensure the cache is not expired, reloading if necessary.
    ///
    /// Returns `true` if the cache is usable (or was successfully reloaded),
    /// and `false` if it could not be refreshed.
    async fn ensure_cache_fresh(&mut self, key: &str, key_type: &str) -> bool {
        if self.cache.is_expired() {
            debug!(
                key = %key,
                key_type = %key_type,
                "Cache expired, attempting to reload"
            );

            match self.reload_cache_if_needed().await {
                Ok(_) => {
                    debug!("Successfully reloaded flow log cache");
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        key = %key,
                        key_type = %key_type,
                        "Cache expired and could not be reloaded"
                    );
                    return false;
                }
            }
        }
        true
    }

    // -----------------------------------------------------------------------
    // Status helpers
    // -----------------------------------------------------------------------

    fn is_fetch_disabled(&self) -> bool {
        self.fetch_disabled_until
            .is_some_and(|disabled_until| Instant::now() < disabled_until)
    }

    fn remaining_cooldown(&self) -> Option<Duration> {
        self.fetch_disabled_until.and_then(|disabled_until| {
            let now = Instant::now();
            if now < disabled_until {
                Some(disabled_until.duration_since(now))
            } else {
                None
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Public handle
// ---------------------------------------------------------------------------

/// Main flow log manager that coordinates cache, S3, and EC2 operations.
///
/// `FlowLogManager` is a cheap-to-clone, `Send + Sync` handle backed by a shared
/// `Arc<Mutex<FlowLogManagerInner>>`.  Multiple tokio tasks may hold clones of the
/// same handle and call [`get_config`] / [`get_config_by_bucket`] concurrently —
/// the internal mutex serialises cache reads, lazy field parsing, and cache refreshes.
///
/// ## Lookup methods
/// - [`get_config`]: look up by CloudWatch log group name (CloudWatch Logs destinations)
/// - [`get_config_by_bucket`]: look up by S3 bucket name + object key (S3 destinations)
///
/// [`get_config`]: FlowLogManager::get_config
/// [`get_config_by_bucket`]: FlowLogManager::get_config_by_bucket
#[derive(Clone)]
pub struct FlowLogManager {
    inner: Arc<Mutex<FlowLogManagerInner>>,
}

impl FlowLogManager {
    /// Create a new flow log manager.
    pub fn new(
        ec2_client: Ec2Client,
        s3_client: Option<S3Client>,
        s3_bucket: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FlowLogManagerInner::new(
                ec2_client, s3_client, s3_bucket,
            ))),
        }
    }

    /// Initialize the flow log manager by loading the cache from S3 and fetching from EC2.
    pub async fn initialize(&self) -> Result<(), FlowLogError> {
        let mut inner = self.inner.lock().await;
        match inner.reload_cache_if_needed().await {
            Ok(_) => {
                info!("Flow log manager initialization complete");
                Ok(())
            }
            Err(e) => {
                error!(error = %e, "Failed to fetch flow logs from EC2 during initialization");
                Err(FlowLogError::Ec2(e))
            }
        }
    }

    /// Get flow log configuration for a CloudWatch log group name.
    ///
    /// Checks the in-memory cache first. If the cache is expired it attempts a reload
    /// from S3 / EC2 before returning. Returns `None` if no matching configuration is
    /// found or if the cache cannot be refreshed.
    ///
    /// Parsed fields are lazily initialised on first access and written back into the
    /// cache to avoid re-parsing on subsequent calls.
    pub async fn get_config(&self, log_group_name: &str) -> Option<FlowLogConfig> {
        let mut inner = self.inner.lock().await;

        if !inner.ensure_cache_fresh(log_group_name, "log_group").await {
            return None;
        }

        let mut config = inner.cache.get_by_log_group(log_group_name)?;

        // Lazily compute parsed fields and write them back into the cache so
        // that subsequent callers find them already populated.
        if config.parsed_fields.is_none() {
            let parsed = Arc::new(Self::compute_parsed_fields(
                &config.log_format,
                log_group_name,
                "log_group",
            ));
            inner
                .cache
                .set_parsed_fields_by_log_group(log_group_name, Arc::clone(&parsed));
            config.parsed_fields = Some(parsed);
        }

        Some(config)
    }

    /// Get flow log configuration for an S3 object.
    ///
    /// Mirrors [`get_config`] but looks up by S3 bucket name + object key rather than a
    /// CloudWatch log group name. Because multiple flow logs may target the same bucket
    /// (differentiated by a folder prefix), the full object key is required to select
    /// the correct configuration. This is used when processing VPC flow log files
    /// delivered to S3.
    ///
    /// Note: `parsed_fields` is intentionally **not** populated on the returned config.
    /// S3-delivered VPC flow log files carry their own column-header line, which the
    /// S3 record parser reads directly via [`parse_vpclog_header`].  The `log_format`
    /// string stored in the cache is therefore unused on the S3 path.
    ///
    /// [`get_config`]: FlowLogManager::get_config
    /// [`parse_vpclog_header`]: crate::parse::vpclog::parse_vpclog_header
    pub async fn get_config_by_bucket(
        &self,
        bucket_name: &str,
        object_key: &str,
    ) -> Option<FlowLogConfig> {
        let mut inner = self.inner.lock().await;

        if !inner.ensure_cache_fresh(bucket_name, "bucket").await {
            return None;
        }

        inner.cache.get_by_bucket(bucket_name, object_key)
    }

    // -----------------------------------------------------------------------
    // Status accessors (read-only, acquire lock briefly)
    // -----------------------------------------------------------------------

    /// Check if flow log fetching is currently disabled due to the circuit breaker.
    pub async fn is_fetch_disabled(&self) -> bool {
        self.inner.lock().await.is_fetch_disabled()
    }

    /// Get the remaining cooldown time if fetching is disabled.
    ///
    /// Returns `Some(Duration)` with the remaining time if the circuit breaker is active,
    /// or `None` if fetching is currently enabled.
    pub async fn remaining_cooldown(&self) -> Option<Duration> {
        self.inner.lock().await.remaining_cooldown()
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Parse the log format string into typed fields, returning a `ParsedFields` value.
    fn compute_parsed_fields(log_format: &str, key: &str, key_type: &str) -> ParsedFields {
        let fields = parse_log_format(log_format);

        if fields.is_empty() {
            warn!(
                key = %key,
                key_type = %key_type,
                log_format = %log_format,
                "Failed to parse flow log format fields"
            );
            ParsedFields::Error("Failed to parse log format or no fields found".to_string())
        } else {
            debug!(
                key = %key,
                key_type = %key_type,
                field_count = fields.len(),
                "Parsed log format fields with types for flow log"
            );
            ParsedFields::Success(fields)
        }
    }

    // -----------------------------------------------------------------------
    // Test helpers (only compiled in test builds)
    // -----------------------------------------------------------------------

    /// Get cache statistics.
    #[cfg(test)]
    pub async fn cache_stats(&self) -> CacheStats {
        let inner = self.inner.lock().await;
        CacheStats {
            entry_count: inner.cache.len(),
            persist_enabled: inner.persist_enabled,
            fetch_disabled: inner.is_fetch_disabled(),
        }
    }

    /// Expose inner cache length for tests.
    #[cfg(test)]
    pub async fn cache_len(&self) -> usize {
        self.inner.lock().await.cache.len()
    }

    /// Expose inner cache expiry status for tests.
    #[cfg(test)]
    pub async fn cache_is_expired(&self) -> bool {
        self.inner.lock().await.cache.is_expired()
    }

    /// Load a snapshot directly into the inner cache (test helper).
    #[cfg(test)]
    pub async fn load_cache_snapshot(&self, snapshot: CacheSnapshot) {
        self.inner.lock().await.cache.load_snapshot(snapshot);
    }

    /// Override the cooldown duration (test helper).
    #[cfg(test)]
    pub async fn set_cooldown_duration(&self, duration: Duration) {
        self.inner.lock().await.cooldown_duration = duration;
    }

    /// Simulate an AccessDenied circuit-breaker trip (test helper).
    #[cfg(test)]
    pub async fn trigger_fetch_disabled(&self) {
        let mut inner = self.inner.lock().await;
        let cooldown = inner.cooldown_duration;
        inner.fetch_disabled_until = Some(Instant::now() + cooldown);
    }
}

/// Parse the LogFormat string to extract field names and their types.
///
/// LogFormat strings look like: `"${version} ${account-id} ${interface-id} ..."`
/// This function extracts the field names between `${` and `}` and assigns types based on
/// the AWS VPC Flow Logs documentation.
pub fn parse_log_format(log_format: &str) -> Vec<ParsedField> {
    let mut fields = Vec::new();
    let mut chars = log_format.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' {
            if chars.peek() == Some(&'{') {
                chars.next(); // consume '{'
                let mut field_name = String::new();

                // Read until '}'
                while let Some(&ch) = chars.peek() {
                    if ch == '}' {
                        chars.next(); // consume '}'
                        break;
                    }
                    field_name.push(chars.next().unwrap());
                }

                if !field_name.is_empty() {
                    let field_type = get_field_type(&field_name);
                    fields.push(ParsedField::new(field_name, field_type));
                }
            }
        }
    }

    debug!(
        field_count = fields.len(),
        "Parsed log format fields with types"
    );
    fields
}

/// Cache statistics (test builds only).
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of entries currently in the cache
    pub entry_count: usize,
    /// Whether S3 persistence is enabled
    pub persist_enabled: bool,
    /// Whether flow log fetching is currently disabled due to AccessDenied circuit breaker
    pub fetch_disabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;

    #[tokio::test]
    async fn test_circuit_breaker_cooldown_duration() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let manager = FlowLogManager::new(ec2_client, None, None);

        // Set a short cooldown for testing
        manager
            .set_cooldown_duration(Duration::from_millis(100))
            .await;

        // Simulate AccessDenied by tripping the circuit breaker
        manager.trigger_fetch_disabled().await;

        // Initially should be disabled
        let stats = manager.cache_stats().await;
        assert!(stats.fetch_disabled);

        // Wait for cooldown to elapse
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should now be enabled again
        let stats = manager.cache_stats().await;
        assert!(!stats.fetch_disabled);
    }

    #[tokio::test]
    async fn test_cache_reuse_optimization_by_log_group() {
        use crate::flowlogs::cache::{CacheSnapshot, FlowLogConfig};
        use std::collections::HashMap;

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let manager = FlowLogManager::new(ec2_client, None, None);

        // Manually insert a valid cache entry via a snapshot
        let mut by_log_group = HashMap::new();
        by_log_group.insert(
            "/aws/ec2/test-flowlogs".to_string(),
            FlowLogConfig {
                log_format: "${version} ${account-id}".to_string(),
                flow_log_id: "fl-test123".to_string(),
                tags: HashMap::new(),
                folder_prefix: None,
                parsed_fields: None,
            },
        );

        let snapshot = CacheSnapshot {
            by_log_group,
            by_bucket: HashMap::new(),
            last_refreshed_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        manager.load_cache_snapshot(snapshot).await;

        assert!(!manager.cache_is_expired().await);
        assert_eq!(manager.cache_len().await, 1);

        let config = manager.get_config("/aws/ec2/test-flowlogs").await;
        assert!(config.is_some());
        assert_eq!(config.unwrap().flow_log_id, "fl-test123");
    }

    #[tokio::test]
    async fn test_cache_reuse_optimization_by_bucket() {
        use crate::flowlogs::cache::{CacheSnapshot, FlowLogConfig};
        use std::collections::HashMap;

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let manager = FlowLogManager::new(ec2_client, None, None);

        let mut by_bucket = HashMap::new();
        by_bucket.insert(
            "my-flow-logs-bucket".to_string(),
            vec![FlowLogConfig {
                log_format: "${version} ${account-id} ${srcaddr}".to_string(),
                flow_log_id: "fl-s3-abc456".to_string(),
                tags: HashMap::new(),
                folder_prefix: None,
                parsed_fields: None,
            }],
        );

        let snapshot = CacheSnapshot {
            by_log_group: HashMap::new(),
            by_bucket,
            last_refreshed_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        manager.load_cache_snapshot(snapshot).await;

        assert!(!manager.cache_is_expired().await);
        assert_eq!(manager.cache_len().await, 1);

        let config = manager
            .get_config_by_bucket("my-flow-logs-bucket", "AWSLogs/2024/01/01/flow.log.gz")
            .await;
        assert!(config.is_some());
        let config = config.unwrap();
        assert_eq!(config.flow_log_id, "fl-s3-abc456");
        // parsed_fields is intentionally not populated on the S3 path — S3-delivered
        // VPC flow log files carry their own column-header line which is parsed at
        // read time, so the cached log_format is unused on this path.
        assert!(config.parsed_fields.is_none());
    }

    #[tokio::test]
    async fn test_get_config_by_bucket_miss() {
        use std::collections::HashMap;

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let manager = FlowLogManager::new(ec2_client, None, None);

        // Seed with a fresh (non-expired) empty cache so we don't attempt an EC2 fetch
        let snapshot = crate::flowlogs::cache::CacheSnapshot {
            by_log_group: HashMap::new(),
            by_bucket: HashMap::new(),
            last_refreshed_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        manager.load_cache_snapshot(snapshot).await;

        let result = manager
            .get_config_by_bucket("non-existent-bucket", "some/key.log.gz")
            .await;
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_log_format_default() {
        let log_format = "${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}";
        let fields = parse_log_format(log_format);

        assert_eq!(fields.len(), 14);
        assert_eq!(fields[0].field_name, "version");
        assert_eq!(fields[1].field_name, "account-id");
        assert_eq!(fields[2].field_name, "interface-id");
        assert_eq!(fields[13].field_name, "log-status");
    }

    #[test]
    fn test_parse_log_format_custom() {
        let log_format = "${version} ${vpc-id} ${subnet-id} ${instance-id} ${srcaddr} ${dstaddr}";
        let fields = parse_log_format(log_format);

        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0].field_name, "version");
        assert_eq!(fields[1].field_name, "vpc-id");
        assert_eq!(fields[2].field_name, "subnet-id");
        assert_eq!(fields[3].field_name, "instance-id");
        assert_eq!(fields[4].field_name, "srcaddr");
        assert_eq!(fields[5].field_name, "dstaddr");
    }

    #[test]
    fn test_parse_log_format_with_extra_spaces() {
        let log_format = "${version}  ${account-id}   ${interface-id}";
        let fields = parse_log_format(log_format);

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].field_name, "version");
        assert_eq!(fields[1].field_name, "account-id");
        assert_eq!(fields[2].field_name, "interface-id");
    }

    #[tokio::test]
    async fn test_cache_expiration_and_reload() {
        use crate::flowlogs::cache::{CacheSnapshot, FlowLogConfig};
        use std::collections::HashMap;

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let manager = FlowLogManager::new(ec2_client, None, None);

        // Manually insert a cache entry with an expired timestamp
        let mut by_log_group = HashMap::new();
        by_log_group.insert(
            "/aws/ec2/expired-flowlogs".to_string(),
            FlowLogConfig {
                log_format: "${version} ${account-id}".to_string(),
                flow_log_id: "fl-expired123".to_string(),
                tags: HashMap::new(),
                folder_prefix: None,
                parsed_fields: None,
            },
        );

        // Create an expired snapshot (older than 30 minutes)
        let expired_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 1900; // 31+ minutes ago

        let snapshot = CacheSnapshot {
            by_log_group,
            by_bucket: HashMap::new(),
            last_refreshed_secs: expired_time,
        };

        manager.load_cache_snapshot(snapshot).await;

        // load_snapshot ignores expired snapshots, so cache should be empty & expired
        assert!(manager.cache_is_expired().await);

        // Attempt to get config - should return None since cache is expired
        // and we can't reload from EC2 (no permissions) or S3 (not configured)
        let config = manager.get_config("/aws/ec2/expired-flowlogs").await;
        assert!(config.is_none());
    }

    /// Verify that a `FlowLogManager` clone shares the same underlying state.
    #[tokio::test]
    async fn test_clone_shares_state() {
        use crate::flowlogs::cache::{CacheSnapshot, FlowLogConfig};
        use std::collections::HashMap;

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let manager = FlowLogManager::new(ec2_client, None, None);
        let manager_clone = manager.clone();

        // Seed via the original handle
        let mut by_log_group = HashMap::new();
        by_log_group.insert(
            "/aws/ec2/shared-flowlogs".to_string(),
            FlowLogConfig {
                log_format: "${version} ${account-id}".to_string(),
                flow_log_id: "fl-shared".to_string(),
                tags: HashMap::new(),
                folder_prefix: None,
                parsed_fields: None,
            },
        );
        let snapshot = CacheSnapshot {
            by_log_group,
            by_bucket: HashMap::new(),
            last_refreshed_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        manager.load_cache_snapshot(snapshot).await;

        // Clone should see the same data
        let result = manager_clone.get_config("/aws/ec2/shared-flowlogs").await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().flow_log_id, "fl-shared");
    }

    /// Verify that two concurrent tasks can call get_config simultaneously.
    #[tokio::test]
    async fn test_concurrent_get_config() {
        use crate::flowlogs::cache::{CacheSnapshot, FlowLogConfig};
        use std::collections::HashMap;

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let manager = FlowLogManager::new(ec2_client, None, None);

        // Seed the cache
        let mut by_log_group = HashMap::new();
        for i in 0..5 {
            by_log_group.insert(
                format!("/aws/ec2/flowlogs-{}", i),
                FlowLogConfig {
                    log_format: "${version} ${account-id}".to_string(),
                    flow_log_id: format!("fl-concurrent-{}", i),
                    tags: HashMap::new(),
                    folder_prefix: None,
                    parsed_fields: None,
                },
            );
        }
        let snapshot = CacheSnapshot {
            by_log_group,
            by_bucket: HashMap::new(),
            last_refreshed_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        manager.load_cache_snapshot(snapshot).await;

        // Spawn five concurrent lookups on cloned handles
        let mut handles = Vec::new();
        for i in 0..5 {
            let m = manager.clone();
            handles.push(tokio::spawn(async move {
                m.get_config(&format!("/aws/ec2/flowlogs-{}", i)).await
            }));
        }

        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.await.expect("task panicked");
            assert!(result.is_some(), "task {} got None", i);
            assert_eq!(result.unwrap().flow_log_id, format!("fl-concurrent-{}", i));
        }
    }
}
