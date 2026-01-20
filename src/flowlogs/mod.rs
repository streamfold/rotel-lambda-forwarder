//! EC2 Flow Log Manager Module
//!
//! This module provides comprehensive management of EC2 Flow Log configurations, including:
//! - Fetching flow log configurations from EC2 API (DescribeFlowLogs)
//! - Extracting flow log format strings for dynamic parsing
//! - Extracting and applying flow log tags to resource attributes
//! - Caching configurations in-memory with 30-minute TTL
//! - Persisting configurations to S3 for durability across Lambda cold starts
//!
//! ## Cold Start Optimization
//!
//! During Lambda initialization, the manager first attempts to load cached flow log
//! configurations from S3. If a valid (non-expired) cache is found, it is used immediately
//! and the EC2 DescribeFlowLogs API call is skipped. This significantly reduces cold start
//! time by avoiding the API call when cached data is still fresh.
//!
//! Only when the cache is expired, missing, or failed to load will the manager fetch
//! configurations from the EC2 API.
//!
//! ## EC2 Flow Log Tags
//!
//! When EC2 Flow Logs are configured with tags, those tags are automatically extracted
//! from the EC2 DescribeFlowLogs API and applied to log resource attributes with the
//! prefix `ec2.flow-logs.tags.<key>`.
//!
//! For example, if an EC2 Flow Log has the following tags:
//! - Environment: production
//! - Team: platform
//!
//! The resulting log resource attributes will include:
//! - `ec2.flow-logs.tags.Environment`: "production"
//! - `ec2.flow-logs.tags.Team`: "platform"
//!
//! This allows for rich filtering and aggregation of EC2 Flow Logs based on their
//! infrastructure metadata.

mod cache;
mod ec2;

pub use cache::{
    CacheSnapshot, FlowLogCache, FlowLogConfig, ParsedField, ParsedFieldType, ParsedFields,
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
    /// Version of the cache file format
    pub version: u32,
    /// Cache snapshot containing flow logs and last refresh timestamp
    pub snapshot: CacheSnapshot,
}

impl FlowLogCacheFile {
    /// Create a new cache file from a snapshot
    pub fn new(snapshot: CacheSnapshot) -> Self {
        Self {
            version: 1,
            snapshot,
        }
    }
}

/// Main flow log manager that coordinates cache, S3, and EC2 operations
///
/// The `FlowLogManager` provides automatic caching and persistence of EC2 Flow Log
/// configurations. It fetches configurations from EC2 API on startup and caches them
/// in memory with a 30-minute TTL. Optionally, configs can be persisted to S3 for
/// durability across Lambda cold starts.
///
/// ## Cold Start Optimization
///
/// During initialization, the manager prioritizes cache reuse to minimize cold start time.
/// If a valid cached configuration is loaded from S3 (not expired), the EC2 API call is
/// skipped entirely. This optimization reduces cold start latency and EC2 API calls while
/// ensuring data freshness through the 30-minute TTL.
///
/// ## Tag Handling
///
/// The manager extracts tags from EC2 Flow Logs during the EC2 API query. These tags
/// are stored in the cache alongside the log format and other configuration details.
/// When logs are processed, the tags are applied as resource attributes with the prefix
/// `ec2.flow-logs.tags.<key>`, allowing for rich metadata-based filtering and
/// aggregation.
pub struct FlowLogManager {
    /// In-memory cache
    cache: FlowLogCache,
    /// S3 persistence layer (optional)
    s3_cache: Option<S3Cache<FlowLogCacheFile>>,
    /// EC2 client for fetching flow log configurations
    ec2_fetcher: Ec2FlowLogFetcher,
    /// Whether to persist to S3
    persist_enabled: bool,
    /// Circuit breaker: timestamp when flow log fetching was disabled due to AccessDenied
    fetch_disabled_until: Option<Instant>,
    /// Circuit breaker cooldown period (30 minutes)
    cooldown_duration: Duration,
}

impl FlowLogManager {
    /// Create a new flow log manager
    ///
    /// # Arguments
    /// * `ec2_client` - EC2 client for fetching flow log configurations
    /// * `s3_client` - Optional S3 client for persistence
    /// * `s3_bucket` - Optional S3 bucket name for persistence
    pub fn new(
        ec2_client: Ec2Client,
        s3_client: Option<S3Client>,
        s3_bucket: Option<String>,
    ) -> Self {
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

    /// Initialize the flow log manager by loading the cache from S3 and fetching from EC2
    ///
    /// This should be called once during startup. It will:
    /// 1. Load cached flow log configurations from S3 (if available)
    /// 2. If cache is valid (not expired), use it and skip EC2 API call (optimization)
    /// 3. If cache is expired/missing, fetch current flow log configurations from EC2 API
    /// 4. Persist the data to S3
    ///
    /// Errors loading the cache are logged but do not fail initialization.
    pub async fn initialize(&mut self) -> Result<(), FlowLogError> {
        let mut cache_loaded = false;

        // Load from S3 if available
        if let Some(s3_cache) = &mut self.s3_cache {
            match s3_cache.load().await {
                Ok(Some(cache_file)) => {
                    let is_expired = cache_file.snapshot.is_expired();
                    info!(
                        entry_count = cache_file.snapshot.flow_logs.len(),
                        expired = is_expired,
                        "Loaded flow log cache from S3"
                    );

                    if !is_expired {
                        // Cache is still valid, use it and skip EC2 API call
                        self.cache.load_snapshot(cache_file.snapshot);
                        cache_loaded = true;
                        info!("Using valid cached flow log configurations, skipping EC2 API call");
                    } else {
                        debug!("Cached flow log configurations are expired, will fetch from EC2");
                    }
                }
                Ok(None) => {
                    debug!("No existing flow log cache found in S3");
                }
                Err(e) => {
                    error!(error = %e, "Failed to load flow log cache from S3, continuing with empty cache");
                    // Don't fail initialization, just start with empty cache
                }
            }
        }

        // Only fetch from EC2 if we don't have a valid cache
        if !cache_loaded {
            match self.fetch_and_update_all().await {
                Ok(_) => {
                    info!("Successfully fetched and cached flow log configurations from EC2");
                }
                Err(Ec2Error::AccessDenied(_)) => {
                    warn!("Access denied when fetching flow logs from EC2, using cached data only");
                    // Don't fail initialization, just use cached data (if any)
                }
                Err(e) => {
                    error!(error = %e, "Failed to fetch flow logs from EC2 during initialization");
                    // Return error for non-AccessDenied errors only if we have no cache
                    if self.cache.is_empty() {
                        return Err(FlowLogError::Ec2(e));
                    } else {
                        warn!("Using potentially stale cache due to EC2 API error");
                    }
                }
            }
        }

        info!("Flow log manager initialization complete");
        Ok(())
    }

    /// Get flow log configuration for a log group
    ///
    /// First checks the in-memory cache. Returns None if not found or expired.
    /// Lazily parses the log format fields on first access and caches the result.
    pub fn get_config(&mut self, log_group_name: &str) -> Option<FlowLogConfig> {
        // Get a mutable reference to the config so we can parse fields if needed
        let config = self.cache.get_mut(log_group_name)?;

        // Parse fields if not already attempted
        if config.parsed_fields.is_none() {
            let fields = parse_log_format(&config.log_format);

            if fields.is_empty() {
                // Parsing failed - cache the error
                config.parsed_fields = Some(Arc::new(ParsedFields::Error(
                    "Failed to parse log format or no fields found".to_string(),
                )));
                warn!(
                    log_group = %log_group_name,
                    log_format = %config.log_format,
                    "Failed to parse flow log format fields"
                );
            } else {
                // Parsing succeeded - cache the result
                let field_count = fields.len();
                config.parsed_fields = Some(Arc::new(ParsedFields::Success(fields)));
                debug!(
                    log_group = %log_group_name,
                    field_count = field_count,
                    "Parsed log format fields with types for flow log"
                );
            }
        }

        Some(config.clone())
    }

    /// Fetch all flow logs from EC2 and update the cache
    async fn fetch_and_update_all(&mut self) -> Result<(), Ec2Error> {
        // Check if fetching is currently disabled due to AccessDenied
        if let Some(disabled_until) = self.fetch_disabled_until {
            let now = Instant::now();
            if now < disabled_until {
                let remaining = disabled_until.duration_since(now);
                warn!(
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

        debug!("Fetching flow logs from EC2");

        let flow_log_configs = match self.ec2_fetcher.fetch_all_flow_logs().await {
            Ok(configs) => configs,
            Err(Ec2Error::AccessDenied(_)) => {
                // Disable fetching for the next 30 minutes
                let disabled_until = Instant::now() + self.cooldown_duration;
                self.fetch_disabled_until = Some(disabled_until);

                warn!(
                    cooldown_minutes = self.cooldown_duration.as_secs() / 60,
                    "AccessDenied error, disabling flow log fetching"
                );

                return Err(Ec2Error::AccessDenied(
                    "AccessDenied when fetching flow logs".to_string(),
                ));
            }
            Err(e) => {
                // For other errors, propagate them normally
                return Err(e);
            }
        };

        // Update cache with fetched configurations
        for (log_group, config) in flow_log_configs {
            self.cache.insert(log_group, config);
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

    /// Persist the current cache to S3
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

                    // Replace out local cache with the most recent snapshot
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

    /// Check if flow log fetching is currently disabled due to the circuit breaker
    pub fn is_fetch_disabled(&self) -> bool {
        self.fetch_disabled_until
            .is_some_and(|disabled_until| Instant::now() < disabled_until)
    }

    /// Get the remaining cooldown time if fetching is disabled
    ///
    /// Returns `Some(Duration)` with the remaining time if the circuit breaker is active,
    /// or `None` if fetching is currently enabled.
    pub fn remaining_cooldown(&self) -> Option<Duration> {
        self.fetch_disabled_until.and_then(|disabled_until| {
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
    /// the number of cached entries and whether fetching is currently disabled.
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.cache.len(),
            persist_enabled: self.persist_enabled,
            fetch_disabled: self.is_fetch_disabled(),
        }
    }
}

/// Parse the LogFormat string to extract field names and their types
///
/// LogFormat strings look like: "${version} ${account-id} ${interface-id} ..."
/// This function extracts the field names between ${ and } and assigns types based on
/// the AWS VPC Flow Logs documentation.
pub fn parse_log_format(log_format: &str) -> Vec<ParsedField> {
    use crate::flowlogs::cache::{ParsedField, get_field_type};

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

/// Cache statistics
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
    async fn test_flow_log_manager_creation_without_s3() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let manager = FlowLogManager::new(ec2_client, None, None);
        assert!(!manager.persist_enabled);

        let stats = manager.cache_stats();
        assert_eq!(stats.entry_count, 0);
        assert!(!stats.persist_enabled);
        assert!(!stats.fetch_disabled);
    }

    #[tokio::test]
    async fn test_flow_log_manager_creation_with_s3() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);
        let s3_client = S3Client::new(&config);

        let manager =
            FlowLogManager::new(ec2_client, Some(s3_client), Some("test-bucket".to_string()));
        assert!(manager.persist_enabled);
    }

    #[tokio::test]
    async fn test_circuit_breaker_cooldown_duration() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let mut manager = FlowLogManager::new(ec2_client, None, None);

        // Set a short cooldown for testing
        manager.cooldown_duration = Duration::from_millis(100);

        // Simulate AccessDenied by setting the disabled time
        manager.fetch_disabled_until = Some(Instant::now() + manager.cooldown_duration);

        // Initially should be disabled
        let stats = manager.cache_stats();
        assert!(stats.fetch_disabled);

        // Wait for cooldown to elapse
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should now be enabled again
        let stats = manager.cache_stats();
        assert!(!stats.fetch_disabled);
    }

    #[tokio::test]
    async fn test_cache_reuse_optimization() {
        use crate::flowlogs::cache::{CacheSnapshot, FlowLogConfig};
        use std::collections::HashMap;

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let mut manager = FlowLogManager::new(ec2_client, None, None);

        // Manually insert a valid cache entry
        let mut flow_logs = HashMap::new();
        flow_logs.insert(
            "/aws/ec2/test-flowlogs".to_string(),
            FlowLogConfig {
                log_format: "${version} ${account-id}".to_string(),
                destination_type: "cloud-watch-logs".to_string(),
                flow_log_id: "fl-test123".to_string(),
                tags: HashMap::new(),
                parsed_fields: None,
            },
        );

        let snapshot = CacheSnapshot {
            flow_logs,
            last_refreshed_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        manager.cache.load_snapshot(snapshot);

        // Verify cache has the entry and is not expired
        assert!(!manager.cache.is_expired());
        assert_eq!(manager.cache.len(), 1);

        let config = manager.get_config("/aws/ec2/test-flowlogs");
        assert!(config.is_some());
        assert_eq!(config.unwrap().flow_log_id, "fl-test123");
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
}
