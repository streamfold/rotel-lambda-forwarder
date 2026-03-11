use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, trace};

const MAX_CACHE_AGE_SECS: u64 = 30 * 60;

/// Data type for a flow log field, based on Parquet data types from AWS documentation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedFieldType {
    String,
    Int32,
    Int64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedField {
    /// The field name (e.g., "version", "account-id")
    pub field_name: String,
    pub field_type: ParsedFieldType,
}

impl ParsedField {
    pub fn new(field_name: String, field_type: ParsedFieldType) -> Self {
        Self {
            field_name,
            field_type,
        }
    }
}

/// Static mapping of non-string field names to their data types based on AWS VPC Flow Logs documentation
/// Reference: https://docs.aws.amazon.com/vpc/latest/userguide/flow-log-records.html
/// NOTE: All fields not listed here are assumed to be a String, this reduces map size.
static FIELD_TYPE_MAP: LazyLock<HashMap<&'static str, ParsedFieldType>> = LazyLock::new(|| {
    let mut map = HashMap::new();

    // Version 2 fields
    map.insert("version", ParsedFieldType::Int32);
    map.insert("srcport", ParsedFieldType::Int32);
    map.insert("dstport", ParsedFieldType::Int32);
    map.insert("protocol", ParsedFieldType::Int32);
    map.insert("packets", ParsedFieldType::Int64);
    map.insert("bytes", ParsedFieldType::Int64);
    map.insert("start", ParsedFieldType::Int64);
    map.insert("end", ParsedFieldType::Int64);

    // Version 3 fields
    map.insert("tcp-flags", ParsedFieldType::Int32);

    // Version 5 fields
    map.insert("traffic-path", ParsedFieldType::Int32);

    // Version 10 fields
    map.insert("encryption-status", ParsedFieldType::Int32);

    map
});

/// Get the field type for a given field name, defaulting to String if unknown
pub fn get_field_type(field_name: &str) -> ParsedFieldType {
    FIELD_TYPE_MAP
        .get(field_name)
        .copied()
        .unwrap_or(ParsedFieldType::String)
}

/// Result of parsing flow log format fields
#[derive(Debug, Clone, PartialEq)]
pub enum ParsedFields {
    /// Successfully parsed field names and types from the log format
    Success(Vec<ParsedField>),
    /// Failed to parse the log format, with error message
    Error(String),
}

/// Flow log configuration for a specific destination (log group or S3 bucket)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlowLogConfig {
    /// The log format string (e.g., "${version} ${account-id} ${interface-id} ...")
    pub log_format: String,
    pub flow_log_id: String,
    pub tags: std::collections::HashMap<String, String>,

    /// Parsed field names from the log format (lazily computed, not serialized)
    #[serde(skip)]
    pub parsed_fields: Option<Arc<ParsedFields>>, // Use an Arc to reduce clone costs
}

/// In-memory cache for flow log configurations, partitioned by destination type.
///
/// Two independent look-up maps are maintained:
/// - `by_log_group`: CloudWatch Logs destinations, keyed by log group name.
/// - `by_bucket`:    S3 destinations, keyed by bucket name.
///
/// Both maps share a single TTL timestamp. The cache expires 30 minutes after it
/// was last refreshed from the EC2 API — reading does not extend the TTL.
///
/// Since all flow logs are queried together with a single DescribeFlowLogs call,
/// the last_refreshed timestamp applies to the entire cache, not individual entries.
#[derive(Debug, Clone)]
pub struct FlowLogCache {
    by_log_group: HashMap<String, FlowLogConfig>,
    by_bucket: HashMap<String, FlowLogConfig>,
    /// Unix timestamp in seconds when the cache was last refreshed from EC2 API
    last_refreshed_secs: u64,
}

impl FlowLogCache {
    pub fn new() -> Self {
        Self {
            by_log_group: HashMap::new(),
            by_bucket: HashMap::new(),
            last_refreshed_secs: 0,
        }
    }

    // -----------------------------------------------------------------------
    // CloudWatch look-ups (keyed by log group name)
    // -----------------------------------------------------------------------

    /// Get a mutable reference to the flow log configuration for a CloudWatch log group.
    /// Returns `None` if not found or the cache is expired.
    ///
    /// Used for lazy initialisation of parsed fields.
    pub fn get_mut_by_log_group(&mut self, log_group: &str) -> Option<&mut FlowLogConfig> {
        if self.is_expired() {
            debug!("Cache expired");
            return None;
        }

        if let Some(config) = self.by_log_group.get_mut(log_group) {
            trace!(log_group = %log_group, "Cache hit mutable (by_log_group)");
            Some(config)
        } else {
            trace!(log_group = %log_group, "Cache miss (by_log_group)");
            None
        }
    }

    /// Insert or update a CloudWatch flow log configuration.
    pub fn insert_by_log_group(&mut self, log_group: String, config: FlowLogConfig) {
        debug!(
            log_group = %log_group,
            flow_log_id = %config.flow_log_id,
            "Inserting flow log config into cache (by_log_group)"
        );
        self.by_log_group.insert(log_group, config);
    }

    // -----------------------------------------------------------------------
    // S3 look-ups (keyed by bucket name)
    // -----------------------------------------------------------------------

    /// Get a mutable reference to the flow log configuration for an S3 bucket.
    /// Returns `None` if not found or the cache is expired.
    ///
    /// Used for lazy initialisation of parsed fields.
    pub fn get_mut_by_bucket(&mut self, bucket: &str) -> Option<&mut FlowLogConfig> {
        if self.is_expired() {
            debug!("Cache expired");
            return None;
        }

        if let Some(config) = self.by_bucket.get_mut(bucket) {
            trace!(bucket = %bucket, "Cache hit mutable (by_bucket)");
            Some(config)
        } else {
            trace!(bucket = %bucket, "Cache miss (by_bucket)");
            None
        }
    }

    /// Insert or update an S3 flow log configuration.
    pub fn insert_by_bucket(&mut self, bucket: String, config: FlowLogConfig) {
        debug!(
            bucket = %bucket,
            flow_log_id = %config.flow_log_id,
            "Inserting flow log config into cache (by_bucket)"
        );
        self.by_bucket.insert(bucket, config);
    }

    // -----------------------------------------------------------------------
    // TTL / lifecycle helpers
    // -----------------------------------------------------------------------

    /// Check if the entire cache is expired (older than 30 minutes).
    pub fn is_expired(&self) -> bool {
        if self.last_refreshed_secs == 0 {
            return true; // Never been refreshed
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let age_secs = now_secs.saturating_sub(self.last_refreshed_secs);
        age_secs > MAX_CACHE_AGE_SECS
    }

    /// Mark the cache as refreshed with the current timestamp.
    ///
    /// Should be called after successfully fetching flow logs from the EC2 API.
    /// Resets the 30-minute TTL for the entire cache.
    pub fn mark_refreshed(&mut self) {
        self.last_refreshed_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        debug!(
            timestamp = self.last_refreshed_secs,
            "Cache marked as refreshed"
        );
    }

    /// Get a serialisable snapshot of the current cache contents and timestamp.
    pub fn get_snapshot(&self) -> CacheSnapshot {
        CacheSnapshot {
            by_log_group: self.by_log_group.clone(),
            by_bucket: self.by_bucket.clone(),
            last_refreshed_secs: self.last_refreshed_secs,
        }
    }

    /// Restore cache contents from a snapshot (used when reloading from S3 persistence).
    ///
    /// Expired snapshots are silently ignored.
    pub fn load_snapshot(&mut self, snapshot: CacheSnapshot) {
        debug!(
            log_group_count = snapshot.by_log_group.len(),
            bucket_count = snapshot.by_bucket.len(),
            "Loading snapshot into cache"
        );

        if !snapshot.is_expired() {
            self.by_log_group = snapshot.by_log_group;
            self.by_bucket = snapshot.by_bucket;
            self.last_refreshed_secs = snapshot.last_refreshed_secs;
        } else {
            debug!("Snapshot is expired, not loading");
        }
    }

    /// Total number of cached entries across both destination maps.
    pub fn len(&self) -> usize {
        self.by_log_group.len() + self.by_bucket.len()
    }

    /// Returns `true` if both destination maps are empty.
    pub fn is_empty(&self) -> bool {
        self.by_log_group.is_empty() && self.by_bucket.is_empty()
    }

    /// Clear all entries from both maps and reset the timestamp.
    #[cfg(test)]
    pub fn clear(&mut self) {
        self.by_log_group.clear();
        self.by_bucket.clear();
        self.last_refreshed_secs = 0;
    }
}

/// Serialisable snapshot of the flow log cache for persistence (e.g. S3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSnapshot {
    /// CloudWatch flow logs: log_group_name → config
    #[serde(default)]
    pub by_log_group: HashMap<String, FlowLogConfig>,
    /// S3 flow logs: bucket_name → config
    #[serde(default)]
    pub by_bucket: HashMap<String, FlowLogConfig>,
    /// Unix timestamp (seconds) when the cache was last refreshed
    pub last_refreshed_secs: u64,
}

impl CacheSnapshot {
    /// Check if this snapshot is expired (older than 30 minutes).
    pub fn is_expired(&self) -> bool {
        if self.last_refreshed_secs == 0 {
            return true;
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let age_secs = now_secs.saturating_sub(self.last_refreshed_secs);
        age_secs > MAX_CACHE_AGE_SECS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ------------------------------------------------------------------
    // CloudWatch (by_log_group) tests
    // ------------------------------------------------------------------

    #[test]
    fn test_cache_insert_and_get_by_log_group() {
        let mut cache = FlowLogCache::new();
        let config = FlowLogConfig {
            log_format: "${version} ${account-id} ${interface-id}".to_string(),
            flow_log_id: "fl-1234567890abcdef0".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };

        cache.insert_by_log_group("/aws/ec2/flowlogs".to_string(), config.clone());
        cache.mark_refreshed();

        let retrieved = cache.get_mut_by_log_group("/aws/ec2/flowlogs");
        assert!(retrieved.is_some());
        assert_eq!(*retrieved.unwrap(), config);
    }

    #[test]
    fn test_cache_miss_by_log_group() {
        let mut cache = FlowLogCache::new();
        let retrieved = cache.get_mut_by_log_group("non-existent");
        assert!(retrieved.is_none());
    }

    // ------------------------------------------------------------------
    // S3 (by_bucket) tests
    // ------------------------------------------------------------------

    #[test]
    fn test_cache_insert_and_get_by_bucket() {
        let mut cache = FlowLogCache::new();
        let config = FlowLogConfig {
            log_format: "${version} ${account-id} ${interface-id}".to_string(),
            flow_log_id: "fl-s3-abc123".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };

        cache.insert_by_bucket("my-flow-logs-bucket".to_string(), config.clone());
        cache.mark_refreshed();

        let retrieved = cache.get_mut_by_bucket("my-flow-logs-bucket");
        assert!(retrieved.is_some());
        assert_eq!(*retrieved.unwrap(), config);
    }

    #[test]
    fn test_cache_miss_by_bucket() {
        let mut cache = FlowLogCache::new();
        let retrieved = cache.get_mut_by_bucket("non-existent-bucket");
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_cache_len_counts_both_maps() {
        let mut cache = FlowLogCache::new();

        let config = FlowLogConfig {
            log_format: "${version} ${account-id}".to_string(),
            flow_log_id: "fl-xxx".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };

        cache.insert_by_log_group("/aws/ec2/flowlogs".to_string(), config.clone());
        cache.insert_by_bucket("my-bucket".to_string(), config.clone());
        cache.mark_refreshed();

        assert_eq!(cache.len(), 2);
        assert!(!cache.is_empty());
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = FlowLogCache::new();
        let config = FlowLogConfig {
            log_format: "${version}".to_string(),
            flow_log_id: "fl-yyy".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };
        cache.insert_by_log_group("group".to_string(), config.clone());
        cache.insert_by_bucket("bucket".to_string(), config);
        cache.mark_refreshed();

        cache.clear();
        assert!(cache.is_empty());
        assert!(cache.is_expired());
    }

    // ------------------------------------------------------------------
    // Expiration tests
    // ------------------------------------------------------------------

    #[test]
    fn test_cache_expiration() {
        let mut cache = FlowLogCache::new();

        // New cache should be expired (never refreshed)
        assert!(cache.is_expired());

        // Mark as refreshed - should not be expired
        cache.mark_refreshed();
        assert!(!cache.is_expired());

        // Manually set old timestamp - should be expired
        cache.last_refreshed_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(31 * 60); // 31 minutes ago
        assert!(cache.is_expired());
    }

    // ------------------------------------------------------------------
    // Snapshot tests
    // ------------------------------------------------------------------

    #[test]
    fn test_snapshot_round_trip() {
        let mut cache = FlowLogCache::new();

        let cw_config = FlowLogConfig {
            log_format: "${version} ${account-id}".to_string(),
            flow_log_id: "fl-cw-111".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };
        let s3_config = FlowLogConfig {
            log_format: "${version} ${interface-id}".to_string(),
            flow_log_id: "fl-s3-222".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };

        cache.insert_by_log_group("/aws/ec2/flowlogs".to_string(), cw_config.clone());
        cache.insert_by_bucket("my-bucket".to_string(), s3_config.clone());
        cache.mark_refreshed();

        let snapshot = cache.get_snapshot();
        assert_eq!(snapshot.by_log_group.len(), 1);
        assert_eq!(snapshot.by_bucket.len(), 1);
        assert_eq!(
            snapshot.by_log_group.get("/aws/ec2/flowlogs").unwrap(),
            &cw_config
        );
        assert_eq!(snapshot.by_bucket.get("my-bucket").unwrap(), &s3_config);
    }

    #[test]
    fn test_load_snapshot() {
        let mut cache = FlowLogCache::new();

        let cw_config = FlowLogConfig {
            log_format: "${version} ${account-id}".to_string(),
            flow_log_id: "fl-cw-123".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };
        let s3_config = FlowLogConfig {
            log_format: "${version} ${srcaddr}".to_string(),
            flow_log_id: "fl-s3-456".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };

        let snapshot = CacheSnapshot {
            by_log_group: {
                let mut m = HashMap::new();
                m.insert("/aws/ec2/flowlogs".to_string(), cw_config.clone());
                m
            },
            by_bucket: {
                let mut m = HashMap::new();
                m.insert("my-bucket".to_string(), s3_config.clone());
                m
            },
            last_refreshed_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        cache.load_snapshot(snapshot);

        assert_eq!(
            *cache.get_mut_by_log_group("/aws/ec2/flowlogs").unwrap(),
            cw_config
        );
        assert_eq!(*cache.get_mut_by_bucket("my-bucket").unwrap(), s3_config);
    }

    #[test]
    fn test_load_expired_snapshot_is_ignored() {
        let mut cache = FlowLogCache::new();

        let config = FlowLogConfig {
            log_format: "${version}".to_string(),
            flow_log_id: "fl-old".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };

        let snapshot = CacheSnapshot {
            by_log_group: {
                let mut m = HashMap::new();
                m.insert("/old/group".to_string(), config);
                m
            },
            by_bucket: HashMap::new(),
            last_refreshed_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_sub(31 * 60), // expired
        };

        cache.load_snapshot(snapshot);

        // Cache should remain empty / expired
        assert!(cache.is_expired());
        assert!(cache.is_empty());
    }

    // ------------------------------------------------------------------
    // Tags test
    // ------------------------------------------------------------------

    #[test]
    fn test_flow_log_config_with_tags() {
        let mut cache = FlowLogCache::new();

        let mut tags = HashMap::new();
        tags.insert("Environment".to_string(), "production".to_string());
        tags.insert("Team".to_string(), "platform".to_string());
        tags.insert("Application".to_string(), "vpc-monitoring".to_string());

        let config = FlowLogConfig {
            log_format: "${version} ${account-id} ${interface-id}".to_string(),
            flow_log_id: "fl-1234567890abcdef0".to_string(),
            tags: tags.clone(),
            parsed_fields: None,
        };

        cache.insert_by_log_group("/aws/ec2/flowlogs".to_string(), config.clone());
        cache.mark_refreshed();

        let retrieved = cache.get_mut_by_log_group("/aws/ec2/flowlogs").unwrap();
        assert_eq!(retrieved.tags.len(), 3);
        assert_eq!(retrieved.tags.get("Environment").unwrap(), "production");
        assert_eq!(retrieved.tags.get("Team").unwrap(), "platform");
        assert_eq!(retrieved.tags.get("Application").unwrap(), "vpc-monitoring");
    }

    // ------------------------------------------------------------------
    // Field-type tests (unchanged from original)
    // ------------------------------------------------------------------

    #[test]
    fn test_parsed_field_type_mapping() {
        assert_eq!(get_field_type("version"), ParsedFieldType::Int32);
        assert_eq!(get_field_type("account-id"), ParsedFieldType::String);
        assert_eq!(get_field_type("srcport"), ParsedFieldType::Int32);
        assert_eq!(get_field_type("packets"), ParsedFieldType::Int64);
        assert_eq!(get_field_type("bytes"), ParsedFieldType::Int64);
        assert_eq!(get_field_type("action"), ParsedFieldType::String);
        assert_eq!(get_field_type("unknown-field"), ParsedFieldType::String);
    }

    #[test]
    fn test_parsed_field_creation() {
        let field = ParsedField::new("version".to_string(), ParsedFieldType::Int32);
        assert_eq!(field.field_name, "version");
        assert_eq!(field.field_type, ParsedFieldType::Int32);
    }

    #[test]
    fn test_parsed_fields_success() {
        let fields = vec![
            ParsedField::new("version".to_string(), ParsedFieldType::Int32),
            ParsedField::new("account-id".to_string(), ParsedFieldType::String),
        ];
        let parsed = ParsedFields::Success(fields.clone());
        assert_eq!(parsed, ParsedFields::Success(fields));
    }

    #[test]
    fn test_parsed_fields_error() {
        let error_msg = "Invalid format string".to_string();
        let parsed = ParsedFields::Error(error_msg.clone());
        assert_eq!(parsed, ParsedFields::Error(error_msg));
    }

    #[test]
    fn test_flow_log_config_with_parsed_fields() {
        let mut cache = FlowLogCache::new();

        let config = FlowLogConfig {
            log_format: "${version} ${account-id}".to_string(),
            flow_log_id: "fl-123".to_string(),
            tags: HashMap::new(),
            parsed_fields: Some(Arc::new(ParsedFields::Success(vec![
                ParsedField::new("version".to_string(), ParsedFieldType::Int32),
                ParsedField::new("account-id".to_string(), ParsedFieldType::String),
            ]))),
        };

        cache.insert_by_log_group("/aws/ec2/flowlogs".to_string(), config.clone());
        cache.mark_refreshed();

        let retrieved = cache.get_mut_by_log_group("/aws/ec2/flowlogs").unwrap();
        assert!(retrieved.parsed_fields.is_some());
        if let Some(parsed_fields) = &retrieved.parsed_fields {
            if let ParsedFields::Success(fields) = parsed_fields.as_ref() {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].field_name, "version");
                assert_eq!(fields[0].field_type, ParsedFieldType::Int32);
                assert_eq!(fields[1].field_name, "account-id");
                assert_eq!(fields[1].field_type, ParsedFieldType::String);
            } else {
                panic!("Expected ParsedFields::Success");
            }
        }
    }

    #[test]
    fn test_flow_log_config_with_parse_error() {
        let mut cache = FlowLogCache::new();

        let config = FlowLogConfig {
            log_format: "invalid format".to_string(),
            flow_log_id: "fl-123".to_string(),
            tags: HashMap::new(),
            parsed_fields: Some(Arc::new(ParsedFields::Error("Parse failed".to_string()))),
        };

        cache.insert_by_log_group("/aws/ec2/flowlogs".to_string(), config);
        cache.mark_refreshed();

        let retrieved = cache.get_mut_by_log_group("/aws/ec2/flowlogs").unwrap();
        if let Some(parsed_fields) = &retrieved.parsed_fields {
            if let ParsedFields::Error(msg) = parsed_fields.as_ref() {
                assert_eq!(msg, "Parse failed");
            } else {
                panic!("Expected ParsedFields::Error");
            }
        } else {
            panic!("Expected Some(parsed_fields)");
        }
    }
}
