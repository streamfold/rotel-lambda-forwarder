use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, trace};

/// Maximum age for cached entries (30 minutes in seconds)
const MAX_CACHE_AGE_SECS: u64 = 30 * 60;

/// Data type for a flow log field, based on Parquet data types from AWS documentation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedFieldType {
    String,
    Int32,
    Int64,
}

/// A parsed field from a flow log format string with its name and type
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedField {
    /// The field name (e.g., "version", "account-id")
    pub field_name: String,
    /// The data type of this field
    pub field_type: ParsedFieldType,
}

impl ParsedField {
    /// Create a new ParsedField
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

/// Flow log configuration for a specific log group
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlowLogConfig {
    /// The log format string (e.g., "${version} ${account-id} ${interface-id} ...")
    pub log_format: String,
    /// The destination type (e.g., "cloud-watch-logs", "s3")
    pub destination_type: String,
    /// The flow log ID
    pub flow_log_id: String,
    /// Tags associated with the flow log
    pub tags: std::collections::HashMap<String, String>,
    /// Parsed field names from the log format (lazily computed, not serialized)
    #[serde(skip)]
    pub parsed_fields: Option<Arc<ParsedFields>>, // Use an Arc to reduce clone costs
}

/// In-memory cache for flow log configurations
///
/// Cache is refreshed when configurations are fetched from EC2 API.
/// Reading from the cache does not extend the TTL - the cache expires 30 minutes
/// after it was last refreshed from the API.
///
/// Since all flow logs are queried together with a single DescribeFlowLogs call,
/// the last_seen timestamp applies to the entire cache, not individual entries.
#[derive(Debug, Clone)]
pub struct FlowLogCache {
    inner: HashMap<String, FlowLogConfig>,
    /// Unix timestamp in seconds when the cache was last refreshed from EC2 API
    last_refreshed_secs: u64,
}

impl FlowLogCache {
    /// Create a new empty flow log cache
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
            last_refreshed_secs: 0,
        }
    }

    /// Get flow log configuration for a log group if it exists and cache is not expired
    /// Returns None if not found or cache is expired
    ///
    /// Note: This does not update the timestamp - cache expires 30 minutes
    /// after it was last refreshed from the API.
    pub fn get(&self, log_group: &str) -> Option<FlowLogConfig> {
        if self.is_expired() {
            debug!("Cache expired");
            return None;
        }

        if let Some(config) = self.inner.get(log_group) {
            trace!(log_group = %log_group, "Cache hit");
            Some(config.clone())
        } else {
            trace!(log_group = %log_group, "Cache miss");
            None
        }
    }

    /// Get mutable reference to flow log configuration for a log group
    /// Returns None if not found or cache is expired
    ///
    /// This is used for lazy initialization of parsed fields.
    pub fn get_mut(&mut self, log_group: &str) -> Option<&mut FlowLogConfig> {
        if self.is_expired() {
            debug!("Cache expired");
            return None;
        }

        if let Some(config) = self.inner.get_mut(log_group) {
            trace!(log_group = %log_group, "Cache hit (mutable)");
            Some(config)
        } else {
            trace!(log_group = %log_group, "Cache miss");
            None
        }
    }

    /// Check if the entire cache is expired (older than 30 minutes)
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

    /// Insert or update flow log configuration for a log group
    ///
    /// Should only be called when configuration is freshly fetched from EC2 API.
    pub fn insert(&mut self, log_group: String, config: FlowLogConfig) {
        debug!(log_group = %log_group, flow_log_id = %config.flow_log_id, "Inserting flow log config into cache");
        self.inner.insert(log_group, config);
    }

    /// Mark the cache as refreshed with the current timestamp
    ///
    /// This should be called after successfully fetching flow logs from EC2 API.
    /// It resets the 30-minute TTL for the entire cache.
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

    /// Get a snapshot of the current cache with timestamp
    pub fn get_snapshot(&self) -> CacheSnapshot {
        CacheSnapshot {
            flow_logs: self.inner.clone(),
            last_refreshed_secs: self.last_refreshed_secs,
        }
    }

    /// Load entries from a snapshot (used when restoring from S3)
    pub fn load_snapshot(&mut self, snapshot: CacheSnapshot) {
        debug!(
            entry_count = snapshot.flow_logs.len(),
            "Loading snapshot into cache"
        );

        if !snapshot.is_expired() {
            self.inner = snapshot.flow_logs;
            self.last_refreshed_secs = snapshot.last_refreshed_secs;
        } else {
            debug!("Snapshot is expired, not loading");
        }
    }

    /// Get the number of entries in the cache
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clear all entries from the cache and reset timestamp
    #[cfg(test)]
    pub fn clear(&mut self) {
        self.inner.clear();
        self.last_refreshed_secs = 0;
    }
}

/// Snapshot of the flow log cache for serialization/deserialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSnapshot {
    /// Map of log group names to their flow log configurations
    pub flow_logs: HashMap<String, FlowLogConfig>,
    /// Unix timestamp in seconds when the cache was last refreshed
    pub last_refreshed_secs: u64,
}

impl CacheSnapshot {
    /// Check if this snapshot is expired (older than 30 minutes)
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

    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = FlowLogCache::new();
        let config = FlowLogConfig {
            log_format: "${version} ${account-id} ${interface-id}".to_string(),
            destination_type: "cloud-watch-logs".to_string(),
            flow_log_id: "fl-1234567890abcdef0".to_string(),
            tags: std::collections::HashMap::new(),
            parsed_fields: None,
        };

        cache.insert("/aws/ec2/flowlogs".to_string(), config.clone());
        cache.mark_refreshed();

        let retrieved = cache.get("/aws/ec2/flowlogs");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), config);
    }

    #[test]
    fn test_cache_miss() {
        let cache = FlowLogCache::new();
        let retrieved = cache.get("non-existent");
        assert!(retrieved.is_none());
    }

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

    #[test]
    fn test_snapshot() {
        let mut cache = FlowLogCache::new();

        let config1 = FlowLogConfig {
            log_format: "${version} ${account-id}".to_string(),
            destination_type: "cloud-watch-logs".to_string(),
            flow_log_id: "fl-111".to_string(),
            tags: std::collections::HashMap::new(),
            parsed_fields: None,
        };
        cache.insert("/aws/ec2/flowlogs1".to_string(), config1.clone());

        let config2 = FlowLogConfig {
            log_format: "${version} ${interface-id}".to_string(),
            destination_type: "s3".to_string(),
            flow_log_id: "fl-222".to_string(),
            tags: std::collections::HashMap::new(),
            parsed_fields: None,
        };
        cache.insert("/aws/ec2/flowlogs2".to_string(), config2.clone());
        cache.mark_refreshed();

        let snapshot = cache.get_snapshot();
        assert_eq!(snapshot.flow_logs.len(), 2);
        assert_eq!(
            snapshot.flow_logs.get("/aws/ec2/flowlogs1").unwrap(),
            &config1
        );
        assert_eq!(
            snapshot.flow_logs.get("/aws/ec2/flowlogs2").unwrap(),
            &config2
        );
    }

    #[test]
    fn test_load_snapshot() {
        let mut cache = FlowLogCache::new();

        let config = FlowLogConfig {
            log_format: "${version} ${account-id}".to_string(),
            destination_type: "cloud-watch-logs".to_string(),
            flow_log_id: "fl-123".to_string(),
            tags: std::collections::HashMap::new(),
            parsed_fields: None,
        };
        let snapshot = CacheSnapshot {
            flow_logs: {
                let mut map = HashMap::new();
                map.insert("/aws/ec2/flowlogs".to_string(), config.clone());
                map
            },
            last_refreshed_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        cache.load_snapshot(snapshot);

        let retrieved = cache.get("/aws/ec2/flowlogs");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), config);
    }

    #[test]
    fn test_flow_log_config_with_tags() {
        let mut cache = FlowLogCache::new();

        // Create config with tags
        let mut tags = std::collections::HashMap::new();
        tags.insert("Environment".to_string(), "production".to_string());
        tags.insert("Team".to_string(), "platform".to_string());
        tags.insert("Application".to_string(), "vpc-monitoring".to_string());

        let config = FlowLogConfig {
            log_format: "${version} ${account-id} ${interface-id}".to_string(),
            destination_type: "cloud-watch-logs".to_string(),
            flow_log_id: "fl-1234567890abcdef0".to_string(),
            tags: tags.clone(),
            parsed_fields: None,
        };

        cache.insert("/aws/ec2/flowlogs".to_string(), config.clone());
        cache.mark_refreshed();

        let retrieved = cache.get("/aws/ec2/flowlogs");
        assert!(retrieved.is_some());

        let retrieved_config = retrieved.unwrap();
        assert_eq!(retrieved_config.tags.len(), 3);
        assert_eq!(
            retrieved_config.tags.get("Environment").unwrap(),
            "production"
        );
        assert_eq!(retrieved_config.tags.get("Team").unwrap(), "platform");
        assert_eq!(
            retrieved_config.tags.get("Application").unwrap(),
            "vpc-monitoring"
        );
    }

    #[test]
    fn test_parsed_field_type_mapping() {
        // Test some known fields
        assert_eq!(get_field_type("version"), ParsedFieldType::Int32);
        assert_eq!(get_field_type("account-id"), ParsedFieldType::String);
        assert_eq!(get_field_type("srcport"), ParsedFieldType::Int32);
        assert_eq!(get_field_type("packets"), ParsedFieldType::Int64);
        assert_eq!(get_field_type("bytes"), ParsedFieldType::Int64);
        assert_eq!(get_field_type("action"), ParsedFieldType::String);

        // Test unknown field defaults to String
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
            destination_type: "cloud-watch-logs".to_string(),
            flow_log_id: "fl-123".to_string(),
            tags: std::collections::HashMap::new(),
            parsed_fields: Some(Arc::new(ParsedFields::Success(vec![
                ParsedField::new("version".to_string(), ParsedFieldType::Int32),
                ParsedField::new("account-id".to_string(), ParsedFieldType::String),
            ]))),
        };

        cache.insert("/aws/ec2/flowlogs".to_string(), config.clone());
        cache.mark_refreshed();

        let retrieved = cache.get("/aws/ec2/flowlogs");
        assert!(retrieved.is_some());

        let retrieved_config = retrieved.unwrap();
        assert!(retrieved_config.parsed_fields.is_some());
        if let Some(parsed_fields) = &retrieved_config.parsed_fields {
            if let ParsedFields::Success(fields) = parsed_fields.as_ref() {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].field_name, "version");
                assert_eq!(fields[0].field_type, ParsedFieldType::Int32);
                assert_eq!(fields[1].field_name, "account-id");
                assert_eq!(fields[1].field_type, ParsedFieldType::String);
            } else {
                panic!("Expected ParsedFields::Success");
            }
        } else {
            panic!("Expected Some(parsed_fields)");
        }
    }

    #[test]
    fn test_flow_log_config_with_parse_error() {
        let mut cache = FlowLogCache::new();

        let config = FlowLogConfig {
            log_format: "invalid format".to_string(),
            destination_type: "cloud-watch-logs".to_string(),
            flow_log_id: "fl-123".to_string(),
            tags: std::collections::HashMap::new(),
            parsed_fields: Some(Arc::new(ParsedFields::Error("Parse failed".to_string()))),
        };

        cache.insert("/aws/ec2/flowlogs".to_string(), config.clone());
        cache.mark_refreshed();

        let retrieved = cache.get("/aws/ec2/flowlogs");
        assert!(retrieved.is_some());

        let retrieved_config = retrieved.unwrap();
        assert!(retrieved_config.parsed_fields.is_some());

        if let Some(parsed_fields) = &retrieved_config.parsed_fields {
            if let ParsedFields::Error(msg) = parsed_fields.as_ref() {
                assert_eq!(msg, "Parse failed");
            } else {
                panic!("Expected ParsedFields::Error")
            }
        } else {
            panic!("Expected Some(parsed_fields)");
        }
    }
}
