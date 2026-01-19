use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, trace};

/// Maximum age for cached entries (15 minutes in seconds)
const MAX_CACHE_AGE_SECS: u64 = 15 * 60;

/// Entry in the tag cache with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// Tags for the log group
    pub tags: HashMap<String, String>,
    /// Unix timestamp in seconds when this entry was last seen
    pub last_seen_secs: u64,
}

impl CacheEntry {
    /// Create a new cache entry with current timestamp
    pub fn new(tags: HashMap<String, String>) -> Self {
        let last_seen_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            tags,
            last_seen_secs,
        }
    }

    /// Check if this entry is expired (older than 15 minutes)
    pub fn is_expired(&self) -> bool {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let age_secs = now_secs.saturating_sub(self.last_seen_secs);
        age_secs > MAX_CACHE_AGE_SECS
    }
}

/// In-memory cache for log group tags
///
/// Cache entries are only updated when tags are refreshed from CloudWatch API.
/// Reading from the cache does not extend the TTL - entries expire 15 minutes
/// after they were last refreshed from the API.
#[derive(Debug, Clone)]
pub struct TagCache {
    inner: HashMap<String, CacheEntry>,
}

impl TagCache {
    /// Create a new empty tag cache
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Get tags for a log group if they exist and are not expired
    /// Returns None if not found or expired
    ///
    /// Note: This does not update the timestamp - entries expire 15 minutes
    /// after they were last inserted/updated from the API.
    pub fn get(&self, log_group: &str) -> Option<HashMap<String, String>> {
        if let Some(entry) = self.inner.get(log_group) {
            if entry.is_expired() {
                debug!(log_group = %log_group, "Cache entry expired");
                None
            } else {
                trace!(log_group = %log_group, "Cache hit");
                Some(entry.tags.clone())
            }
        } else {
            trace!(log_group = %log_group, "Cache miss");
            None
        }
    }

    /// Insert or update tags for a log group
    ///
    /// This updates the timestamp, resetting the 15-minute TTL.
    /// Should only be called when tags are freshly fetched from CloudWatch API.
    pub fn insert(&mut self, log_group: String, tags: HashMap<String, String>) {
        debug!(log_group = %log_group, tag_count = tags.len(), "Inserting tags into cache");
        self.inner.insert(log_group, CacheEntry::new(tags));
    }

    /// Remove expired entries and return a snapshot of the current cache
    pub fn get_snapshot(&self) -> HashMap<String, CacheEntry> {
        // Return a snapshot of non-expired entries
        self.inner
            .iter()
            .filter(|(log_group, entry)| {
                if entry.is_expired() {
                    debug!(log_group = %log_group, "Skipping expired entry in snapshot");
                    false
                } else {
                    true
                }
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Load entries from a snapshot (used when restoring from S3)
    pub fn load_snapshot(&mut self, snapshot: HashMap<String, CacheEntry>) {
        debug!(entry_count = snapshot.len(), "Loading snapshot into cache");

        for (log_group, entry) in snapshot {
            if !entry.is_expired() {
                self.inner.insert(log_group, entry);
            }
        }
    }

    /// Merge a snapshot with the current cache, keeping entries with most recent last_seen
    pub fn merge_snapshot(&mut self, snapshot: HashMap<String, CacheEntry>) {
        debug!(
            incoming_count = snapshot.len(),
            current_count = self.inner.len(),
            "Merging snapshot into cache"
        );

        for (log_group, incoming_entry) in snapshot {
            let should_update = if let Some(existing_entry) = self.inner.get(&log_group) {
                // Keep the entry with the most recent last_seen time
                incoming_entry.last_seen_secs > existing_entry.last_seen_secs
            } else {
                // New entry, always insert
                true
            };

            if should_update {
                debug!(log_group = %log_group, "Updating cache entry with more recent data");
                self.inner.insert(log_group, incoming_entry);
            } else {
                trace!(log_group = %log_group, "Keeping existing cache entry (more recent)");
            }
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

    /// Clear all entries from the cache
    #[cfg(test)]
    pub fn clear(&mut self) {
        self.inner.clear()
    }
}

impl Default for TagCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_insert_and_get() {
        let mut cache = TagCache::new();
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "prod".to_string());

        cache.insert("log-group-1".to_string(), tags.clone());

        let retrieved = cache.get("log-group-1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), tags);
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = TagCache::new();
        let retrieved = cache.get("non-existent");
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        // Test with current time - should not be expired
        let entry = CacheEntry::new(HashMap::new());
        assert!(!entry.is_expired());

        // Test with old timestamp - should be expired
        let mut old_entry = CacheEntry::new(HashMap::new());
        old_entry.last_seen_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .saturating_sub(16 * 60); // 16 minutes ago
        assert!(old_entry.is_expired());
    }

    #[tokio::test]
    async fn test_snapshot() {
        let mut cache = TagCache::new();

        let mut tags1 = HashMap::new();
        tags1.insert("env".to_string(), "prod".to_string());
        cache.insert("log-group-1".to_string(), tags1.clone());

        let mut tags2 = HashMap::new();
        tags2.insert("env".to_string(), "dev".to_string());
        cache.insert("log-group-2".to_string(), tags2.clone());

        let snapshot = cache.get_snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get("log-group-1").unwrap().tags, tags1);
        assert_eq!(snapshot.get("log-group-2").unwrap().tags, tags2);
    }

    #[tokio::test]
    async fn test_load_snapshot() {
        let mut cache = TagCache::new();

        let mut snapshot = HashMap::new();
        let mut tags1 = HashMap::new();
        tags1.insert("env".to_string(), "prod".to_string());
        let entry = CacheEntry {
            tags: tags1.clone(),
            last_seen_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        snapshot.insert("log-group-1".to_string(), entry);

        cache.load_snapshot(snapshot);

        let retrieved = cache.get("log-group-1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), tags1);
    }

    #[tokio::test]
    async fn test_merge_snapshot_keeps_most_recent() {
        let mut cache = TagCache::new();

        // Insert an entry with current time
        let mut tags1 = HashMap::new();
        tags1.insert("env".to_string(), "prod".to_string());
        cache.insert("log-group-1".to_string(), tags1.clone());

        // Create a snapshot with an older entry
        let mut snapshot = HashMap::new();
        let mut tags2 = HashMap::new();
        tags2.insert("env".to_string(), "staging".to_string());
        let old_entry = CacheEntry {
            tags: tags2.clone(),
            last_seen_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_sub(60), // 60 seconds in the past
        };
        snapshot.insert("log-group-1".to_string(), old_entry);

        // Merge should keep the current entry (more recent)
        cache.merge_snapshot(snapshot);

        let retrieved = cache.get("log-group-1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), tags1); // Should still be prod, not staging
    }

    #[tokio::test]
    async fn test_merge_snapshot_updates_with_newer() {
        let mut cache = TagCache::new();

        // Insert an old entry
        let mut tags1 = HashMap::new();
        tags1.insert("env".to_string(), "prod".to_string());
        let old_entry = CacheEntry {
            tags: tags1.clone(),
            last_seen_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_sub(120), // 120 seconds in the past
        };
        cache.inner.insert("log-group-1".to_string(), old_entry);

        // Create a snapshot with a newer entry
        let mut snapshot = HashMap::new();
        let mut tags2 = HashMap::new();
        tags2.insert("env".to_string(), "staging".to_string());
        let new_entry = CacheEntry {
            tags: tags2.clone(),
            last_seen_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        snapshot.insert("log-group-1".to_string(), new_entry);

        // Merge should update with the newer entry
        cache.merge_snapshot(snapshot);

        let retrieved = cache.get("log-group-1");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), tags2); // Should be staging now
    }

    #[tokio::test]
    async fn test_get_does_not_extend_ttl() {
        let mut cache = TagCache::new();
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "prod".to_string());

        cache.insert("log-group-1".to_string(), tags.clone());

        // Getting from cache should not extend TTL
        let retrieved = cache.get("log-group-1");
        assert!(retrieved.is_some());

        // Entry should still be valid since it hasn't expired yet
        let retrieved = cache.get("log-group-1");
        assert!(retrieved.is_some());
    }
}
