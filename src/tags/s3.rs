use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use thiserror::Error;
use tracing::{debug, error, info};

use super::cache::CacheEntry;

/// S3 cache key for storing tags
const CACHE_KEY: &str = "rotel-lambda-forwarder/cache/log-groups/tags.json.gz";

/// Errors that can occur during S3 operations
#[derive(Debug, Error)]
pub enum S3Error {
    #[error("S3 operation failed: {0}")]
    S3Operation(String),

    #[error("Compression failed: {0}")]
    Compression(String),

    #[error("Decompression failed: {0}")]
    Decompression(String),

    #[error("Serialization failed: {0}")]
    Serialization(String),

    #[error("Deserialization failed: {0}")]
    Deserialization(String),

    #[error("Cache file not found")]
    NotFound,

    #[error("Conditional write failed - cache was modified")]
    ConditionalWriteFailed,

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}

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

/// S3 persistence layer for tag cache
pub struct S3Cache {
    client: S3Client,
    bucket: String,
    /// Current ETag of the cache file, if known
    current_etag: Option<String>,
}

impl S3Cache {
    /// Create a new S3 cache
    pub fn new(client: S3Client, bucket: String) -> Self {
        Self {
            client,
            bucket,
            current_etag: None,
        }
    }

    /// Load the cache from S3
    /// Returns None if the cache file doesn't exist
    pub async fn load(&mut self) -> Result<Option<HashMap<String, CacheEntry>>, S3Error> {
        debug!(bucket = %self.bucket, key = %CACHE_KEY, "Loading cache from S3");

        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(CACHE_KEY)
            .send()
            .await;

        match result {
            Ok(output) => {
                // Store the ETag for later conditional writes
                self.current_etag = output.e_tag().map(|s| s.to_string());

                let body = output
                    .body
                    .collect()
                    .await
                    .map_err(|e| S3Error::S3Operation(format!("Failed to read body: {}", e)))?;

                let bytes = body.into_bytes();

                // Decompress the data
                let cache_file = decompress_and_deserialize(&bytes)?;

                info!(
                    bucket = %self.bucket,
                    key = %CACHE_KEY,
                    entry_count = cache_file.log_groups.len(),
                    etag = ?self.current_etag,
                    "Successfully loaded cache from S3"
                );

                Ok(Some(cache_file.log_groups))
            }
            Err(e) => {
                let err_msg = format!("{:?}", e);
                if err_msg.contains("NoSuchKey") || err_msg.contains("NotFound") {
                    debug!(bucket = %self.bucket, key = %CACHE_KEY, "Cache file not found in S3");
                    Ok(None)
                } else {
                    error!(bucket = %self.bucket, key = %CACHE_KEY, error = %err_msg, "Failed to load cache from S3");
                    Err(S3Error::S3Operation(err_msg))
                }
            }
        }
    }

    /// Save the cache to S3 with conditional write
    /// If the write fails due to ETag mismatch, returns an error
    pub async fn save(&mut self, cache_data: HashMap<String, CacheEntry>) -> Result<(), S3Error> {
        debug!(
            bucket = %self.bucket,
            key = %CACHE_KEY,
            entry_count = cache_data.len(),
            "Saving cache to S3"
        );

        let cache_file = CacheFile::new(cache_data);

        // Serialize and compress the data
        let compressed_data = serialize_and_compress(&cache_file)?;

        // Prepare the put request with conditional write if we have an ETag
        let mut put_request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(CACHE_KEY)
            .body(ByteStream::from(compressed_data))
            .content_type("application/gzip");

        // Add conditional write based on ETag if we have one, otherwise
        // do a conditional write on the non-existence of the file
        if let Some(ref etag) = self.current_etag {
            debug!(etag = %etag, "Using conditional write with ETag");
            put_request = put_request.if_match(etag);
        } else {
            debug!("Using conditional write with if-none-match");
            put_request = put_request.if_none_match("*");
        }

        match put_request.send().await {
            Ok(output) => {
                // Update our stored ETag
                self.current_etag = output.e_tag().map(|s| s.to_string());

                debug!(
                    bucket = %self.bucket,
                    key = %CACHE_KEY,
                    entry_count = cache_file.log_groups.len(),
                    new_etag = ?self.current_etag,
                    "Successfully saved cache to S3"
                );
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("{:?}", e);

                // Check if this is a conditional write failure (ETag mismatch)
                if err_msg.contains("PreconditionFailed") || err_msg.contains("IfMatch") {
                    info!(
                        bucket = %self.bucket,
                        key = %CACHE_KEY,
                        "Conditional write conflict to S3 cache file, will retry"
                    );
                    Err(S3Error::ConditionalWriteFailed)
                } else {
                    error!(
                        bucket = %self.bucket,
                        key = %CACHE_KEY,
                        error = %err_msg,
                        "Failed to save cache to S3"
                    );
                    Err(S3Error::S3Operation(err_msg))
                }
            }
        }
    }

    /// Reload the cache from S3 and merge with the provided data
    /// This is used when a conditional write fails
    /// Keeps the entry with the most recent last_seen timestamp
    pub async fn reload_and_merge(
        &mut self,
        local_snapshot: HashMap<String, CacheEntry>,
    ) -> Result<HashMap<String, CacheEntry>, S3Error> {
        debug!("Reloading cache from S3 to merge with new data");

        // Load the current cache from S3
        let from_s3 = self.load().await?;

        // Merge the data, keeping the most recent entry for each log group
        let mut merged = from_s3.unwrap_or_default();
        for (log_group, new_entry) in local_snapshot {
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
            } else {
                debug!(log_group = %log_group, "Keeping existing entry (more recent)");
            }
        }

        debug!(merged_count = merged.len(), "Merged cache data");
        Ok(merged)
    }
}

/// Serialize the cache file and compress with gzip
fn serialize_and_compress(cache_file: &CacheFile) -> Result<Vec<u8>, S3Error> {
    // Serialize to JSON
    let json = serde_json::to_vec(cache_file).map_err(|e| S3Error::Serialization(e.to_string()))?;

    // Compress with gzip
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&json)
        .map_err(|e| S3Error::Compression(e.to_string()))?;

    encoder
        .finish()
        .map_err(|e| S3Error::Compression(e.to_string()))
}

/// Decompress gzip data and deserialize the cache file
fn decompress_and_deserialize(compressed_data: &[u8]) -> Result<CacheFile, S3Error> {
    // Decompress
    let mut decoder = GzDecoder::new(compressed_data);
    let mut json = Vec::new();
    decoder
        .read_to_end(&mut json)
        .map_err(|e| S3Error::Decompression(e.to_string()))?;

    // Deserialize
    let cache_file: CacheFile =
        serde_json::from_slice(&json).map_err(|e| S3Error::Deserialization(e.to_string()))?;

    Ok(cache_file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_and_compress() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut log_groups = HashMap::new();
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        let entry = CacheEntry {
            tags,
            last_seen_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        log_groups.insert("log-group-1".to_string(), entry);

        let cache_file = CacheFile::new(log_groups);
        let result = serialize_and_compress(&cache_file);
        assert!(result.is_ok());

        let compressed = result.unwrap();
        assert!(!compressed.is_empty());
    }

    #[test]
    fn test_decompress_and_deserialize() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut log_groups = HashMap::new();
        let mut tags = HashMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        let entry = CacheEntry {
            tags: tags.clone(),
            last_seen_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        log_groups.insert("log-group-1".to_string(), entry.clone());

        let cache_file = CacheFile::new(log_groups.clone());
        let compressed = serialize_and_compress(&cache_file).unwrap();

        let result = decompress_and_deserialize(&compressed);
        assert!(result.is_ok());

        let decompressed = result.unwrap();
        assert_eq!(decompressed.version, 1);
        assert_eq!(
            decompressed.log_groups.get("log-group-1").unwrap().tags,
            tags
        );
    }

    #[test]
    fn test_round_trip() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut log_groups = HashMap::new();

        let mut tags1 = HashMap::new();
        tags1.insert("env".to_string(), "prod".to_string());
        tags1.insert("team".to_string(), "platform".to_string());
        let entry1 = CacheEntry {
            tags: tags1.clone(),
            last_seen_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        log_groups.insert("log-group-1".to_string(), entry1);

        let mut tags2 = HashMap::new();
        tags2.insert("env".to_string(), "dev".to_string());
        let entry2 = CacheEntry {
            tags: tags2.clone(),
            last_seen_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        log_groups.insert("log-group-2".to_string(), entry2);

        let cache_file = CacheFile::new(log_groups.clone());
        let compressed = serialize_and_compress(&cache_file).unwrap();
        let decompressed = decompress_and_deserialize(&compressed).unwrap();

        assert_eq!(decompressed.log_groups.len(), log_groups.len());
        assert_eq!(
            decompressed.log_groups.get("log-group-1").unwrap().tags,
            tags1
        );
        assert_eq!(
            decompressed.log_groups.get("log-group-2").unwrap().tags,
            tags2
        );
    }
}
