//! Generic S3 Cache Module
//!
//! This module provides a reusable S3 persistence layer that can be used for different
//! types of cache data (tags, flow logs, etc.). It handles compression, serialization,
//! conditional writes, and conflict resolution.

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use thiserror::Error;
use tracing::{debug, error, info};

/// Errors that can occur during S3 operations
#[derive(Debug, Error)]
pub enum S3CacheError {
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

/// Generic S3 persistence layer for cache data
///
/// This provides a reusable S3 caching mechanism that can store any serializable data.
/// It handles compression, ETags for conditional writes, and conflict resolution.
pub struct S3Cache<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    client: S3Client,
    bucket: String,
    key: String,
    /// Current ETag of the cache file, if known
    current_etag: Option<String>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> S3Cache<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    /// Create a new S3 cache
    ///
    /// # Arguments
    /// * `client` - AWS S3 client
    /// * `bucket` - S3 bucket name
    /// * `key` - S3 object key for the cache file
    pub fn new(client: S3Client, bucket: String, key: String) -> Self {
        Self {
            client,
            bucket,
            key,
            current_etag: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Load the cache from S3
    /// Returns None if the cache file doesn't exist
    pub async fn load(&mut self) -> Result<Option<T>, S3CacheError> {
        debug!(bucket = %self.bucket, key = %self.key, "Loading cache from S3");

        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .send()
            .await;

        match result {
            Ok(output) => {
                // Store the ETag for later conditional writes
                self.current_etag = output.e_tag().map(|s| s.to_string());

                let body = output.body.collect().await.map_err(|e| {
                    S3CacheError::S3Operation(format!("Failed to read body: {}", e))
                })?;

                let bytes = body.into_bytes();

                // Decompress and deserialize the data
                let data = decompress_and_deserialize(&bytes)?;

                info!(
                    bucket = %self.bucket,
                    key = %self.key,
                    etag = ?self.current_etag,
                    "Successfully loaded cache from S3"
                );

                Ok(Some(data))
            }
            Err(e) => {
                let err_msg = format!("{:?}", e);
                if err_msg.contains("NoSuchKey") || err_msg.contains("NotFound") {
                    debug!(bucket = %self.bucket, key = %self.key, "Cache file not found in S3");
                    Ok(None)
                } else {
                    error!(bucket = %self.bucket, key = %self.key, error = %err_msg, "Failed to load cache from S3");
                    Err(S3CacheError::S3Operation(err_msg))
                }
            }
        }
    }

    /// Save the cache to S3 with conditional write
    /// If the write fails due to ETag mismatch, returns ConditionalWriteFailed error
    pub async fn save(&mut self, data: T) -> Result<(), S3CacheError> {
        debug!(
            bucket = %self.bucket,
            key = %self.key,
            "Saving cache to S3"
        );

        // Serialize and compress the data
        let compressed_data = serialize_and_compress(&data)?;

        // Prepare the put request with conditional write if we have an ETag
        let mut put_request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
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
                    key = %self.key,
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
                        key = %self.key,
                        "Conditional write conflict to S3 cache file, will retry"
                    );
                    Err(S3CacheError::ConditionalWriteFailed)
                } else {
                    error!(
                        bucket = %self.bucket,
                        key = %self.key,
                        error = %err_msg,
                        "Failed to save cache to S3"
                    );
                    Err(S3CacheError::S3Operation(err_msg))
                }
            }
        }
    }

    /// Reload the cache from S3 and merge with the provided data using a merge function
    /// This is used when a conditional write fails
    ///
    /// # Arguments
    /// * `local_data` - The local data to merge
    /// * `merge_fn` - Function that takes (from_s3, local) and returns merged data
    pub async fn reload_and_merge<F>(
        &mut self,
        local_data: T,
        merge_fn: F,
    ) -> Result<T, S3CacheError>
    where
        F: FnOnce(T, T) -> T,
    {
        debug!("Reloading cache from S3 to merge with new data");

        // Load the current cache from S3
        let from_s3 = self.load().await?;

        // If nothing in S3, just use local data
        let from_s3 = match from_s3 {
            Some(data) => data,
            None => {
                debug!("No data in S3, using local data only");
                return Ok(local_data);
            }
        };

        // Merge the data using the provided function
        let merged = merge_fn(from_s3, local_data);

        debug!("Merged cache data");
        Ok(merged)
    }

    /// Get the current ETag if known
    pub fn etag(&self) -> Option<&str> {
        self.current_etag.as_deref()
    }

    /// Get the S3 bucket name
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the S3 key
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// Serialize data and compress with gzip
fn serialize_and_compress<T>(data: &T) -> Result<Vec<u8>, S3CacheError>
where
    T: Serialize,
{
    // Serialize to JSON
    let json = serde_json::to_vec(data).map_err(|e| S3CacheError::Serialization(e.to_string()))?;

    // Compress with gzip
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&json)
        .map_err(|e| S3CacheError::Compression(e.to_string()))?;

    encoder
        .finish()
        .map_err(|e| S3CacheError::Compression(e.to_string()))
}

/// Decompress gzip data and deserialize
fn decompress_and_deserialize<T>(compressed_data: &[u8]) -> Result<T, S3CacheError>
where
    T: for<'de> Deserialize<'de>,
{
    // Decompress
    let mut decoder = GzDecoder::new(compressed_data);
    let mut json = Vec::new();
    decoder
        .read_to_end(&mut json)
        .map_err(|e| S3CacheError::Decompression(e.to_string()))?;

    // Deserialize
    let data: T =
        serde_json::from_slice(&json).map_err(|e| S3CacheError::Deserialization(e.to_string()))?;

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        items: HashMap<String, String>,
        count: u32,
    }

    #[test]
    fn test_serialize_and_compress() {
        let mut items = HashMap::new();
        items.insert("key1".to_string(), "value1".to_string());
        items.insert("key2".to_string(), "value2".to_string());

        let data = TestData { items, count: 42 };

        let result = serialize_and_compress(&data);
        assert!(result.is_ok());

        let compressed = result.unwrap();
        assert!(!compressed.is_empty());
    }

    #[test]
    fn test_decompress_and_deserialize() {
        let mut items = HashMap::new();
        items.insert("key1".to_string(), "value1".to_string());
        items.insert("key2".to_string(), "value2".to_string());

        let data = TestData {
            items: items.clone(),
            count: 42,
        };

        let compressed = serialize_and_compress(&data).unwrap();
        let result: Result<TestData, S3CacheError> = decompress_and_deserialize(&compressed);
        assert!(result.is_ok());

        let decompressed = result.unwrap();
        assert_eq!(decompressed.items, items);
        assert_eq!(decompressed.count, 42);
    }

    #[test]
    fn test_round_trip() {
        let mut items = HashMap::new();
        items.insert("env".to_string(), "prod".to_string());
        items.insert("team".to_string(), "platform".to_string());

        let data = TestData { items, count: 100 };

        let compressed = serialize_and_compress(&data).unwrap();
        let decompressed: TestData = decompress_and_deserialize(&compressed).unwrap();

        assert_eq!(decompressed, data);
    }
}
