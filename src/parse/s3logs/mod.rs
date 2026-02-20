use aws_lambda_events::s3::S3Event;
use aws_sdk_s3::Client as S3Client;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use tracing::{error, info};

use crate::aws_attributes::AwsAttributes;

mod memory_semaphore;
mod s3record;

use memory_semaphore::MemorySemaphore;
use s3record::S3Record;

/// Configuration for S3 log processing
#[derive(Debug, Clone)]
pub struct S3LogsConfig {
    /// Maximum number of S3 objects to process in parallel
    pub max_parallel_objects: usize,
    /// Maximum total size of objects to load in memory at once (in bytes)
    pub max_memory_bytes: usize,
    /// Maximum number of log records to batch before sending
    pub batch_size: usize,
}

impl Default for S3LogsConfig {
    fn default() -> Self {
        Self {
            max_parallel_objects: 5,
            max_memory_bytes: 100 * 1024 * 1024, // 100 MB
            batch_size: 1000,
        }
    }
}

/// Parser for S3 event notifications that converts log files into OpenTelemetry ResourceLogs
pub struct Parser {
    aws_attributes: AwsAttributes,
    request_id: String,
    s3_client: S3Client,
    config: S3LogsConfig,
}

impl Parser {
    pub fn new(aws_attributes: &AwsAttributes, request_id: &String, s3_client: &S3Client) -> Self {
        let config = Self::load_config();
        Self {
            aws_attributes: aws_attributes.clone(),
            request_id: request_id.clone(),
            s3_client: s3_client.clone(),
            config,
        }
    }

    /// Load configuration from environment variables
    fn load_config() -> S3LogsConfig {
        let max_parallel_objects = std::env::var("FORWARDER_S3_MAX_PARALLEL_OBJECTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);

        let max_memory_bytes = std::env::var("FORWARDER_S3_MAX_MEMORY_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(100 * 1024 * 1024);

        let batch_size = std::env::var("FORWARDER_S3_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        S3LogsConfig {
            max_parallel_objects,
            max_memory_bytes,
            batch_size,
        }
    }

    /// Parse an S3 event and return ResourceLogs
    pub async fn parse(&self, s3_event: S3Event) -> Result<Vec<ResourceLogs>, ParserError> {
        info!(
            request_id = %self.request_id,
            records_count = s3_event.records.len(),
            "Starting to parse S3 event"
        );

        let mut all_resource_logs = Vec::new();

        // Create semaphore for controlling parallel access
        let memory_semaphore = MemorySemaphore::new(self.config.max_memory_bytes);

        // Process records in parallel with controlled concurrency
        let mut tasks = Vec::new();
        let max_concurrent = self.config.max_parallel_objects;

        for (idx, record) in s3_event.records.into_iter().enumerate() {
            let s3_client = self.s3_client.clone();
            let aws_attributes = self.aws_attributes.clone();
            let request_id = self.request_id.clone();
            let memory_sem = memory_semaphore.clone();
            let batch_size = self.config.batch_size;

            // Wait if we've hit the concurrency limit
            if tasks.len() >= max_concurrent {
                if let Some(result) = tasks.pop() {
                    match result.await {
                        Ok(Ok(logs)) => all_resource_logs.extend(logs),
                        Ok(Err(e)) => {
                            error!(error = %e, "Failed to process S3 object");
                        }
                        Err(e) => {
                            error!(error = %e, "Task panicked while processing S3 object");
                        }
                    }
                }
            }

            let s3_record = S3Record::new(
                record,
                idx,
                s3_client,
                aws_attributes,
                request_id,
                memory_sem,
                batch_size,
            );

            let task = tokio::spawn(async move { s3_record.process().await });

            tasks.push(task);
        }

        // Wait for remaining tasks
        for task in tasks {
            match task.await {
                Ok(Ok(logs)) => all_resource_logs.extend(logs),
                Ok(Err(e)) => {
                    error!(error = %e, "Failed to process S3 object");
                }
                Err(e) => {
                    error!(error = %e, "Task panicked while processing S3 object");
                }
            }
        }

        info!(
            request_id = %self.request_id,
            resource_logs_count = all_resource_logs.len(),
            "Successfully parsed S3 event"
        );

        Ok(all_resource_logs)
    }
}

/// Errors that can occur during S3 log parsing
#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    #[error("S3 error: {0}")]
    S3Error(String),

    #[error("Failed to decompress: {0}")]
    DecompressionError(String),

    #[error("Invalid event: {0}")]
    InvalidEvent(String),

    #[error("Encoding error: {0}")]
    EncodingError(String),

    #[error("Parse error: {0}")]
    ParseError(String),
}
