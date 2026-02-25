use aws_lambda_events::s3::S3Event;
use aws_sdk_s3::Client as S3Client;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::aws_attributes::AwsAttributes;

mod json_blob;
mod s3record;

pub use json_blob::JsonLogRecords;
use s3record::S3Record;

/// Configuration for S3 log processing
#[derive(Debug, Clone)]
pub struct S3LogsConfig {
    /// Maximum number of S3 objects to process in parallel
    pub max_parallel_objects: usize,
    /// Maximum number of log records to batch before sending
    pub batch_size: usize,
}

impl Default for S3LogsConfig {
    fn default() -> Self {
        Self {
            max_parallel_objects: 5,
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

        let batch_size = std::env::var("FORWARDER_S3_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000);

        S3LogsConfig {
            max_parallel_objects,
            batch_size,
        }
    }

    /// Parse an S3 event, streaming batches of ResourceLogs to `result_tx` as each S3 object
    /// completes. This enables the caller to pipeline downstream processing (batching, exporting)
    /// concurrently with ongoing S3 reads.
    ///
    /// Returns `Ok(())` once all S3 objects have been processed. Individual object errors are
    /// logged but do not abort processing of remaining objects. A send error (receiver dropped)
    /// causes an early return with `Err`.
    pub async fn parse(
        &self,
        s3_event: S3Event,
        result_tx: mpsc::Sender<Vec<ResourceLogs>>,
    ) -> Result<(), ParserError> {
        info!(
            request_id = %self.request_id,
            records_count = s3_event.records.len(),
            "Starting to parse S3 event"
        );

        let mut total_resource_logs: usize = 0;

        // Process records in parallel with controlled concurrency
        let mut tasks: JoinSet<Result<Vec<ResourceLogs>, ParserError>> = JoinSet::new();
        let max_concurrent = self.config.max_parallel_objects;

        for (idx, record) in s3_event.records.into_iter().enumerate() {
            let s3_client = self.s3_client.clone();
            let aws_attributes = self.aws_attributes.clone();
            let request_id = self.request_id.clone();
            let batch_size = self.config.batch_size;

            // Wait for the first task to finish if we've hit the concurrency limit, then
            // stream its results immediately rather than accumulating them.
            while tasks.len() >= max_concurrent {
                match tasks.join_next().await {
                    Some(Ok(Ok(logs))) => {
                        total_resource_logs += logs.len();
                        if result_tx.send(logs).await.is_err() {
                            // Receiver was dropped; the caller has given up — stop processing.
                            error!(
                                request_id = %self.request_id,
                                "Result receiver dropped; aborting S3 parse"
                            );
                            return Err(ParserError::ParseError(
                                "Result receiver dropped".to_string(),
                            ));
                        }
                    }
                    Some(Ok(Err(e))) => {
                        error!(error = %e, "Failed to process S3 object");
                    }
                    Some(Err(e)) => {
                        error!(error = %e, "Task panicked while processing S3 object");
                    }
                    None => break,
                }
            }

            let s3_record = S3Record::new(
                record,
                idx,
                s3_client,
                aws_attributes,
                request_id,
                batch_size,
            );

            tasks.spawn(async move { s3_record.process().await });
        }

        // Drain remaining tasks, streaming each result as it completes.
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(logs)) => {
                    total_resource_logs += logs.len();
                    if result_tx.send(logs).await.is_err() {
                        error!(
                            request_id = %self.request_id,
                            "Result receiver dropped; aborting S3 parse"
                        );
                        return Err(ParserError::ParseError(
                            "Result receiver dropped".to_string(),
                        ));
                    }
                }
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
            resource_logs_count = total_resource_logs,
            "Successfully parsed S3 event"
        );

        Ok(())
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
