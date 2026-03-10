use aws_lambda_events::s3::S3Event;
use aws_sdk_s3::Client as S3Client;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{debug, error};

use crate::aws_attributes::AwsAttributes;
use crate::flowlogs::FlowLogManager;

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
pub struct Parser<'a> {
    aws_attributes: AwsAttributes,
    request_id: String,
    s3_client: S3Client,
    config: S3LogsConfig,
    flow_log_manager: &'a mut FlowLogManager,
}

impl<'a> Parser<'a> {
    pub fn new(
        aws_attributes: &AwsAttributes,
        request_id: &String,
        s3_client: &S3Client,
        flow_log_manager: &'a mut FlowLogManager,
    ) -> Self {
        let config = Self::load_config();
        Self {
            aws_attributes: aws_attributes.clone(),
            request_id: request_id.clone(),
            s3_client: s3_client.clone(),
            config,
            flow_log_manager,
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
    /// The flow log manager stored on `self` is queried per-record to detect VPC Flow Log files
    /// delivered to S3 and enrich them with the corresponding flow log tags and format information.
    ///
    /// Returns `Ok(())` once all S3 objects have been processed. Individual object errors are
    /// logged but do not abort processing of remaining objects. A send error (receiver dropped)
    /// causes an early return with `Err`.
    pub async fn parse(
        &mut self,
        s3_event: S3Event,
        result_tx: mpsc::Sender<Vec<ResourceLogs>>,
    ) -> Result<(), ParserError> {
        debug!(
            request_id = %self.request_id,
            records_count = s3_event.records.len(),
            "Starting to parse S3 event"
        );

        let mut total_resource_logs: usize = 0;

        // Process records in parallel with controlled concurrency.
        //
        // NOTE: FlowLogManager requires &mut self for its async cache look-ups, so we must
        // resolve the per-record flow log config *before* spawning the task (which needs an
        // owned, Send value). We look up the config here in the driver loop and pass the
        // resolved Option<FlowLogConfig> into each task.
        let mut tasks: JoinSet<Result<Vec<ResourceLogs>, ParserError>> = JoinSet::new();
        let max_concurrent = self.config.max_parallel_objects;

        for (idx, record) in s3_event.records.into_iter().enumerate() {
            // Resolve the bucket name early so we can query the flow log manager.
            let bucket_name = record.s3.bucket.name.as_deref().unwrap_or("").to_string();

            // Look up VPC flow log config for this bucket (if any) before spawning the task.
            let flow_log_config = if !bucket_name.is_empty() {
                self.flow_log_manager
                    .get_config_by_bucket(&bucket_name)
                    .await
            } else {
                None
            };

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
                flow_log_config,
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

        debug!(
            request_id = %self.request_id,
            resource_logs_count = total_resource_logs,
            "Successfully parsed S3 event"
        );

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParserType {
    Json,
    VpcLog,
    Unknown,
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
