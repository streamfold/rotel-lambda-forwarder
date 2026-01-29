use std::io::Read;
use std::sync::Arc;

use aws_lambda_events::s3::{S3Event, S3EventRecord};
use aws_sdk_s3::Client as S3Client;
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value},
    logs::v1::{ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::aws_attributes::AwsAttributes;
use crate::parse::cwlogs::{LogPlatform, ParserType};
use crate::parse::record_parser::RecordParser;

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
        let memory_semaphore = Arc::new(Semaphore::new(self.config.max_memory_bytes));

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

            let task = tokio::spawn(async move {
                process_s3_record(
                    record,
                    idx,
                    s3_client,
                    aws_attributes,
                    request_id,
                    memory_sem,
                    batch_size,
                )
                .await
            });

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

/// Process a single S3 event record
async fn process_s3_record(
    record: S3EventRecord,
    idx: usize,
    s3_client: S3Client,
    aws_attributes: AwsAttributes,
    request_id: String,
    memory_semaphore: Arc<Semaphore>,
    batch_size: usize,
) -> Result<Vec<ResourceLogs>, ParserError> {
    // Log S3 event record properties
    info!(
        request_id = %request_id,
        record_index = idx,
        event_version = %record.event_version.as_deref().unwrap_or("unknown"),
        event_source = %record.event_source.as_deref().unwrap_or("unknown"),
        event_name = %record.event_name.as_deref().unwrap_or("unknown"),
        aws_region = %record.aws_region.as_deref().unwrap_or("unknown"),
        event_time = %record.event_time.to_rfc3339(),
        "Processing S3 event record"
    );

    // Extract S3 entity information
    let s3_entity = record.s3;
    let bucket_name = s3_entity
        .bucket
        .name
        .as_deref()
        .ok_or_else(|| ParserError::InvalidEvent("Missing bucket name".to_string()))?;
    let object_key = s3_entity
        .object
        .key
        .as_deref()
        .ok_or_else(|| ParserError::InvalidEvent("Missing object key".to_string()))?;
    let object_size = s3_entity.object.size.unwrap_or(0);

    debug!(
        request_id = %request_id,
        bucket = %bucket_name,
        key = %object_key,
        size = object_size,
        e_tag = ?s3_entity.object.e_tag,
        version_id = ?s3_entity.object.version_id,
        "S3 object details"
    );

    // Acquire memory permit based on object size
    let permit = if object_size > 0 {
        match memory_semaphore
            .clone()
            .acquire_many_owned(object_size as u32)
            .await
        {
            Ok(p) => Some(p),
            Err(e) => {
                warn!(
                    request_id = %request_id,
                    error = %e,
                    object_size = object_size,
                    "Failed to acquire memory permit, processing anyway"
                );
                None
            }
        }
    } else {
        None
    };

    // Load object from S3
    let object_data = load_s3_object(&s3_client, bucket_name, object_key, &request_id).await?;

    // Decompress if needed
    let decompressed_data = decompress_if_needed(&object_data, object_key)?;

    debug!(
        request_id = %request_id,
        original_size = object_data.len(),
        decompressed_size = decompressed_data.len(),
        "Object loaded and decompressed"
    );

    // Parse log lines into OTLP records
    let resource_logs = parse_log_lines(
        &decompressed_data,
        bucket_name,
        object_key,
        &aws_attributes,
        &request_id,
        batch_size,
    )?;

    // Release memory permit
    drop(permit);

    Ok(resource_logs)
}

/// Load an object from S3
async fn load_s3_object(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    request_id: &str,
) -> Result<Vec<u8>, ParserError> {
    debug!(
        request_id = %request_id,
        bucket = %bucket,
        key = %key,
        "Loading object from S3"
    );

    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| ParserError::S3Error(format!("Failed to get S3 object: {}", e)))?;

    let body_bytes = response
        .body
        .collect()
        .await
        .map_err(|e| ParserError::S3Error(format!("Failed to read S3 object body: {}", e)))?
        .into_bytes()
        .to_vec();

    debug!(
        request_id = %request_id,
        size = body_bytes.len(),
        "Successfully loaded object from S3"
    );

    Ok(body_bytes)
}

/// Decompress data if the file extension indicates compression
fn decompress_if_needed(data: &[u8], key: &str) -> Result<Vec<u8>, ParserError> {
    if key.ends_with(".gz") || key.ends_with(".gzip") {
        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            ParserError::DecompressionError(format!("Failed to decompress gzip: {}", e))
        })?;
        Ok(decompressed)
    } else {
        Ok(data.to_vec())
    }
}

/// Parse log lines from decompressed data into ResourceLogs
fn parse_log_lines(
    data: &[u8],
    bucket: &str,
    key: &str,
    aws_attributes: &AwsAttributes,
    request_id: &str,
    batch_size: usize,
) -> Result<Vec<ResourceLogs>, ParserError> {
    let content = std::str::from_utf8(data)
        .map_err(|e| ParserError::EncodingError(format!("Invalid UTF-8: {}", e)))?;

    let lines: Vec<&str> = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();

    debug!(
        request_id = %request_id,
        total_lines = lines.len(),
        "Parsing log lines"
    );

    // Detect log format from first few lines
    let (log_platform, parser_type) = detect_log_format(&lines);

    debug!(
        request_id = %request_id,
        platform = ?log_platform,
        parser_type = ?parser_type,
        "Detected log format"
    );

    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    // Create base resource attributes
    let base_attributes = create_base_attributes(bucket, key, aws_attributes);

    // Parse lines into log records using RecordParser
    let rec_parser = RecordParser::new(log_platform, parser_type, None);

    // Batch the log records
    let mut resource_logs_list = Vec::new();
    let mut current_batch = Vec::new();

    for (line_num, line) in lines.iter().enumerate() {
        // Create a mock LogEntry for parsing
        let mut log_entry = aws_lambda_events::cloudwatch_logs::LogEntry::default();
        log_entry.id = format!("{}:{}", key, line_num);
        log_entry.timestamp = now_nanos as i64 / 1_000_000; // Convert to milliseconds
        log_entry.message = line.to_string();

        let log_record = rec_parser.parse(now_nanos, log_entry);
        current_batch.push(log_record);

        // Create a ResourceLogs when we hit the batch size
        if current_batch.len() >= batch_size {
            let resource_logs = create_resource_logs(
                base_attributes.clone(),
                aws_attributes,
                std::mem::take(&mut current_batch),
            );
            resource_logs_list.push(resource_logs);
        }
    }

    // Add remaining records
    if !current_batch.is_empty() {
        let resource_logs =
            create_resource_logs(base_attributes.clone(), aws_attributes, current_batch);
        resource_logs_list.push(resource_logs);
    }

    info!(
        request_id = %request_id,
        total_lines = lines.len(),
        resource_logs_count = resource_logs_list.len(),
        "Successfully parsed log lines"
    );

    Ok(resource_logs_list)
}

/// Create base resource attributes for S3 logs
fn create_base_attributes(
    bucket: &str,
    key: &str,
    aws_attributes: &AwsAttributes,
) -> Vec<KeyValue> {
    vec![
        KeyValue {
            key: "cloud.provider".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("aws".to_string())),
            }),
        },
        KeyValue {
            key: "cloud.region".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(aws_attributes.region.clone())),
            }),
        },
        KeyValue {
            key: "cloud.account.id".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(aws_attributes.account_id.clone())),
            }),
        },
        KeyValue {
            key: "s3.bucket.name".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(bucket.to_string())),
            }),
        },
        KeyValue {
            key: "s3.object.key".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(key.to_string())),
            }),
        },
    ]
}

/// Create a ResourceLogs structure from attributes and log records
fn create_resource_logs(
    attributes: Vec<KeyValue>,
    aws_attributes: &AwsAttributes,
    log_records: Vec<opentelemetry_proto::tonic::logs::v1::LogRecord>,
) -> ResourceLogs {
    ResourceLogs {
        resource: Some(Resource {
            attributes,
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }),
        scope_logs: vec![ScopeLogs {
            scope: Some(InstrumentationScope {
                name: "rotel-lambda-forwarder".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                attributes: vec![KeyValue {
                    key: "aws.lambda.invoked_arn".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(
                            aws_attributes.invoked_function_arn.clone(),
                        )),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            log_records,
            schema_url: "".to_string(),
        }],
        schema_url: String::new(),
    }
}

/// Detect log format from sample lines
fn detect_log_format(lines: &[&str]) -> (LogPlatform, ParserType) {
    if lines.is_empty() {
        return (LogPlatform::Unknown, ParserType::Unknown);
    }

    // Sample first few lines
    let sample_size = lines.len().min(5);
    let mut json_count = 0;
    let mut keyvalue_count = 0;

    for line in &lines[..sample_size] {
        let trimmed = line.trim();

        // Check for JSON
        if trimmed.starts_with('{') && trimmed.ends_with('}') {
            json_count += 1;
        }

        // Check for key-value patterns (contains = signs)
        if trimmed.contains('=') && !trimmed.starts_with('{') {
            keyvalue_count += 1;
        }
    }

    // Decide based on majority
    let parser_type = if json_count > sample_size / 2 {
        ParserType::Json
    } else if keyvalue_count > sample_size / 2 {
        ParserType::KeyValue
    } else {
        ParserType::Unknown
    };

    // Platform is unknown for S3 logs unless we add more sophisticated detection
    (LogPlatform::Unknown, parser_type)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_log_format_json() {
        let lines = vec![
            r#"{"level":"info","msg":"test message 1"}"#,
            r#"{"level":"debug","msg":"test message 2"}"#,
            r#"{"level":"error","msg":"test message 3"}"#,
        ];

        let (_platform, parser_type) = detect_log_format(&lines);
        assert_eq!(parser_type, ParserType::Json);
    }

    #[test]
    fn test_detect_log_format_keyvalue() {
        let lines = vec![
            r#"time="2025-01-01T00:00:00Z" level=info msg="test message 1""#,
            r#"time="2025-01-01T00:00:01Z" level=debug msg="test message 2""#,
            r#"time="2025-01-01T00:00:02Z" level=error msg="test message 3""#,
        ];

        let (_platform, parser_type) = detect_log_format(&lines);
        assert_eq!(parser_type, ParserType::KeyValue);
    }

    #[test]
    fn test_detect_log_format_unknown() {
        let lines = vec![
            "Plain text log line 1",
            "Plain text log line 2",
            "Plain text log line 3",
        ];

        let (_platform, parser_type) = detect_log_format(&lines);
        assert_eq!(parser_type, ParserType::Unknown);
    }

    #[test]
    fn test_decompress_gzip() {
        // Create a simple gzip-compressed string
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let original = b"test log line\n";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let result = decompress_if_needed(&compressed, "test.log.gz").unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn test_decompress_no_compression() {
        let data = b"test log line\n";
        let result = decompress_if_needed(data, "test.log").unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_parse_log_lines_json() {
        let log_data = r#"{"level":"info","msg":"test message 1","service":"test"}
{"level":"error","msg":"test message 2","service":"test"}
{"level":"debug","msg":"test message 3","service":"test"}"#;

        let aws_attributes = AwsAttributes {
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123456789012:function:test".to_string(),
        };

        let result = parse_log_lines(
            log_data.as_bytes(),
            "test-bucket",
            "test-key.log",
            &aws_attributes,
            "test-request-id",
            1000,
        );

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        assert_eq!(resource_logs.len(), 1);

        let logs = &resource_logs[0];
        assert_eq!(logs.scope_logs.len(), 1);
        assert_eq!(logs.scope_logs[0].log_records.len(), 3);
    }

    #[tokio::test]
    async fn test_parse_log_lines_keyvalue() {
        let log_data = r#"time="2025-01-01T00:00:00Z" level=info msg="test message 1"
time="2025-01-01T00:00:01Z" level=error msg="test message 2"
time="2025-01-01T00:00:02Z" level=debug msg="test message 3""#;

        let aws_attributes = AwsAttributes {
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123456789012:function:test".to_string(),
        };

        let result = parse_log_lines(
            log_data.as_bytes(),
            "test-bucket",
            "test-key.log",
            &aws_attributes,
            "test-request-id",
            1000,
        );

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        assert_eq!(resource_logs.len(), 1);

        let logs = &resource_logs[0];
        assert_eq!(logs.scope_logs.len(), 1);
        assert_eq!(logs.scope_logs[0].log_records.len(), 3);
    }

    #[tokio::test]
    async fn test_parse_log_lines_batching() {
        // Create more lines than batch size to test batching
        let mut log_lines = Vec::new();
        for i in 0..2500 {
            log_lines.push(format!(
                r#"{{"level":"info","msg":"message {}","index":{}}}"#,
                i, i
            ));
        }
        let log_data = log_lines.join("\n");

        let aws_attributes = AwsAttributes {
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123456789012:function:test".to_string(),
        };

        let result = parse_log_lines(
            log_data.as_bytes(),
            "test-bucket",
            "test-key.log",
            &aws_attributes,
            "test-request-id",
            1000, // batch size
        );

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        // Should create 3 ResourceLogs: 1000 + 1000 + 500
        assert_eq!(resource_logs.len(), 3);

        assert_eq!(resource_logs[0].scope_logs[0].log_records.len(), 1000);
        assert_eq!(resource_logs[1].scope_logs[0].log_records.len(), 1000);
        assert_eq!(resource_logs[2].scope_logs[0].log_records.len(), 500);
    }
}
