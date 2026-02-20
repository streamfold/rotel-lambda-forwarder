use std::io::Read;

use aws_lambda_events::s3::S3EventRecord;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value},
    logs::v1::{ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use tracing::{debug, info};

use crate::aws_attributes::AwsAttributes;
use crate::parse::cwlogs::{LogPlatform, ParserType};
use crate::parse::record_parser::{RecordLogEntry, RecordParser};

use super::ParserError;
use super::memory_semaphore::MemorySemaphore;

/// Represents an S3 record to be processed
pub struct S3Record {
    record: S3EventRecord,
    idx: usize,
    s3_client: S3Client,
    aws_attributes: AwsAttributes,
    request_id: String,
    memory_semaphore: MemorySemaphore,
    batch_size: usize,
}

impl S3Record {
    /// Create a new S3Record for processing
    pub fn new(
        record: S3EventRecord,
        idx: usize,
        s3_client: S3Client,
        aws_attributes: AwsAttributes,
        request_id: String,
        memory_semaphore: MemorySemaphore,
        batch_size: usize,
    ) -> Self {
        Self {
            record,
            idx,
            s3_client,
            aws_attributes,
            request_id,
            memory_semaphore,
            batch_size,
        }
    }

    /// Process the S3 record and return ResourceLogs
    pub async fn process(self) -> Result<Vec<ResourceLogs>, ParserError> {
        // Log S3 event record properties
        info!(
            request_id = %self.request_id,
            record_index = self.idx,
            event_version = %self.record.event_version.as_deref().unwrap_or("unknown"),
            event_source = %self.record.event_source.as_deref().unwrap_or("unknown"),
            event_name = %self.record.event_name.as_deref().unwrap_or("unknown"),
            aws_region = %self.record.aws_region.as_deref().unwrap_or("unknown"),
            event_time = %self.record.event_time.to_rfc3339(),
            "Processing S3 event record"
        );

        // Extract S3 entity information
        let s3_entity = self.record.s3;
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
            request_id = %self.request_id,
            bucket = %bucket_name,
            key = %object_key,
            size = object_size,
            e_tag = ?s3_entity.object.e_tag,
            version_id = ?s3_entity.object.version_id,
            "S3 object details"
        );

        // Acquire memory permit based on object size
        let permit = self
            .memory_semaphore
            .acquire(object_size as usize, &self.request_id)
            .await;

        // Load object from S3
        let object_data =
            load_s3_object(&self.s3_client, bucket_name, object_key, &self.request_id).await?;

        // Decompress if needed
        let decompressed_data = decompress_if_needed(&object_data, object_key)?;

        debug!(
            request_id = %self.request_id,
            original_size = object_data.len(),
            decompressed_size = decompressed_data.len(),
            "Object loaded and decompressed"
        );

        // Parse log lines into OTLP records
        let resource_logs = parse_log_lines(
            &decompressed_data,
            self.record.event_time,
            bucket_name,
            object_key,
            &self.aws_attributes,
            &self.request_id,
            self.batch_size,
        )?;

        // Release memory permit
        drop(permit);

        Ok(resource_logs)
    }
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

fn parse_log_lines(
    data: &[u8],
    event_time: DateTime<Utc>,
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

    let event_timestamp = event_time.timestamp_millis() as i64;

    // Batch the log records
    let mut resource_logs_list = Vec::new();
    let mut current_batch = Vec::new();

    let lines_count = lines.len();
    for line in lines.into_iter() {
        let log_entry = RecordLogEntry::new(None, event_timestamp, String::from(line));

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
        total_lines = lines_count,
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

        let event_time = Utc::now();
        let result = parse_log_lines(
            log_data.as_bytes(),
            event_time,
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

        let event_time = Utc::now();
        let result = parse_log_lines(
            log_data.as_bytes(),
            event_time,
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

        let event_time = Utc::now();
        let result = parse_log_lines(
            log_data.as_bytes(),
            event_time,
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
