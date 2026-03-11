use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use tracing::warn;

use aws_lambda_events::s3::S3EventRecord;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::{
    common::v1::{InstrumentationScope, KeyValue},
    logs::v1::{ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use tracing::debug;

use crate::flowlogs::{FlowLogConfig, ParsedFields};
use crate::parse::json::parse_json_to_map;
use crate::parse::platform::LogPlatform;
use crate::parse::record_parser::LogBuilder;
use crate::parse::utils::string_kv;
use crate::parse::vpclog::{parse_vpclog_header, parse_vpclog_to_map};
use crate::{aws_attributes::AwsAttributes, s3logs::ParserType};

use super::{JsonLogRecords, ParserError};

/// Represents an S3 record to be processed
pub struct S3Record {
    record: S3EventRecord,
    idx: usize,
    s3_client: S3Client,
    aws_attributes: AwsAttributes,
    request_id: String,
    batch_size: usize,
    /// VPC flow log configuration for the S3 bucket, if applicable.
    flow_log_config: Option<FlowLogConfig>,
}

impl S3Record {
    /// Create a new S3Record for processing
    pub fn new(
        record: S3EventRecord,
        idx: usize,
        s3_client: S3Client,
        aws_attributes: AwsAttributes,
        request_id: String,
        batch_size: usize,
        flow_log_config: Option<FlowLogConfig>,
    ) -> Self {
        Self {
            record,
            idx,
            s3_client,
            aws_attributes,
            request_id,
            batch_size,
            flow_log_config,
        }
    }

    /// Process the S3 record and return ResourceLogs
    pub async fn process(self) -> Result<Vec<ResourceLogs>, ParserError> {
        // Log S3 event record properties
        debug!(
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
            self.flow_log_config.as_ref(),
        )?;

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

#[allow(clippy::too_many_arguments)]
fn parse_log_lines(
    data: &[u8],
    event_time: DateTime<Utc>,
    bucket: &str,
    key: &str,
    aws_attributes: &AwsAttributes,
    request_id: &str,
    batch_size: usize,
    flow_log_config: Option<&FlowLogConfig>,
) -> Result<Vec<ResourceLogs>, ParserError> {
    let content = std::str::from_utf8(data)
        .map_err(|e| ParserError::EncodingError(format!("Invalid UTF-8: {}", e)))?;

    // Check if this is a JSON blob file (ends with .json.gz or .json.gzip after decompression)
    let key_without_compression = key
        .strip_suffix(".gz")
        .or_else(|| key.strip_suffix(".gzip"))
        .unwrap_or(key);

    if key_without_compression.ends_with(".json") {
        // Try to parse as a JSON blob with Records array
        if let Ok(json_blob) = JsonLogRecords::from_str(content) {
            debug!(
                request_id = %request_id,
                record_count = json_blob.len(),
                "Parsing as JSON blob with Records array"
            );

            return parse_json_blob(
                json_blob,
                event_time,
                bucket,
                key,
                aws_attributes,
                request_id,
                batch_size,
            );
        }
        // If parsing as JSON blob fails, fall through to line-by-line parsing
        debug!(
            request_id = %request_id,
            "Failed to parse as JSON blob, falling back to line-by-line parsing"
        );
    }

    // Fall back to line-by-line parsing
    let lines: Vec<&str> = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();

    debug!(
        request_id = %request_id,
        total_lines = lines.len(),
        "Parsing log lines"
    );

    // If we have a VPC flow log configuration for this bucket, use it directly.
    // Otherwise detect format from the key and content.
    let (log_platform, parser_type) = if flow_log_config.is_some() {
        debug!(
            request_id = %request_id,
            bucket = %bucket,
            "Using VPC flow log parser for S3 object"
        );
        (LogPlatform::VpcFlowLog, ParserType::VpcLog)
    } else {
        let (platform, pt) = detect_log_format(key, &lines);
        debug!(
            request_id = %request_id,
            platform = ?platform,
            parser_type = ?pt,
            "Detected log format"
        );
        (platform, pt)
    };

    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    // Build the extra flow-log tags that will be added to resource attributes.
    let flow_log_tags: HashMap<String, String> =
        flow_log_config.map(|c| c.tags.clone()).unwrap_or_default();

    // For VPC flow logs the first line is the column header, e.g.:
    //   "version account-id interface-id srcaddr dstaddr ..."
    // Parse it into typed ParsedFields and treat the remainder as data lines.
    let (flow_log_parsed_fields, data_lines): (Option<Arc<ParsedFields>>, &[&str]) =
        if parser_type == ParserType::VpcLog {
            if let Some((header, rest)) = lines.split_first() {
                let fields = parse_vpclog_header(header);
                debug!(
                    request_id = %request_id,
                    field_count = fields.len(),
                    "Parsed VPC flow log column header from S3 file"
                );
                (Some(Arc::new(ParsedFields::Success(fields))), rest)
            } else {
                (None, &[])
            }
        } else {
            (None, &lines)
        };

    // Create base resource attributes
    let base_attributes =
        create_base_attributes(bucket, key, aws_attributes, log_platform, &flow_log_tags);

    let builder = LogBuilder::new(log_platform);

    let event_timestamp = event_time.timestamp_millis() as i64;

    // Batch the log records
    let mut resource_logs_list = Vec::new();
    let mut current_batch = Vec::new();

    let lines_count = data_lines.len();
    for line in data_lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let log_record = parse_line(
            &builder,
            now_nanos,
            event_timestamp,
            line,
            parser_type,
            flow_log_parsed_fields.clone(),
        );
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

    debug!(
        request_id = %request_id,
        total_lines = lines_count,
        resource_logs_count = resource_logs_list.len(),
        "Successfully parsed log lines"
    );

    Ok(resource_logs_list)
}

/// Parse a single log line string into a [`LogRecord`].
///
/// Dispatches based on `parser_type`:
/// * `VpcLog`   — parse as space-separated VPC flow log fields; raw line becomes the body.
/// * `Json`     — parse as a JSON object; on failure the raw string becomes the body.
/// * `Unknown`  — auto-detect: attempt JSON for `{`-prefixed lines, otherwise plain text.
fn parse_line(
    builder: &LogBuilder,
    now_nanos: u64,
    timestamp_ms: i64,
    line: &str,
    parser_type: ParserType,
    flow_log_parsed_fields: Option<Arc<ParsedFields>>,
) -> opentelemetry_proto::tonic::logs::v1::LogRecord {
    let mut record_builder = builder.start(now_nanos, timestamp_ms, vec![]);

    match parser_type {
        ParserType::VpcLog => {
            // VPC Flow Logs always preserve the raw line as the body.
            // When parsed fields are available, also emit individual fields as attributes.
            record_builder = record_builder.set_body_text(line.to_string());
            if let Some(parsed_fields) = flow_log_parsed_fields {
                match parse_vpclog_to_map(line, parsed_fields) {
                    Ok(map) => {
                        record_builder.populate_from_map(map);
                    }
                    Err(e) => {
                        warn!(error = ?e, "Failed to parse VPC flow log line as structured map");
                    }
                }
            }
            record_builder.finish()
        }

        ParserType::Json => {
            match parse_json_to_map(line) {
                Ok(map) => {
                    record_builder.populate_from_map(map);
                }
                Err(err) => {
                    warn!(error = ?err, "Failed to parse log line as JSON, using raw text as body");
                    return record_builder.set_body_text(line.to_string()).finish();
                }
            }
            record_builder.finish()
        }

        ParserType::Unknown => {
            if line.len() > 2 && line.starts_with('{') {
                match parse_json_to_map(line) {
                    Ok(map) => {
                        record_builder.populate_from_map(map);
                    }
                    Err(err) => {
                        warn!(error = ?err, "Failed to parse log line, using raw text as body");
                        return record_builder.set_body_text(line.to_string()).finish();
                    }
                }
                record_builder.finish()
            } else {
                record_builder.set_body_text(line.to_string()).finish()
            }
        }
    }
}

/// Parse a JSON blob containing a Records array
fn parse_json_blob(
    json_blob: JsonLogRecords,
    event_time: DateTime<Utc>,
    bucket: &str,
    key: &str,
    aws_attributes: &AwsAttributes,
    request_id: &str,
    batch_size: usize,
) -> Result<Vec<ResourceLogs>, ParserError> {
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    // Detect log platform from key (e.g., CloudTrail files)
    let log_platform = detect_log_platform_from_key(key);

    // Create base resource attributes (no flow log tags for JSON blob paths)
    let base_attributes =
        create_base_attributes(bucket, key, aws_attributes, log_platform, &HashMap::new());

    debug!(
        request_id = %request_id,
        platform = ?log_platform,
        "Detected log platform for JSON blob"
    );

    let builder = LogBuilder::new(log_platform);

    let event_timestamp = event_time.timestamp_millis() as i64;

    // Batch the log records
    let mut resource_logs_list = Vec::new();
    let mut current_batch = Vec::new();

    let records_count = json_blob.len();
    for record_map in json_blob.records {
        let log_record = builder.parse_from_map(now_nanos, event_timestamp, record_map);
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
        let resource_logs = create_resource_logs(base_attributes, aws_attributes, current_batch);
        resource_logs_list.push(resource_logs);
    }

    debug!(
        request_id = %request_id,
        total_records = records_count,
        resource_logs_count = resource_logs_list.len(),
        "Successfully parsed JSON blob"
    );

    Ok(resource_logs_list)
}

/// Create base resource attributes for S3 logs.
///
/// When `flow_log_tags` is non-empty the tags are emitted as
/// `ec2.flow-logs.tags.<key>` resource attributes, mirroring the CloudWatch path.
fn create_base_attributes(
    bucket: &str,
    key: &str,
    aws_attributes: &AwsAttributes,
    log_platform: LogPlatform,
    flow_log_tags: &HashMap<String, String>,
) -> Vec<KeyValue> {
    let mut attributes = vec![
        string_kv("cloud.provider", "aws"),
        string_kv("cloud.region", aws_attributes.region.clone()),
        string_kv("cloud.account.id", aws_attributes.account_id.clone()),
        string_kv("aws.s3.bucket", bucket),
        string_kv("aws.s3.key", key),
    ];

    // Add cloud.platform attribute based on detected platform
    if log_platform != LogPlatform::Unknown {
        attributes.push(string_kv("cloud.platform", log_platform.as_str()));
    }

    // Add EC2 Flow Log tags as resource attributes (matches CloudWatch behaviour)
    for (tag_key, tag_value) in flow_log_tags {
        attributes.push(string_kv(
            &format!("ec2.flow-logs.tags.{}", tag_key),
            tag_value.clone(),
        ));
    }

    attributes
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
                name: env!("CARGO_PKG_NAME").to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                attributes: vec![string_kv(
                    "aws.lambda.invoked_arn",
                    aws_attributes.invoked_function_arn.clone(),
                )],
                dropped_attributes_count: 0,
            }),
            log_records,
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// Detect log format from key name and sample lines
fn detect_log_format(key: &str, lines: &[&str]) -> (LogPlatform, ParserType) {
    // First, try to detect format from the key name
    // Remove compression suffix first
    let key_without_compression = key
        .strip_suffix(".gz")
        .or_else(|| key.strip_suffix(".gzip"))
        .unwrap_or(key);

    // Check for .json suffix
    if key_without_compression.ends_with(".json") {
        return (LogPlatform::Unknown, ParserType::Json);
    }

    // If we can't detect from key name, examine content
    if lines.is_empty() {
        return (LogPlatform::Unknown, ParserType::Unknown);
    }

    // Sample first few lines
    let sample_size = lines.len().min(5);
    let mut json_count = 0;

    for line in &lines[..sample_size] {
        let trimmed = line.trim();

        // Check for JSON
        if trimmed.starts_with('{') && trimmed.ends_with('}') {
            json_count += 1;
        }
    }

    // Decide based on majority
    let parser_type = if json_count > sample_size / 2 {
        ParserType::Json
    } else {
        ParserType::Unknown
    };

    // Platform is unknown for S3 logs unless we add more sophisticated detection
    (LogPlatform::Unknown, parser_type)
}

/// Detect log platform from the S3 key name
fn detect_log_platform_from_key(key: &str) -> LogPlatform {
    // CloudTrail files typically contain "CloudTrail" in the path or filename
    if key.contains("CloudTrail") {
        return LogPlatform::Cloudtrail;
    }

    // Default to Unknown
    LogPlatform::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_aws_attributes() -> AwsAttributes {
        AwsAttributes {
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123456789012:function:test".to_string(),
        }
    }

    // ------------------------------------------------------------------
    // detect_log_format
    // ------------------------------------------------------------------

    #[test]
    fn test_detect_log_format_json_from_suffix() {
        let lines = vec!["some content"];

        let (_platform, parser_type) = detect_log_format("logs/app.json", &lines);
        assert_eq!(parser_type, ParserType::Json);

        let (_platform, parser_type) = detect_log_format("logs/app.json.gz", &lines);
        assert_eq!(parser_type, ParserType::Json);

        let (_platform, parser_type) = detect_log_format("logs/app.json.gzip", &lines);
        assert_eq!(parser_type, ParserType::Json);
    }

    #[test]
    fn test_detect_log_format_json_from_content() {
        let lines = vec![
            r#"{"level":"info","msg":"test message 1"}"#,
            r#"{"level":"debug","msg":"test message 2"}"#,
            r#"{"level":"error","msg":"test message 3"}"#,
        ];

        let (_platform, parser_type) = detect_log_format("logs/app.log", &lines);
        assert_eq!(parser_type, ParserType::Json);
    }

    #[test]
    fn test_detect_log_format_unknown() {
        let lines = vec![
            "Plain text log line 1",
            "Plain text log line 2",
            "Plain text log line 3",
        ];

        let (_platform, parser_type) = detect_log_format("logs/app.log", &lines);
        assert_eq!(parser_type, ParserType::Unknown);
    }

    // ------------------------------------------------------------------
    // parse_log_lines (non-VPC)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_parse_log_lines_json() {
        let log_data = r#"{"level":"info","msg":"test message 1","service":"test"}
{"level":"error","msg":"test message 2","service":"test"}
{"level":"debug","msg":"test message 3","service":"test"}"#;

        let aws_attributes = make_aws_attributes();
        let event_time = Utc::now();
        let result = parse_log_lines(
            log_data.as_bytes(),
            event_time,
            "test-bucket",
            "test-key.log",
            &aws_attributes,
            "test-request-id",
            1000,
            None,
        );

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        assert_eq!(resource_logs.len(), 1);
        assert_eq!(resource_logs[0].scope_logs[0].log_records.len(), 3);
    }

    #[tokio::test]
    async fn test_parse_log_lines_batching() {
        let mut log_lines = Vec::new();
        for i in 0..2500 {
            log_lines.push(format!(
                r#"{{"level":"info","msg":"message {}","index":{}}}"#,
                i, i
            ));
        }
        let log_data = log_lines.join("\n");

        let aws_attributes = make_aws_attributes();
        let event_time = Utc::now();
        let result = parse_log_lines(
            log_data.as_bytes(),
            event_time,
            "test-bucket",
            "test-key.log",
            &aws_attributes,
            "test-request-id",
            1000,
            None,
        );

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        assert_eq!(resource_logs.len(), 3);
        assert_eq!(resource_logs[0].scope_logs[0].log_records.len(), 1000);
        assert_eq!(resource_logs[1].scope_logs[0].log_records.len(), 1000);
        assert_eq!(resource_logs[2].scope_logs[0].log_records.len(), 500);
    }

    #[tokio::test]
    async fn test_parse_json_blob_with_records() {
        let json_data = r#"{
            "Records": [
                {"eventName": "CreateBucket", "eventTime": "2024-01-01T12:00:00Z", "level": "info"},
                {"eventName": "DeleteBucket", "eventTime": "2024-01-01T13:00:00Z", "level": "warn"}
            ]
        }"#;

        let aws_attributes = make_aws_attributes();
        let event_time = Utc::now();
        let result = parse_log_lines(
            json_data.as_bytes(),
            event_time,
            "test-bucket",
            "cloudtrail/test.json.gz",
            &aws_attributes,
            "test-request-id",
            1000,
            None,
        );

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        assert_eq!(resource_logs.len(), 1);
        assert_eq!(resource_logs[0].scope_logs[0].log_records.len(), 2);

        let first_log = &resource_logs[0].scope_logs[0].log_records[0];
        let has_event_name = first_log.attributes.iter().any(|kv| kv.key == "eventName");
        assert!(has_event_name);
    }

    // ------------------------------------------------------------------
    // detect_log_platform_from_key
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_detect_log_platform_from_key() {
        assert_eq!(
            detect_log_platform_from_key(
                "AWSLogs/123456789012/CloudTrail/us-east-1/2024/01/01/file.json.gz"
            ),
            LogPlatform::Cloudtrail
        );

        assert_eq!(
            detect_log_platform_from_key("CloudTrail/logs/test.json"),
            LogPlatform::Cloudtrail
        );

        assert_eq!(
            detect_log_platform_from_key("other/logs/test.json"),
            LogPlatform::Unknown
        );
    }

    // ------------------------------------------------------------------
    // parse_line (non-VPC)
    // ------------------------------------------------------------------

    #[test]
    fn test_parse_line_json() {
        let builder = LogBuilder::new(LogPlatform::Unknown);
        let lr = parse_line(
            &builder,
            123_456_789,
            1_000,
            r#"{"level":"info","msg":"hello"}"#,
            ParserType::Json,
            None,
        );
        assert!(lr.body.is_some());
    }

    #[test]
    fn test_parse_line_unknown_auto_detects_json() {
        let builder = LogBuilder::new(LogPlatform::Unknown);
        let lr = parse_line(
            &builder,
            123_456_789,
            1_000,
            r#"{"level":"info","msg":"hello"}"#,
            ParserType::Unknown,
            None,
        );
        assert!(lr.body.is_some());
    }

    #[test]
    fn test_parse_line_plain_text_fallback() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let builder = LogBuilder::new(LogPlatform::Unknown);
        let lr = parse_line(
            &builder,
            123_456_789,
            1_000,
            "plain text log line",
            ParserType::Unknown,
            None,
        );
        if let Some(body) = &lr.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "plain text log line");
            } else {
                panic!("Expected StringValue body");
            }
        } else {
            panic!("Expected body");
        }
    }

    #[test]
    fn test_parse_line_invalid_json_falls_back_to_plain_text() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;

        let builder = LogBuilder::new(LogPlatform::Unknown);
        let raw = r#"{"broken json"#.to_string();
        let lr = parse_line(&builder, 123_456_789, 1_000, &raw, ParserType::Json, None);
        if let Some(body) = &lr.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, &raw);
            } else {
                panic!("Expected StringValue body");
            }
        } else {
            panic!("Expected body");
        }
    }

    // ------------------------------------------------------------------
    // VPC flow log via S3
    // ------------------------------------------------------------------

    #[test]
    fn test_parse_line_vpc_log_sets_body_and_attributes() {
        use crate::flowlogs::{ParsedField, ParsedFieldType, ParsedFields};
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use std::sync::Arc;

        let fields = vec![
            ParsedField::new("version".to_string(), ParsedFieldType::Int32),
            ParsedField::new("account-id".to_string(), ParsedFieldType::String),
            ParsedField::new("interface-id".to_string(), ParsedFieldType::String),
            ParsedField::new("srcaddr".to_string(), ParsedFieldType::String),
            ParsedField::new("dstaddr".to_string(), ParsedFieldType::String),
            ParsedField::new("srcport".to_string(), ParsedFieldType::Int32),
            ParsedField::new("dstport".to_string(), ParsedFieldType::Int32),
            ParsedField::new("protocol".to_string(), ParsedFieldType::Int32),
            ParsedField::new("packets".to_string(), ParsedFieldType::Int64),
            ParsedField::new("bytes".to_string(), ParsedFieldType::Int64),
            ParsedField::new("start".to_string(), ParsedFieldType::Int64),
            ParsedField::new("end".to_string(), ParsedFieldType::Int64),
            ParsedField::new("action".to_string(), ParsedFieldType::String),
            ParsedField::new("log-status".to_string(), ParsedFieldType::String),
        ];
        let parsed_fields = Arc::new(ParsedFields::Success(fields));

        let line =
            "2 123456789012 eni-abc123 10.0.0.1 10.0.0.2 443 52000 6 10 840 1620000000 1620000060 ACCEPT OK"
                .to_string();

        let builder = LogBuilder::new(LogPlatform::VpcFlowLog);
        let lr = parse_line(
            &builder,
            123_456_789,
            1_000,
            &line,
            ParserType::VpcLog,
            Some(parsed_fields),
        );

        // Body should be the raw line
        if let Some(body) = &lr.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, &line);
            } else {
                panic!("Expected StringValue body");
            }
        } else {
            panic!("Expected body");
        }

        // Structured attributes should be present
        assert!(lr.attributes.iter().any(|kv| kv.key == "srcaddr"));
        assert!(lr.attributes.iter().any(|kv| kv.key == "dstaddr"));
        assert!(lr.attributes.iter().any(|kv| kv.key == "action"));
    }

    #[test]
    fn test_parse_log_lines_vpc_flow_logs_with_tags() {
        use crate::flowlogs::FlowLogConfig;

        let log_format = "${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}";

        let mut tags = HashMap::new();
        tags.insert("Environment".to_string(), "production".to_string());
        tags.insert("Team".to_string(), "networking".to_string());

        let flow_log_config = FlowLogConfig {
            log_format: log_format.to_string(),
            flow_log_id: "fl-s3-test123".to_string(),
            tags,
            // No pre-parsed fields — the header line in the file is the source of truth.
            parsed_fields: None,
        };

        let vpc_data = "version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status\n\
                        2 123456789012 eni-abc123 10.0.0.1 10.0.0.2 443 52000 6 10 840 1620000000 1620000060 ACCEPT OK\n\
                        2 123456789012 eni-abc123 10.0.0.2 10.0.0.1 52000 443 6 8 620 1620000060 1620000120 ACCEPT OK\n";

        let aws_attributes = make_aws_attributes();
        let event_time = Utc::now();

        let result = parse_log_lines(
            vpc_data.as_bytes(),
            event_time,
            "my-flow-logs-bucket",
            "AWSLogs/123456789012/vpcflowlogs/us-east-1/2024/01/01/flow.log.gz",
            &aws_attributes,
            "test-request-id",
            1000,
            Some(&flow_log_config),
        );

        assert!(result.is_ok(), "parse_log_lines failed: {:?}", result.err());
        let resource_logs = result.unwrap();
        assert_eq!(resource_logs.len(), 1);

        let rl = &resource_logs[0];

        // cloud.platform should be set to VPC flow log value
        let resource = rl.resource.as_ref().unwrap();
        let platform_attr = resource
            .attributes
            .iter()
            .find(|kv| kv.key == "cloud.platform");
        assert!(platform_attr.is_some(), "cloud.platform attribute missing");
        if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
            &platform_attr.unwrap().value.as_ref().unwrap().value
        {
            assert_eq!(s, "aws_vpc_flow_log");
        }

        // Flow log tags should appear as resource attributes
        let has_env_tag = resource
            .attributes
            .iter()
            .any(|kv| kv.key == "ec2.flow-logs.tags.Environment");
        let has_team_tag = resource
            .attributes
            .iter()
            .any(|kv| kv.key == "ec2.flow-logs.tags.Team");
        assert!(has_env_tag, "ec2.flow-logs.tags.Environment missing");
        assert!(has_team_tag, "ec2.flow-logs.tags.Team missing");

        // Two log records for two data lines
        assert_eq!(rl.scope_logs[0].log_records.len(), 2);

        // Each record should have structured attributes
        let first_record = &rl.scope_logs[0].log_records[0];
        assert!(
            first_record.attributes.iter().any(|kv| kv.key == "srcaddr"),
            "srcaddr attribute missing"
        );
        assert!(
            first_record.attributes.iter().any(|kv| kv.key == "action"),
            "action attribute missing"
        );
    }

    #[test]
    fn test_parse_log_lines_vpc_header_defines_columns() {
        use crate::flowlogs::FlowLogConfig;

        // FlowLogConfig with no pre-parsed fields — the header line must be used instead.
        let flow_log_config = FlowLogConfig {
            log_format: "${version} ${account-id}".to_string(),
            flow_log_id: "fl-hdr-test".to_string(),
            tags: HashMap::new(),
            parsed_fields: None,
        };

        // Header line followed by one data line.
        let data = "version account-id\n2 123456789012\n";
        let aws_attributes = make_aws_attributes();
        let event_time = Utc::now();

        let result = parse_log_lines(
            data.as_bytes(),
            event_time,
            "my-bucket",
            "flow.log",
            &aws_attributes,
            "req-id",
            1000,
            Some(&flow_log_config),
        );

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        // Only the data line should produce a record; the header line is consumed as column info.
        assert_eq!(resource_logs[0].scope_logs[0].log_records.len(), 1);

        // Structured attributes should be populated from the header-derived fields.
        let record = &resource_logs[0].scope_logs[0].log_records[0];
        assert!(
            record.attributes.iter().any(|kv| kv.key == "account-id"),
            "account-id attribute should be present"
        );
    }

    #[test]
    fn test_create_base_attributes_with_flow_log_tags() {
        let aws_attributes = make_aws_attributes();
        let mut tags = HashMap::new();
        tags.insert("Env".to_string(), "prod".to_string());

        let attrs = create_base_attributes(
            "my-bucket",
            "some/key.log",
            &aws_attributes,
            LogPlatform::VpcFlowLog,
            &tags,
        );

        let has_platform = attrs.iter().any(|kv| {
            kv.key == "cloud.platform"
                && matches!(
                    &kv.value.as_ref().unwrap().value,
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s))
                    if s == "aws_vpc_flow_log"
                )
        });
        assert!(has_platform);

        let has_tag = attrs.iter().any(|kv| kv.key == "ec2.flow-logs.tags.Env");
        assert!(has_tag);
    }

    #[test]
    fn test_create_base_attributes_no_platform_for_unknown() {
        let aws_attributes = make_aws_attributes();
        let attrs = create_base_attributes(
            "bucket",
            "key",
            &aws_attributes,
            LogPlatform::Unknown,
            &HashMap::new(),
        );
        let has_platform = attrs.iter().any(|kv| kv.key == "cloud.platform");
        // Unknown platform should not add a cloud.platform attribute
        assert!(!has_platform);
    }

    // ------------------------------------------------------------------
    // CloudTrail real-file test (carried over from original)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_parse_real_cloudtrail_file() {
        let cloudtrail_json = r#"{
  "Records": [
    {
      "eventVersion": "1.08",
      "userIdentity": {
        "type": "IAMUser",
        "principalId": "AIDACKCEVSQ6C2EXAMPLE",
        "arn": "arn:aws:iam::123456789012:user/Alice",
        "accountId": "123456789012",
        "userName": "Alice"
      },
      "eventTime": "2024-01-15T10:30:00Z",
      "eventSource": "s3.amazonaws.com",
      "eventName": "GetObject",
      "awsRegion": "us-east-1",
      "sourceIPAddress": "198.51.100.1",
      "userAgent": "aws-sdk-go/1.44.0",
      "requestParameters": {
        "bucketName": "my-bucket",
        "key": "my-object"
      },
      "responseElements": null,
      "requestID": "EXAMPLE123456789",
      "eventID": "EXAMPLE-1234-5678-abcd-123456789012",
      "readOnly": true,
      "resources": [
        {
          "ARN": "arn:aws:s3:::my-bucket/my-object",
          "accountId": "123456789012",
          "type": "AWS::S3::Object"
        }
      ],
      "eventType": "AwsApiCall",
      "managementEvent": false,
      "recipientAccountId": "123456789012"
    },
    {
      "eventVersion": "1.08",
      "userIdentity": {
        "type": "Root",
        "principalId": "123456789012",
        "arn": "arn:aws:iam::123456789012:root",
        "accountId": "123456789012"
      },
      "eventTime": "2024-01-15T11:00:00Z",
      "eventSource": "signin.amazonaws.com",
      "eventName": "ConsoleLogin",
      "awsRegion": "us-east-1",
      "sourceIPAddress": "203.0.113.5",
      "userAgent": "Mozilla/5.0",
      "requestParameters": null,
      "responseElements": {
        "ConsoleLogin": "Success"
      },
      "requestID": "EXAMPLE987654321",
      "eventID": "EXAMPLE-abcd-ef01-2345-678901234567",
      "readOnly": false,
      "eventType": "AwsApiCall",
      "managementEvent": true,
      "recipientAccountId": "123456789012"
    }
  ]
}"#;

        let aws_attributes = make_aws_attributes();
        let event_time = Utc::now();

        let result = parse_log_lines(
            cloudtrail_json.as_bytes(),
            event_time,
            "my-cloudtrail-bucket",
            "AWSLogs/123456789012/CloudTrail/us-east-1/2024/01/15/123456789012_CloudTrail_us-east-1_20240115T1030Z_abc123.json.gz",
            &aws_attributes,
            "test-cloudtrail-request",
            1000,
            None,
        );

        assert!(
            result.is_ok(),
            "CloudTrail parse failed: {:?}",
            result.err()
        );
        let resource_logs = result.unwrap();
        assert_eq!(resource_logs.len(), 1);

        let logs = &resource_logs[0];
        assert_eq!(logs.scope_logs[0].log_records.len(), 2);

        let resource = logs.resource.as_ref().unwrap();

        // Verify S3 attributes are present
        assert!(
            resource
                .attributes
                .iter()
                .any(|kv| kv.key == "aws.s3.bucket")
        );
        assert!(resource.attributes.iter().any(|kv| kv.key == "aws.s3.key"));

        // Verify cloud.platform is set for CloudTrail
        let platform_attr = resource
            .attributes
            .iter()
            .find(|kv| kv.key == "cloud.platform");
        assert!(platform_attr.is_some());
        if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
            &platform_attr.unwrap().value.as_ref().unwrap().value
        {
            assert_eq!(s, "aws_cloudtrail");
        }

        // Verify first record has expected CloudTrail fields
        let first_record = &logs.scope_logs[0].log_records[0];
        assert!(
            first_record
                .attributes
                .iter()
                .any(|kv| kv.key == "eventName")
        );
        assert!(
            first_record
                .attributes
                .iter()
                .any(|kv| kv.key == "eventSource")
        );
    }
}
