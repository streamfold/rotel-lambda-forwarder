use std::sync::{Arc, OnceLock};

use aws_lambda_events::cloudwatch_logs::LogEntry;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, SeverityNumber},
};
use regex::Regex;
use serde_json::Value as JsonValue;
use tracing::{debug, warn};

use crate::flowlogs::ParsedFields;
use crate::parse::{
    cwlogs::{LogPlatform, ParserError, ParserType},
    field_stripper::FieldStripper,
    json::{json_value_to_any_value, parse_json_to_map},
    keyvalue::parse_keyvalue_to_map,
    vpclog::parse_vpclog_to_map,
};

/// Parser for CloudWatch log entries that converts them into OpenTelemetry LogRecords.
///
/// The `RecordParser` handles format detection, parsing, field extraction, and
/// platform-specific transformations in a unified way for both JSON and key-value logs.
pub(crate) struct RecordParser {
    platform: LogPlatform,
    parser_type: ParserType,
    field_stripper: FieldStripper,
    flow_log_parsed_fields: Option<Arc<ParsedFields>>,
}

#[derive(Debug)]
pub(crate) struct RecordParserError(pub(crate) ParserError, pub(crate) String);

impl RecordParser {
    /// Create a new RecordParser for a specific platform and parser type.
    pub(crate) fn new(
        platform: LogPlatform,
        parser_type: ParserType,
        flow_log_parsed_fields: Option<Arc<ParsedFields>>,
    ) -> Self {
        Self {
            platform,
            parser_type,
            field_stripper: FieldStripper::new(platform),
            flow_log_parsed_fields,
        }
    }

    /// Parse a CloudWatch LogEntry into an OpenTelemetry LogRecord.
    /// If parsing fails, the message is treated as plain text.
    pub(crate) fn parse(&self, now_nanos: u64, log_entry: LogEntry) -> LogRecord {
        let mut lr = LogRecord {
            time_unix_nano: (log_entry.timestamp * 1_000_000) as u64,
            observed_time_unix_nano: now_nanos,
            attributes: vec![KeyValue {
                key: "cloudwatch.id".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(log_entry.id)),
                }),
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let message = log_entry.message;

        // Try to parse the message into a map
        let json_map = match self.parser_type {
            ParserType::Json => parse_json_to_map(message).map(|r| Some(r)),
            ParserType::KeyValue => parse_keyvalue_to_map(message).map(|r| Some(r)),
            ParserType::VpcLog => {
                // Always keep log in Body
                lr.body = Some(AnyValue {
                    value: Some(Value::StringValue(message.clone())),
                });

                match self.flow_log_parsed_fields.as_ref() {
                    Some(parsed_fields) => {
                        parse_vpclog_to_map(message, parsed_fields.clone()).map(|r| Some(r))
                    }
                    None => Ok(None),
                }
            }
            ParserType::Unknown => {
                // Auto-detect: try JSON first, otherwise try keyvalue, otherwise plain text
                if message.len() > 2 && message.starts_with("{") {
                    parse_json_to_map(message).map(|r| Some(r))
                } else {
                    // Otherwise, just use the string as the body
                    lr.body = Some(AnyValue {
                        value: Some(Value::StringValue(message)),
                    });
                    Ok(None)
                }
            }
        };

        match json_map {
            Ok(None) => {}
            Ok(Some(mut map)) => {
                self.field_stripper.strip_fields(&mut map);

                self.populate_log_record_from_map(map, &mut lr);
            }
            Err(RecordParserError(err, msg)) => {
                warn!(error = ?err, "Failed to parse log entry, using raw text as body");

                // If parsing failed, treat as plain text
                lr.body = Some(AnyValue {
                    value: Some(Value::StringValue(msg)),
                });
            }
        }

        lr
    }

    /// Populate a LogRecord from a parsed map (applies to both JSON and keyvalue formats)
    fn populate_log_record_from_map(
        &self,
        mut json_map: serde_json::Map<String, JsonValue>,
        lr: &mut LogRecord,
    ) {
        // Handle "level" field for severity
        if let Some(level_value) = json_map.remove("level") {
            if let JsonValue::String(level_str) = &level_value {
                if let Some((severity_number, severity_text)) = map_log_level(level_str) {
                    lr.severity_number = severity_number as i32;
                    lr.severity_text = severity_text;
                } else {
                    // put back if not matched
                    json_map.insert("level".to_string(), level_value);
                }
            } else {
                // put back if not a string
                json_map.insert("level".to_string(), level_value);
            }
        }

        // Check for body fields in order: 'msg', 'log', 'message'
        let body_keys = ["msg", "log", "message"];
        for key in body_keys {
            if let Some(body_value) = json_map.remove(key) {
                lr.body = Some(json_value_to_any_value(body_value));
                break;
            }
        }

        // Check for timestamp fields: 'ts', 'time', or 'timestamp'
        let timestamp_keys = ["ts", "time", "timestamp"];
        for key in timestamp_keys {
            if let Some(ts_value) = json_map.remove(key) {
                if let Some(parsed_nanos) = parse_timestamp(&ts_value) {
                    lr.time_unix_nano = parsed_nanos;
                }
                break;
            }
        }

        // Handle trace_id and span_id if they exist
        if let Some(JsonValue::String(trace_id_str)) = json_map.remove("trace_id") {
            if let Ok(trace_bytes) = decode_hex(&trace_id_str) {
                if trace_bytes.len() == 16 {
                    lr.trace_id = trace_bytes;
                } else {
                    debug!(
                        "trace_id has wrong length: expected 16 bytes, got {}",
                        trace_bytes.len()
                    );
                }
            } else {
                debug!("Failed to decode trace_id as hex");
            }
        }

        if let Some(JsonValue::String(span_id_str)) = json_map.remove("span_id") {
            if let Ok(span_bytes) = decode_hex(&span_id_str) {
                if span_bytes.len() == 8 {
                    lr.span_id = span_bytes;
                } else {
                    debug!(
                        "span_id has wrong length: expected 8 bytes, got {}",
                        span_bytes.len()
                    );
                }
            } else {
                debug!("Failed to decode span_id as hex");
            }
        }

        // Platform-specific body handling
        // CloudTrail: Use eventType::eventName as body if no body field was found
        if self.platform == LogPlatform::Cloudtrail && lr.body.is_none() {
            let event_type = json_map.get("eventType");
            let event_name = json_map.get("eventName");
            lr.body = match (event_type, event_name) {
                (Some(JsonValue::String(t)), Some(JsonValue::String(n))) => Some(AnyValue {
                    value: Some(Value::StringValue(format!("{}::{}", t, n))),
                }),
                (Some(JsonValue::String(t)), None) => Some(AnyValue {
                    value: Some(Value::StringValue(t.to_string())),
                }),
                (_, _) => None,
            };
        }

        // Convert remaining fields to attributes
        for (key, value) in json_map {
            lr.attributes.push(KeyValue {
                key,
                value: Some(json_value_to_any_value(value)),
            });
        }
    }
}

/// Hex decode utilities
fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    hex::decode(s).map_err(|e| format!("Hex decode error: {}", e))
}

/// Parse a timestamp value (supports RFC3339 strings and Unix timestamps)
fn parse_timestamp(value: &JsonValue) -> Option<u64> {
    match value {
        JsonValue::Number(n) => {
            // Try as Unix timestamp (seconds or milliseconds)
            if let Some(ts_float) = n.as_f64() {
                // Heuristic: if > 1e12, assume milliseconds, otherwise seconds
                if ts_float > 1e12 {
                    Some((ts_float * 1_000.0) as u64)
                } else {
                    Some((ts_float * 1_000_000_000.0) as u64)
                }
            } else {
                None
            }
        }
        JsonValue::String(s) => {
            // Try as RFC3339
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                Some(dt.timestamp_nanos_opt().unwrap_or(0) as u64)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn get_level_regexes() -> &'static [(&'static str, SeverityNumber, &'static str, Regex)] {
    static REGEXES: OnceLock<Vec<(&'static str, SeverityNumber, &'static str, Regex)>> =
        OnceLock::new();
    REGEXES.get_or_init(|| {
        vec![
            (
                "trace",
                SeverityNumber::Trace,
                "TRACE",
                Regex::new(r"^trace\d*$").unwrap(),
            ),
            (
                "debug",
                SeverityNumber::Debug,
                "DEBUG",
                Regex::new(r"^(debug|dbug)\d*$").unwrap(),
            ),
            (
                "info",
                SeverityNumber::Info,
                "INFO",
                Regex::new(r"^(info|information)\d*$").unwrap(),
            ),
            (
                "warn",
                SeverityNumber::Warn,
                "WARN",
                Regex::new(r"^(warn|warning|notice)?\d*$").unwrap(),
            ),
            (
                "error",
                SeverityNumber::Error,
                "ERROR",
                Regex::new(r"^(error|err)\d*$").unwrap(),
            ),
            (
                "fatal",
                SeverityNumber::Fatal,
                "FATAL",
                Regex::new(r"^(fatal|critical|crit|panic|emerg|alert)\d*$").unwrap(),
            ),
        ]
    })
}

/// Map log level string to OpenTelemetry severity number and text using regex
pub fn map_log_level(level: &str) -> Option<(SeverityNumber, String)> {
    let level_lower = level.to_lowercase();

    // Try to match against known patterns
    for (_name, severity_number, severity_text, regex) in get_level_regexes() {
        if regex.is_match(&level_lower) {
            return Some((*severity_number, severity_text.to_string()));
        }
    }

    // No match found
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_lambda_events::cloudwatch_logs::LogEntry;
    use opentelemetry_proto::tonic::logs::v1::SeverityNumber;

    /// Test utility: Create a LogEntry from a message string
    fn create_log_entry(message: &str) -> LogEntry {
        let mut log_entry = LogEntry::default();
        log_entry.id = "test-id".to_string();
        log_entry.timestamp = 1000;
        log_entry.message = message.to_string();
        log_entry
    }

    /// Test utility: Parse a log message and return the LogRecord
    fn parse_log_msg(message: &str, platform: LogPlatform, parser_type: ParserType) -> LogRecord {
        let log_entry = create_log_entry(message);
        let parser = RecordParser::new(platform, parser_type, None);
        parser.parse(123456789, log_entry)
    }

    #[test]
    fn test_parse_json_record_with_all_fields() {
        let json_msg = r#"{
            "level": "info",
            "msg": "test message",
            "ts": 1704110400.0,
            "trace_id": "0123456789abcdef0123456789abcdef",
            "span_id": "0123456789abcdef",
            "user": "john",
            "action": "login"
        }"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown, ParserType::Json);

        // Check severity
        assert_eq!(log_record.severity_number, SeverityNumber::Info as i32);
        assert_eq!(log_record.severity_text, "INFO");

        // Check body
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
                &body.value
            {
                assert_eq!(s, "test message");
            } else {
                panic!("Expected StringValue in body");
            }
        }

        // Check timestamp
        assert!(log_record.time_unix_nano > 0);

        // Check trace and span IDs
        assert_eq!(log_record.trace_id.len(), 16);
        assert_eq!(log_record.span_id.len(), 8);

        // Check attributes
        assert!(log_record.attributes.iter().any(|kv| kv.key == "user"));
        assert!(log_record.attributes.iter().any(|kv| kv.key == "action"));
    }

    #[test]
    fn test_parse_keyvalue_record() {
        let kv_msg =
            r#"time="2025-12-24T19:48:32Z" level=info msg="access granted" user=john action=login"#;
        let log_record = parse_log_msg(kv_msg, LogPlatform::Eks, ParserType::KeyValue);

        // Check severity
        assert_eq!(log_record.severity_number, SeverityNumber::Info as i32);
        assert_eq!(log_record.severity_text, "INFO");

        // Check body
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
                &body.value
            {
                assert_eq!(s, "access granted");
            } else {
                panic!("Expected StringValue in body");
            }
        }

        // Check timestamp was parsed
        assert!(log_record.time_unix_nano > 0);

        // Check attributes
        assert!(log_record.attributes.iter().any(|kv| kv.key == "user"));
        assert!(log_record.attributes.iter().any(|kv| kv.key == "action"));
    }

    #[test]
    fn test_parse_cloudtrail_with_credential_stripping() {
        let json_msg = r#"{
    "eventVersion": "1.08",
    "eventTime": "2024-01-15T10:30:45Z",
    "eventSource": "sts.amazonaws.com",
    "eventName": "AssumeRole",
    "eventType": "AwsApiCall",
    "awsRegion": "us-east-1",
    "requestParameters": {
        "roleArn": "arn:aws:iam::123456789012:role/MyRole"
    },
    "responseElements": {
        "credentials": {
            "accessKeyId": "ASIATESTACCESSKEY123",
            "secretAccessKey": "SECRET_KEY_VALUE",
            "sessionToken": "SESSION_TOKEN_VALUE",
            "expiration": "Jan 15, 2024 11:30:45 AM"
        },
        "assumedRoleUser": {
            "arn": "arn:aws:sts::123456789012:assumed-role/MyRole/session123"
        }
    },
    "recipientAccountId": "123456789012"
}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Cloudtrail, ParserType::Json);

        // Check body is set to eventType::eventName
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
                &body.value
            {
                assert_eq!(s, "AwsApiCall::AssumeRole");
            } else {
                panic!("Expected StringValue in body");
            }
        }

        // Find the responseElements attribute
        let response_elements_attr = log_record
            .attributes
            .iter()
            .find(|kv| kv.key == "responseElements");
        assert!(
            response_elements_attr.is_some(),
            "responseElements should be present"
        );

        // Verify that sensitive credentials were stripped
        if let Some(attr) = response_elements_attr {
            if let Some(any_value) = &attr.value {
                if let Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(json_str),
                ) = &any_value.value
                {
                    // The responseElements is stored as a JSON string in attributes
                    // Parse it to verify credentials were stripped
                    let response_obj: serde_json::Value =
                        serde_json::from_str(json_str).expect("Should parse JSON");

                    if let Some(credentials) =
                        response_obj.get("credentials").and_then(|v| v.as_object())
                    {
                        assert!(
                            !credentials.contains_key("accessKeyId"),
                            "accessKeyId should be stripped from credentials"
                        );
                        assert!(
                            !credentials.contains_key("secretAccessKey"),
                            "secretAccessKey should be stripped from credentials"
                        );
                        assert!(
                            !credentials.contains_key("sessionToken"),
                            "sessionToken should be stripped from credentials"
                        );
                        // expiration is not in the strip list, so it should remain
                        assert!(
                            credentials.contains_key("expiration"),
                            "expiration should remain"
                        );
                    } else {
                        panic!("credentials object not found in responseElements");
                    }

                    // Verify assumedRoleUser is still present
                    assert!(
                        response_obj.get("assumedRoleUser").is_some(),
                        "assumedRoleUser should still be present"
                    );
                }
            }
        }

        // Verify other fields are present as attributes
        assert!(
            log_record
                .attributes
                .iter()
                .any(|kv| kv.key == "eventSource")
        );
        assert!(log_record.attributes.iter().any(|kv| kv.key == "awsRegion"));
    }

    #[test]
    fn test_parse_unknown_type_json_autodetect() {
        let json_msg = r#"{"msg":"test message","level":"debug"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown, ParserType::Unknown);

        // Should auto-detect as JSON and parse correctly
        assert_eq!(log_record.severity_number, SeverityNumber::Debug as i32);
        assert!(log_record.body.is_some());
    }

    #[test]
    fn test_parse_unknown_type_plain_text() {
        let plain_msg = "This is just a plain text message";
        let log_record = parse_log_msg(plain_msg, LogPlatform::Unknown, ParserType::Unknown);

        // Should treat as plain text
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
                &body.value
            {
                assert_eq!(s, plain_msg);
            } else {
                panic!("Expected StringValue in body");
            }
        }
    }
}
