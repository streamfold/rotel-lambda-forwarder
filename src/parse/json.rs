//! JSON Parser Module
//!
//! This module provides JSON parsing functionality for CloudWatch log entries.
//! It converts JSON log messages into `serde_json::Map` structures that can be
//! processed by the unified RecordParser.
//!

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use serde_json::Value as JsonValue;

use crate::parse::{cwlogs::ParserError, record_parser::RecordParserError};

/// Parse a JSON log entry and return the parsed map
/// Returns an error if the message is not valid JSON or not an object
pub(crate) fn parse_json_to_map(
    msg: String,
) -> Result<serde_json::Map<String, JsonValue>, RecordParserError> {
    // Parse the message as JSON
    let json_map: serde_json::Map<String, JsonValue> = match serde_json::from_str(&msg) {
        Ok(JsonValue::Object(map)) => map,
        Ok(_) => {
            // Not an object
            return Err(RecordParserError(
                ParserError::FormatParseError("JSON log entry is not an object".to_string()),
                msg,
            ));
        }
        Err(e) => {
            // Failed to parse JSON
            return Err(RecordParserError(
                ParserError::JsonParseError(e.to_string()),
                msg,
            ));
        }
    };

    Ok(json_map)
}

/// Convert a serde_json::Value to an OpenTelemetry AnyValue
pub fn json_value_to_any_value(value: JsonValue) -> AnyValue {
    match value {
        JsonValue::Null => AnyValue { value: None },
        JsonValue::Bool(b) => AnyValue {
            value: Some(Value::BoolValue(b)),
        },
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                AnyValue {
                    value: Some(Value::IntValue(i)),
                }
            } else if let Some(f) = n.as_f64() {
                AnyValue {
                    value: Some(Value::DoubleValue(f)),
                }
            } else {
                AnyValue {
                    value: Some(Value::StringValue(n.to_string())),
                }
            }
        }
        JsonValue::String(s) => AnyValue {
            value: Some(Value::StringValue(s)),
        },
        JsonValue::Array(arr) => {
            let values = arr.into_iter().map(json_value_to_any_value).collect();
            AnyValue {
                value: Some(Value::ArrayValue(
                    opentelemetry_proto::tonic::common::v1::ArrayValue { values },
                )),
            }
        }
        JsonValue::Object(obj) => {
            let values = obj
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.clone(),
                    value: Some(json_value_to_any_value(v)),
                })
                .collect();
            AnyValue {
                value: Some(Value::KvlistValue(
                    opentelemetry_proto::tonic::common::v1::KeyValueList { values },
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::cwlogs::{LogPlatform, ParserType};
    use crate::parse::record_parser::{RecordParser, map_log_level};
    use aws_lambda_events::cloudwatch_logs::LogEntry;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, SeverityNumber};

    /// Test utility: Create a LogEntry from a message string
    fn create_log_entry(message: &str) -> LogEntry {
        let mut log_entry = LogEntry::default();
        log_entry.id = "test-id".to_string();
        log_entry.timestamp = 1000;
        log_entry.message = message.to_string();
        log_entry
    }

    /// Test utility: Parse a log message and return the LogRecord
    fn parse_log_msg(message: &str, platform: LogPlatform) -> LogRecord {
        let log_entry = create_log_entry(message);
        let parser = RecordParser::new(platform, ParserType::Json, None);
        parser.parse(123456789, log_entry)
    }

    #[test]
    fn test_parse_json_with_level() {
        let json_msg = r#"{"level":"info","msg":"test message"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        assert_eq!(log_record.severity_number, SeverityNumber::Info as i32);
        assert_eq!(log_record.severity_text, "INFO");
        assert!(log_record.body.is_some());
    }

    #[test]
    fn test_parse_json_body_fields() {
        // Test 'msg' field
        let json_msg = r#"{"msg":"test message"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "test message");
            }
        }

        // Test 'log' field (when 'msg' not present)
        let json_msg = r#"{"log":"test log"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);
        assert!(log_record.body.is_some());

        // Test 'message' field (when 'msg' and 'log' not present)
        let json_msg = r#"{"message":"test message field"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);
        assert!(log_record.body.is_some());
    }

    #[test]
    fn test_parse_json_timestamp_float() {
        let json_msg = r#"{"ts":1234567890.5,"msg":"test"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        // Should convert seconds to nanoseconds
        assert_eq!(log_record.time_unix_nano, 1234567890500000000);
    }

    #[test]
    fn test_parse_json_timestamp_rfc3339() {
        let json_msg = r#"{"timestamp":"2024-01-01T12:00:00Z","msg":"test"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        // Should parse RFC3339 timestamp
        assert!(log_record.time_unix_nano > 0);
    }

    #[test]
    fn test_parse_json_attributes() {
        let json_msg =
            r#"{"level":"error","msg":"test","user_id":123,"session":"abc123","active":true}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        // Should have attributes for fields not extracted (including cloudwatch.id)
        assert!(log_record.attributes.len() > 0);

        // Check for specific attributes
        let has_user_id = log_record.attributes.iter().any(|kv| kv.key == "user_id");
        let has_session = log_record.attributes.iter().any(|kv| kv.key == "session");
        let has_active = log_record.attributes.iter().any(|kv| kv.key == "active");

        assert!(has_user_id);
        assert!(has_session);
        assert!(has_active);
    }

    #[test]
    fn test_parse_json_with_trace_id() {
        let json_msg = r#"{"msg":"test","trace_id":"0123456789abcdef0123456789abcdef"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        assert_eq!(log_record.trace_id.len(), 16);
    }

    #[test]
    fn test_parse_json_with_span_id() {
        let json_msg = r#"{"msg":"test","span_id":"0123456789abcdef"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        assert_eq!(log_record.span_id.len(), 8);
    }

    #[test]
    fn test_parse_json_with_both_trace_and_span_id() {
        let json_msg = r#"{
            "msg":"test",
            "trace_id":"0123456789abcdef0123456789abcdef",
            "span_id":"0123456789abcdef"
        }"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        assert_eq!(log_record.trace_id.len(), 16);
        assert_eq!(log_record.span_id.len(), 8);
    }

    #[test]
    fn test_parse_json_with_invalid_trace_id_length() {
        let json_msg = r#"{"msg":"test","trace_id":"0123456789abcdef"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        // trace_id should be empty because it's not 16 bytes
        assert_eq!(log_record.trace_id.len(), 0);
    }

    #[test]
    fn test_parse_json_with_invalid_span_id_length() {
        let json_msg = r#"{"msg":"test","span_id":"0123"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        // span_id should be empty because it's not 8 bytes
        assert_eq!(log_record.span_id.len(), 0);
    }

    #[test]
    fn test_parse_json_with_non_hex_trace_id() {
        let json_msg = r#"{"msg":"test","trace_id":"not-a-hex-string-xxxxxxx"}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Unknown);

        assert_eq!(log_record.trace_id.len(), 0);
    }

    #[test]
    fn test_map_log_level() {
        // Basic levels
        assert_eq!(map_log_level("info").unwrap().0, SeverityNumber::Info);
        assert_eq!(map_log_level("INFO").unwrap().0, SeverityNumber::Info);
        assert_eq!(map_log_level("error").unwrap().0, SeverityNumber::Error);
        assert_eq!(map_log_level("ERROR").unwrap().0, SeverityNumber::Error);
        assert_eq!(map_log_level("warn").unwrap().0, SeverityNumber::Warn);
        assert_eq!(map_log_level("warning").unwrap().0, SeverityNumber::Warn);
        assert_eq!(map_log_level("debug").unwrap().0, SeverityNumber::Debug);
        assert_eq!(map_log_level("trace").unwrap().0, SeverityNumber::Trace);
        assert_eq!(map_log_level("fatal").unwrap().0, SeverityNumber::Fatal);

        // Numbered variants
        assert_eq!(map_log_level("info1").unwrap().0, SeverityNumber::Info);
        assert_eq!(map_log_level("info2").unwrap().0, SeverityNumber::Info);
        assert_eq!(map_log_level("debug3").unwrap().0, SeverityNumber::Debug);
        assert_eq!(map_log_level("error4").unwrap().0, SeverityNumber::Error);
        assert_eq!(map_log_level("warn1").unwrap().0, SeverityNumber::Warn);
        assert_eq!(map_log_level("warning2").unwrap().0, SeverityNumber::Warn);
        assert_eq!(map_log_level("trace1").unwrap().0, SeverityNumber::Trace);
        assert_eq!(map_log_level("fatal1").unwrap().0, SeverityNumber::Fatal);

        // Alternate names
        assert_eq!(
            map_log_level("information").unwrap().0,
            SeverityNumber::Info
        );
        assert_eq!(map_log_level("err").unwrap().0, SeverityNumber::Error);
        assert_eq!(map_log_level("critical").unwrap().0, SeverityNumber::Fatal);
        assert_eq!(map_log_level("crit").unwrap().0, SeverityNumber::Fatal);
        assert_eq!(map_log_level("panic").unwrap().0, SeverityNumber::Fatal);
        assert_eq!(map_log_level("emerg").unwrap().0, SeverityNumber::Fatal);
        assert_eq!(map_log_level("alert").unwrap().0, SeverityNumber::Fatal);

        // Unknown levels should return None
        assert!(map_log_level("unknown").is_none());
        assert!(map_log_level("custom").is_none());
    }

    #[test]
    fn test_json_value_to_any_value() {
        // Test string
        let json_val = JsonValue::String("test".to_string());
        let any_val = json_value_to_any_value(json_val);
        assert!(matches!(any_val.value, Some(Value::StringValue(_))));

        // Test integer
        let json_val = JsonValue::Number(serde_json::Number::from(42));
        let any_val = json_value_to_any_value(json_val);
        assert!(matches!(any_val.value, Some(Value::IntValue(_))));

        // Test boolean
        let json_val = JsonValue::Bool(true);
        let any_val = json_value_to_any_value(json_val);
        assert!(matches!(any_val.value, Some(Value::BoolValue(_))));

        // Test array
        let json_val = JsonValue::Array(vec![JsonValue::String("a".to_string())]);
        let any_val = json_value_to_any_value(json_val);
        assert!(matches!(any_val.value, Some(Value::ArrayValue(_))));
    }

    #[test]
    fn test_parse_cloudtrail_aws_vpce_event() {
        let json_msg = r#"{
    "eventVersion": "1.09",
    "userIdentity": {
        "type": "AssumedRole",
        "principalId": "ASIAIOSFODNN7EXAMPLE:role-name",
        "arn": "arn:aws:sts::123456789012:assumed-role/Admin/role-name",
        "accountId": "123456789012",
        "accessKeyId": "ASIAIOSFODNN7EXAMPLE",
        "sessionContext": {
            "sessionIssuer": {
                "type": "Role",
                "principalId": "ASIAIOSFODNN7EXAMPLE",
                "arn": "arn:aws:iam::123456789012:role/Admin",
                "accountId": "123456789012",
                "userName": "Admin"
            },
            "attributes": {
                "creationDate": "2024-06-04T23:10:46Z",
                "mfaAuthenticated": "false"
            }
        }
    },
    "eventTime": "2024-06-04T23:12:50Z",
    "eventSource": "kms.amazonaws.com",
    "eventName": "ListKeys",
    "awsRegion": "us-east-1",
    "sourceIPAddress": "192.0.2.0",
    "requestID": "16bcc089-ac49-43f1-9177-EXAMPLE23731",
    "eventID": "228ca3c8-5f95-4a8a-9732-EXAMPLE60ed9",
    "eventType": "AwsVpceEvent",
    "recipientAccountId": "123456789012",
    "sharedEventID": "a1f3720c-ef19-47e9-a5d5-EXAMPLE8099f",
    "vpcEndpointId": "vpce-EXAMPLE08c1b6b9b7",
    "vpcEndpointAccountId": "123456789012",
    "eventCategory": "NetworkActivity"
}"#;
        let log_record = parse_log_msg(json_msg, LogPlatform::Cloudtrail);

        // Verify the body is set to "AwsVpceEvent::ListKeys"
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "AwsVpceEvent::ListKeys");
            } else {
                panic!("Expected StringValue in body");
            }
        }

        // Verify that eventType and eventName are still in attributes
        let has_event_type = log_record.attributes.iter().any(|kv| kv.key == "eventType");
        let has_event_name = log_record.attributes.iter().any(|kv| kv.key == "eventName");
        assert!(has_event_type);
        assert!(has_event_name);

        // Verify other fields are present as attributes
        let has_event_source = log_record
            .attributes
            .iter()
            .any(|kv| kv.key == "eventSource");
        let has_aws_region = log_record.attributes.iter().any(|kv| kv.key == "awsRegion");
        assert!(has_event_source);
        assert!(has_aws_region);
    }
}
