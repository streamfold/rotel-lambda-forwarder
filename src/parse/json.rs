use std::sync::OnceLock;

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, SeverityNumber},
};
use regex::Regex;
use serde_json::Value as JsonValue;
use tracing::{debug, warn};

use crate::parse::cwlogs::LogPlatform;

/// Hex decode utilities
fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    hex::decode(s).map_err(|e| format!("Hex decode error: {}", e))
}

/// Parse a JSON log entry and populate a LogRecord
pub fn parse_json_log_entry(platform: LogPlatform, msg: String, lr: &mut LogRecord) {
    // Parse the message as JSON
    let mut json_map: serde_json::Map<String, JsonValue> = match serde_json::from_str(&msg) {
        Ok(JsonValue::Object(map)) => map,
        Ok(_) => {
            // Not an object, treat as plain string
            warn!("JSON log entry is not an object, treating as string");
            lr.body = Some(AnyValue {
                value: Some(Value::StringValue(msg)),
            });
            return;
        }
        Err(e) => {
            // Failed to parse JSON, treat as plain string
            warn!("Failed to parse JSON log entry: {}, treating as string", e);
            lr.body = Some(AnyValue {
                value: Some(Value::StringValue(msg)),
            });
            return;
        }
    };

    // Handle "level" field for severity
    if let Some(level_value) = json_map.remove("level")
        && let JsonValue::String(level_str) = &level_value
    {
        if let Some((severity_number, severity_text)) = map_log_level(level_str) {
            lr.severity_number = severity_number as i32;
            lr.severity_text = severity_text;
        } else {
            // put back if not matched
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

    // Check for timestamp fields: 'ts' or 'timestamp'
    let timestamp_keys = ["ts", "timestamp"];
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

    // Special handling. For now inline, abstract this as we expand
    if platform == LogPlatform::Cloudtrail && lr.body.is_none() {
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

/// Parse a timestamp from a JSON value
/// For numbers (float), assume seconds since epoch
/// For strings, try to parse as RFC3339
fn parse_timestamp(value: &JsonValue) -> Option<u64> {
    match value {
        // Convert seconds to nanoseconds
        JsonValue::Number(n) => n.as_f64().map(|f| (f * 1_000_000_000.0) as u64),
        JsonValue::String(s) => {
            // Try to parse as RFC3339
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                Some(dt.timestamp_nanos_opt().unwrap_or(0) as u64)
            } else {
                // Try parsing as integer/float string
                if let Ok(f) = s.parse::<f64>() {
                    Some((f * 1_000_000_000.0) as u64)
                } else {
                    None
                }
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_with_level() {
        let json_msg = r#"{"level":"info","msg":"test message"}"#.to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        assert_eq!(log_record.severity_number, SeverityNumber::Info as i32);
        assert_eq!(log_record.severity_text, "INFO");
        assert!(log_record.body.is_some());
    }

    #[test]
    fn test_parse_json_body_fields() {
        // Test 'msg' field
        let json_msg = r#"{"msg":"test message"}"#.to_string();
        let mut log_record = LogRecord::default();
        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "test message");
            }
        }

        // Test 'log' field (when 'msg' not present)
        let json_msg = r#"{"log":"test log"}"#.to_string();
        let mut log_record = LogRecord::default();
        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);
        assert!(log_record.body.is_some());

        // Test 'message' field (when 'msg' and 'log' not present)
        let json_msg = r#"{"message":"test message field"}"#.to_string();
        let mut log_record = LogRecord::default();
        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);
        assert!(log_record.body.is_some());
    }

    #[test]
    fn test_parse_json_timestamp_float() {
        let json_msg = r#"{"ts":1234567890.5,"msg":"test"}"#.to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        // Should convert seconds to nanoseconds
        assert_eq!(log_record.time_unix_nano, 1234567890500000000);
    }

    #[test]
    fn test_parse_json_timestamp_rfc3339() {
        let json_msg = r#"{"timestamp":"2024-01-01T12:00:00Z","msg":"test"}"#.to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        // Should parse RFC3339 timestamp
        assert!(log_record.time_unix_nano > 0);
    }

    #[test]
    fn test_parse_json_attributes() {
        let json_msg =
            r#"{"level":"error","msg":"test","user_id":123,"session":"abc123","active":true}"#
                .to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        // Should have attributes for fields not extracted
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
        let json_msg =
            r#"{"msg":"test","trace_id":"0123456789abcdef0123456789abcdef"}"#.to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        assert_eq!(log_record.trace_id.len(), 16);
    }

    #[test]
    fn test_parse_json_with_span_id() {
        let json_msg = r#"{"msg":"test","span_id":"0123456789abcdef"}"#.to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        assert_eq!(log_record.span_id.len(), 8);
    }

    #[test]
    fn test_parse_json_with_both_trace_and_span_id() {
        let json_msg = r#"{
            "msg":"test",
            "trace_id":"0123456789abcdef0123456789abcdef",
            "span_id":"0123456789abcdef"
        }"#
        .to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        assert_eq!(log_record.trace_id.len(), 16);
        assert_eq!(log_record.span_id.len(), 8);
    }

    #[test]
    fn test_parse_json_with_invalid_trace_id_length() {
        let json_msg = r#"{"msg":"test","trace_id":"0123456789abcdef"}"#.to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        // trace_id should be empty because it's not 16 bytes
        assert_eq!(log_record.trace_id.len(), 0);
    }

    #[test]
    fn test_parse_json_with_invalid_span_id_length() {
        let json_msg = r#"{"msg":"test","span_id":"0123"}"#.to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

        // span_id should be empty because it's not 8 bytes
        assert_eq!(log_record.span_id.len(), 0);
    }

    #[test]
    fn test_parse_json_with_non_hex_trace_id() {
        let json_msg = r#"{"msg":"test","trace_id":"not-a-hex-string-xxxxxxx"}"#.to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Unknown, json_msg, &mut log_record);

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
}"#
        .to_string();
        let mut log_record = LogRecord::default();

        parse_json_log_entry(LogPlatform::Cloudtrail, json_msg, &mut log_record);

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
