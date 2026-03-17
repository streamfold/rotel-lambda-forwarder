//! JSON Parser Module
//!
//! This module provides JSON parsing functionality for CloudWatch log entries.
//! It converts JSON log messages into `serde_json::Map` structures that can be
//! processed by the unified RecordParser.
//!

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use serde_json::Value as JsonValue;

use crate::parse::{platform::ParserError, record_parser::RecordParserError};

/// Parse a JSON log entry and return the parsed map
/// Returns an error if the message is not valid JSON or not an object
pub(crate) fn parse_json_to_map(
    msg: &str,
) -> Result<serde_json::Map<String, JsonValue>, RecordParserError> {
    // Parse the message as JSON
    let json_map: serde_json::Map<String, JsonValue> = match serde_json::from_str(msg) {
        Ok(JsonValue::Object(map)) => map,
        Ok(_) => {
            // Not an object
            return Err(RecordParserError(
                ParserError::FormatParseError("JSON log entry is not an object".to_string()),
                msg.to_string(),
            ));
        }
        Err(e) => {
            // Failed to parse JSON
            return Err(RecordParserError(
                ParserError::JsonParseError(e.to_string()),
                msg.to_string(),
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
    use crate::parse::record_parser::map_log_level;
    use opentelemetry_proto::tonic::logs::v1::SeverityNumber;

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
}
