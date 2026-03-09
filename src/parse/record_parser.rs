use std::sync::OnceLock;

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, SeverityNumber},
};
use regex::Regex;
use serde_json::Value as JsonValue;
use tracing::debug;

use crate::parse::{
    field_stripper::FieldStripper,
    json::json_value_to_any_value,
    platform::{LogPlatform, ParserError},
};

/// Wraps a parse error together with the original raw message so that callers can fall back
/// to treating the message as plain text.
#[derive(Debug)]
pub(crate) struct RecordParserError(pub(crate) ParserError, pub(crate) String);

// ── LogBuilder ───────────────────────────────────────────────────────────────────────────────────

/// Platform-aware factory for building OpenTelemetry [`LogRecord`]s.
///
/// `LogBuilder` is the long-lived, reusable part: it holds the platform identity and the
/// [`FieldStripper`] configuration that are shared across all records produced from the same
/// log source.  Call [`LogBuilder::start`] to obtain a per-record [`LogRecordBuilder`] that
/// accumulates state and ultimately returns the finished [`LogRecord`].
pub(crate) struct LogBuilder {
    platform: LogPlatform,
    field_stripper: FieldStripper,
}

impl LogBuilder {
    /// Create a new `LogBuilder` for the given platform.
    pub(crate) fn new(platform: LogPlatform) -> Self {
        Self {
            platform,
            field_stripper: FieldStripper::new(platform),
        }
    }

    /// Begin building a new [`LogRecord`].
    ///
    /// * `now_nanos` — wall-clock nanoseconds used as `observed_time_unix_nano` and as a
    ///   fallback `time_unix_nano` when `timestamp_ms` would overflow.
    /// * `timestamp_ms` — millisecond-precision event timestamp converted to `time_unix_nano`.
    /// * `initial_attributes` — zero or more [`KeyValue`] pairs prepended to the record's
    ///   attribute list before any map-derived attributes are added (e.g. `cloudwatch.id`).
    pub(crate) fn start(
        &self,
        now_nanos: u64,
        timestamp_ms: i64,
        initial_attributes: Vec<KeyValue>,
    ) -> LogRecordBuilder<'_> {
        let lr = LogRecord {
            time_unix_nano: timestamp_ms_to_ns(timestamp_ms, now_nanos),
            observed_time_unix_nano: now_nanos,
            attributes: initial_attributes,
            ..Default::default()
        };
        LogRecordBuilder {
            lr,
            platform: self.platform,
            field_stripper: &self.field_stripper,
        }
    }

    /// Convenience: build a [`LogRecord`] directly from a pre-parsed JSON map.
    ///
    /// Used by `s3logs` for JSON-blob files where the entire file is a `Records` array
    /// and each element has already been deserialised.
    pub(crate) fn parse_from_map(
        &self,
        now_nanos: u64,
        timestamp_ms: i64,
        json_map: serde_json::Map<String, JsonValue>,
    ) -> LogRecord {
        let mut builder = self.start(now_nanos, timestamp_ms, vec![]);
        builder.populate_from_map(json_map);
        builder.finish()
    }
}

// ── LogRecordBuilder ─────────────────────────────────────────────────────────────────────────────

/// Per-record builder returned by [`LogBuilder::start`].
///
/// Holds the partially-constructed [`LogRecord`] and a reference back to the parent
/// [`LogBuilder`]'s platform configuration and field stripper.  Obtain a finished record by
/// calling [`LogRecordBuilder::finish`].
pub(crate) struct LogRecordBuilder<'a> {
    lr: LogRecord,
    platform: LogPlatform,
    field_stripper: &'a FieldStripper,
}

impl<'a> LogRecordBuilder<'a> {
    /// Set the record body to a plain-text string value.
    pub(crate) fn set_body_text(mut self, text: String) -> Self {
        self.lr.body = Some(AnyValue {
            value: Some(Value::StringValue(text)),
        });
        self
    }

    /// Strip sensitive fields from `json_map` then extract well-known fields (severity, body,
    /// timestamp, trace/span IDs) and platform-specific fallbacks into the in-progress record.
    /// Any remaining map entries become log record attributes.
    pub(crate) fn populate_from_map(&mut self, mut json_map: serde_json::Map<String, JsonValue>) {
        // Strip sensitive fields before anything else.
        self.field_stripper.strip_fields(&mut json_map);

        // ── Severity ────────────────────────────────────────────────────────────────
        if let Some(level_value) = json_map.remove("level") {
            if let JsonValue::String(ref level_str) = level_value {
                if let Some((severity_number, severity_text)) = map_log_level(level_str) {
                    self.lr.severity_number = severity_number as i32;
                    self.lr.severity_text = severity_text;
                } else {
                    // Not a recognised level string — keep as an attribute.
                    json_map.insert("level".to_string(), level_value);
                }
            } else {
                // Not a string — keep as an attribute.
                json_map.insert("level".to_string(), level_value);
            }
        }

        // ── Body ────────────────────────────────────────────────────────────────────
        // Priority order: 'msg', 'log', 'message'.
        for key in ["msg", "log", "message"] {
            if let Some(body_value) = json_map.remove(key) {
                self.lr.body = Some(json_value_to_any_value(body_value));
                break;
            }
        }

        // ── Timestamp ───────────────────────────────────────────────────────────────
        // Priority order: 'ts', 'time', 'timestamp'.
        for key in ["ts", "time", "timestamp"] {
            if let Some(ts_value) = json_map.remove(key) {
                if let Some(parsed_nanos) = parse_timestamp(&ts_value) {
                    self.lr.time_unix_nano = parsed_nanos;
                }
                break;
            }
        }

        // ── Trace / Span IDs ────────────────────────────────────────────────────────
        if let Some(JsonValue::String(trace_id_str)) = json_map.remove("trace_id") {
            if let Ok(trace_bytes) = decode_hex(&trace_id_str) {
                if trace_bytes.len() == 16 {
                    self.lr.trace_id = trace_bytes;
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
                    self.lr.span_id = span_bytes;
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

        // ── Platform-specific body fallback ─────────────────────────────────────────
        // CloudTrail: synthesise "eventType::eventName" when no body field was found.
        if self.platform == LogPlatform::Cloudtrail && self.lr.body.is_none() {
            let event_type = json_map.get("eventType");
            let event_name = json_map.get("eventName");
            self.lr.body = match (event_type, event_name) {
                (Some(JsonValue::String(t)), Some(JsonValue::String(n))) => Some(AnyValue {
                    value: Some(Value::StringValue(format!("{}::{}", t, n))),
                }),
                (Some(JsonValue::String(t)), None) => Some(AnyValue {
                    value: Some(Value::StringValue(t.to_string())),
                }),
                _ => None,
            };
        }

        // ── Remaining fields → attributes ────────────────────────────────────────────
        for (key, value) in json_map {
            self.lr.attributes.push(KeyValue {
                key,
                value: Some(json_value_to_any_value(value)),
            });
        }
    }

    /// Consume the builder and return the finished [`LogRecord`].
    pub(crate) fn finish(self) -> LogRecord {
        self.lr
    }
}

// ── Free helper functions ────────────────────────────────────────────────────────────────────────

/// Convert a millisecond timestamp to nanoseconds, falling back on overflow or negative values.
pub(crate) fn timestamp_ms_to_ns(timestamp_ms: i64, fallback_ns: u64) -> u64 {
    timestamp_ms
        .checked_mul(1_000_000)
        .filter(|&ns| ns >= 0)
        .map(|ns| ns as u64)
        .unwrap_or(fallback_ns)
}

/// Hex-decode a string, returning an error message on failure.
pub(crate) fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    hex::decode(s).map_err(|e| format!("Hex decode error: {}", e))
}

/// Parse a timestamp value (supports RFC 3339 strings and numeric Unix timestamps).
///
/// Numbers ≤ 1 × 10¹² are treated as seconds; larger numbers as milliseconds.
/// Returns the timestamp as nanoseconds, or `None` on overflow / invalid input.
pub(crate) fn parse_timestamp(value: &JsonValue) -> Option<u64> {
    match value {
        JsonValue::Number(n) => {
            if let Some(ts_float) = n.as_f64() {
                if ts_float > 1e12 {
                    // Milliseconds → nanoseconds
                    let millis = ts_float * 1_000.0;
                    if millis < 0.0 || millis > u64::MAX as f64 {
                        return None;
                    }
                    Some(millis as u64)
                } else {
                    // Seconds → nanoseconds
                    let nanos = ts_float * 1_000_000_000.0;
                    if nanos < 0.0 || nanos > u64::MAX as f64 {
                        return None;
                    }
                    Some(nanos as u64)
                }
            } else {
                None
            }
        }
        JsonValue::String(s) => chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0) as u64),
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

/// Map a log level string to an OpenTelemetry `SeverityNumber` and canonical text.
///
/// Returns `None` if the string does not match any known level pattern.
pub fn map_log_level(level: &str) -> Option<(SeverityNumber, String)> {
    let level_lower = level.to_lowercase();
    for (_name, severity_number, severity_text, regex) in get_level_regexes() {
        if regex.is_match(&level_lower) {
            return Some((*severity_number, severity_text.to_string()));
        }
    }
    None
}

// ── Tests ────────────────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::logs::v1::SeverityNumber;

    fn build_from_json(json_msg: &str, platform: LogPlatform) -> LogRecord {
        let map: serde_json::Map<String, JsonValue> =
            serde_json::from_str(json_msg).expect("valid JSON object");
        LogBuilder::new(platform).parse_from_map(123_456_789, 1000, map)
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
        let lr = build_from_json(json_msg, LogPlatform::Unknown);

        assert_eq!(lr.severity_number, SeverityNumber::Info as i32);
        assert_eq!(lr.severity_text, "INFO");

        assert!(lr.body.is_some());
        if let Some(body) = &lr.body {
            if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
                &body.value
            {
                assert_eq!(s, "test message");
            } else {
                panic!("Expected StringValue in body");
            }
        }

        assert!(lr.time_unix_nano > 0);
        assert_eq!(lr.trace_id.len(), 16);
        assert_eq!(lr.span_id.len(), 8);
        assert!(lr.attributes.iter().any(|kv| kv.key == "user"));
        assert!(lr.attributes.iter().any(|kv| kv.key == "action"));
    }

    #[test]
    fn test_parse_cloudtrail_body_synthesis() {
        let json_msg = r#"{
            "eventType": "AwsApiCall",
            "eventName": "AssumeRole",
            "eventSource": "sts.amazonaws.com"
        }"#;
        let lr = build_from_json(json_msg, LogPlatform::Cloudtrail);

        assert!(lr.body.is_some());
        if let Some(body) = &lr.body {
            if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
                &body.value
            {
                assert_eq!(s, "AwsApiCall::AssumeRole");
            } else {
                panic!("Expected StringValue in body");
            }
        }
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
        let lr = build_from_json(json_msg, LogPlatform::Cloudtrail);

        assert!(lr.body.is_some());
        if let Some(body) = &lr.body {
            if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
                &body.value
            {
                assert_eq!(s, "AwsApiCall::AssumeRole");
            } else {
                panic!("Expected StringValue in body");
            }
        }

        let response_elements_attr = lr.attributes.iter().find(|kv| kv.key == "responseElements");
        assert!(
            response_elements_attr.is_some(),
            "responseElements should be present"
        );

        if let Some(attr) = response_elements_attr {
            if let Some(any_value) = &attr.value {
                if let Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(json_str),
                ) = &any_value.value
                {
                    let response_obj: serde_json::Value =
                        serde_json::from_str(json_str).expect("Should parse JSON");

                    if let Some(credentials) =
                        response_obj.get("credentials").and_then(|v| v.as_object())
                    {
                        assert!(
                            !credentials.contains_key("sessionToken"),
                            "sessionToken should be stripped"
                        );
                        assert!(
                            credentials.contains_key("expiration"),
                            "expiration should remain"
                        );
                    } else {
                        panic!("credentials object not found in responseElements");
                    }
                    assert!(response_obj.get("assumedRoleUser").is_some());
                }
            }
        }

        assert!(lr.attributes.iter().any(|kv| kv.key == "eventSource"));
        assert!(lr.attributes.iter().any(|kv| kv.key == "awsRegion"));
    }

    #[test]
    fn test_start_sets_timestamps_and_initial_attributes() {
        let builder = LogBuilder::new(LogPlatform::Unknown);
        let initial_attrs = vec![KeyValue {
            key: "source".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue("test".to_string())),
            }),
        }];
        let lr = builder.start(999, 500, initial_attrs).finish();

        assert_eq!(lr.observed_time_unix_nano, 999);
        assert_eq!(lr.time_unix_nano, 500_000_000); // 500ms → ns
        assert!(lr.attributes.iter().any(|kv| kv.key == "source"));
    }

    #[test]
    fn test_set_body_text() {
        let builder = LogBuilder::new(LogPlatform::Unknown);
        let lr = builder
            .start(0, 0, vec![])
            .set_body_text("hello world".to_string())
            .finish();

        if let Some(body) = &lr.body {
            if let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) =
                &body.value
            {
                assert_eq!(s, "hello world");
            } else {
                panic!("expected StringValue body");
            }
        } else {
            panic!("body should be set");
        }
    }

    #[test]
    fn test_parse_timestamp_seconds() {
        let value = JsonValue::Number(serde_json::Number::from_f64(1704110400.0).unwrap());
        assert_eq!(parse_timestamp(&value), Some(1704110400_000_000_000u64));
    }

    #[test]
    fn test_parse_timestamp_milliseconds() {
        let value = JsonValue::Number(serde_json::Number::from_f64(1704110400_000.0).unwrap());
        assert_eq!(parse_timestamp(&value), Some(1704110400_000_000u64));
    }

    #[test]
    fn test_parse_timestamp_seconds_overflow() {
        let value = JsonValue::Number(serde_json::Number::from_f64(1e11).unwrap());
        assert_eq!(parse_timestamp(&value), None);
    }

    #[test]
    fn test_parse_timestamp_milliseconds_overflow() {
        let value = JsonValue::Number(serde_json::Number::from_f64(1e19).unwrap());
        assert_eq!(parse_timestamp(&value), None);
    }

    #[test]
    fn test_parse_timestamp_negative_seconds() {
        let value = JsonValue::Number(serde_json::Number::from_f64(-1.0).unwrap());
        assert_eq!(parse_timestamp(&value), None);
    }

    #[test]
    fn test_parse_timestamp_negative_milliseconds() {
        let value = JsonValue::Number(serde_json::Number::from_f64(-2e12).unwrap());
        assert_eq!(parse_timestamp(&value), None);
    }

    #[test]
    fn test_parse_timestamp_rfc3339_string() {
        let value = JsonValue::String("2024-01-01T12:00:00Z".to_string());
        let result = parse_timestamp(&value);
        assert!(result.is_some());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn test_parse_timestamp_invalid_string() {
        let value = JsonValue::String("not-a-timestamp".to_string());
        assert_eq!(parse_timestamp(&value), None);
    }

    #[test]
    fn test_parse_timestamp_non_number() {
        let value = JsonValue::Bool(true);
        assert_eq!(parse_timestamp(&value), None);
    }
}
