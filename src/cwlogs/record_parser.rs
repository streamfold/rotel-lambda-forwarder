use std::sync::Arc;

use aws_lambda_events::cloudwatch_logs::LogEntry;
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use tracing::warn;

use crate::cwlogs::ParserType;
use crate::flowlogs::ParsedFields;
use crate::parse::{
    json::parse_json_to_map,
    keyvalue::parse_keyvalue_to_map,
    platform::LogPlatform,
    record_parser::{LogBuilder, RecordParserError},
    utils::string_kv,
    vpclog::parse_vpclog_to_map,
};

/// The outcome of attempting to parse a raw log message string into a structured map.
enum ParsedMessage {
    /// The message was parsed into a JSON map; the caller should call `populate_from_map`.
    Map(serde_json::Map<String, serde_json::Value>),
    /// The message could not or should not be structured; use this string as the plain-text body.
    /// For VPC logs, the raw line is always preserved as the body even when a map is also
    /// available — so this variant carries both the body text and an optional structured map.
    PlainText(String, Option<serde_json::Map<String, serde_json::Value>>),
    /// Parsing failed outright; use this string as the plain-text body.
    Error(RecordParserError),
}

/// CloudWatch-specific log entry parser.
///
/// `RecordParser` owns the CloudWatch concerns:
///
/// * Building the initial [`LogRecord`] skeleton via [`LogBuilder::start`], seeding it
///   with the CloudWatch millisecond timestamp and, when non-empty, the `cloudwatch.id`
///   attribute.
/// * Dispatching the raw message string to the appropriate format parser
///   (`parse_message_to_map`) based on the detected [`ParserType`].
/// * On success, handing the parsed map to [`crate::parse::record_parser::LogRecordBuilder::populate_from_map`]
///   which strips sensitive fields and extracts well-known fields into the record.
/// * On failure, preserving the raw message as a plain-text body.
///
/// All [`LogRecord`] construction is delegated to [`LogBuilder`] /
/// [`crate::parse::record_parser::LogRecordBuilder`] in the `parse` module — no
/// `LogRecord { … }` literals appear here.
pub(crate) struct RecordParser {
    parser_type: ParserType,
    flow_log_parsed_fields: Option<Arc<ParsedFields>>,
    builder: LogBuilder,
}

impl RecordParser {
    /// Create a new `RecordParser` for the given platform and parser type.
    pub(crate) fn new(
        platform: LogPlatform,
        parser_type: ParserType,
        flow_log_parsed_fields: Option<Arc<ParsedFields>>,
    ) -> Self {
        Self {
            parser_type,
            flow_log_parsed_fields,
            builder: LogBuilder::new(platform),
        }
    }

    /// Parse a CloudWatch [`LogEntry`] into an OpenTelemetry [`LogRecord`].
    ///
    /// On parse failure the raw message is preserved as the log body.
    pub(crate) fn parse(&self, now_nanos: u64, log_entry: LogEntry) -> LogRecord {
        // Seed the record with the CW timestamp and, when non-empty, the entry ID.
        let initial_attributes = if !log_entry.id.is_empty() {
            vec![string_kv("cloudwatch.id", log_entry.id)]
        } else {
            vec![]
        };

        let mut record_builder =
            self.builder
                .start(now_nanos, log_entry.timestamp, initial_attributes);

        match self.parse_message(log_entry.message) {
            ParsedMessage::Map(map) => {
                record_builder.populate_from_map(map);
            }
            ParsedMessage::PlainText(body, maybe_map) => {
                record_builder = record_builder.set_body_text(body);
                if let Some(map) = maybe_map {
                    record_builder.populate_from_map(map);
                }
            }
            ParsedMessage::Error(RecordParserError(err, msg)) => {
                warn!(error = ?err, "Failed to parse log entry, using raw text as body");
                record_builder = record_builder.set_body_text(msg);
            }
        }

        record_builder.finish()
    }

    /// Convert a raw message string into a [`ParsedMessage`] describing what the caller
    /// should do next — no mutation of external state as a side-effect.
    fn parse_message(&self, message: String) -> ParsedMessage {
        match self.parser_type {
            ParserType::Json => match parse_json_to_map(message) {
                Ok(map) => ParsedMessage::Map(map),
                Err(e) => ParsedMessage::Error(e),
            },

            ParserType::KeyValue => match parse_keyvalue_to_map(message) {
                Ok(map) => ParsedMessage::Map(map),
                Err(e) => ParsedMessage::Error(e),
            },

            ParserType::VpcLog => {
                // VPC Flow Logs always preserve the raw line as the body.
                // If parsed fields are available, also return a structured map so that
                // individual flow-log fields are emitted as attributes.
                match self.flow_log_parsed_fields.as_ref() {
                    Some(parsed_fields) => {
                        match parse_vpclog_to_map(message.clone(), parsed_fields.clone()) {
                            Ok(map) => ParsedMessage::PlainText(message, Some(map)),
                            Err(e) => ParsedMessage::Error(e),
                        }
                    }
                    // No field definitions available — just preserve the raw body.
                    None => ParsedMessage::PlainText(message, None),
                }
            }

            ParserType::Unknown => {
                // Auto-detect: attempt JSON for messages that look like objects;
                // otherwise treat as opaque plain text.
                if message.len() > 2 && message.starts_with('{') {
                    match parse_json_to_map(message) {
                        Ok(map) => ParsedMessage::Map(map),
                        Err(e) => ParsedMessage::Error(e),
                    }
                } else {
                    ParsedMessage::PlainText(message, None)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_lambda_events::cloudwatch_logs::LogEntry;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueInner;

    use super::*;

    fn make_entry(id: &str, timestamp: i64, message: &str) -> LogEntry {
        let mut entry = LogEntry::default();
        entry.id = id.to_string();
        entry.timestamp = timestamp;
        entry.message = message.to_string();
        entry
    }

    fn parse(message: &str, platform: LogPlatform, parser_type: ParserType) -> LogRecord {
        let parser = RecordParser::new(platform, parser_type, None);
        parser.parse(123_456_789, make_entry("test-id", 1000, message))
    }

    #[test]
    fn test_cloudwatch_id_attached_as_attribute() {
        let lr = parse(r#"{"msg":"hi"}"#, LogPlatform::Unknown, ParserType::Json);
        assert!(
            lr.attributes.iter().any(|kv| kv.key == "cloudwatch.id"),
            "cloudwatch.id attribute should be present"
        );
    }

    #[test]
    fn test_cloudwatch_id_value() {
        let lr = parse(r#"{"msg":"hi"}"#, LogPlatform::Unknown, ParserType::Json);
        let attr = lr
            .attributes
            .iter()
            .find(|kv| kv.key == "cloudwatch.id")
            .expect("cloudwatch.id not found");
        if let Some(AnyValueInner::StringValue(s)) =
            attr.value.as_ref().and_then(|v| v.value.as_ref())
        {
            assert_eq!(s, "test-id");
        } else {
            panic!("expected StringValue for cloudwatch.id");
        }
    }

    #[test]
    fn test_no_cloudwatch_id_when_empty() {
        let parser = RecordParser::new(LogPlatform::Unknown, ParserType::Json, None);
        let lr = parser.parse(123_456_789, make_entry("", 1000, r#"{"msg":"hi"}"#));
        assert!(
            !lr.attributes.iter().any(|kv| kv.key == "cloudwatch.id"),
            "cloudwatch.id should be absent when id is empty"
        );
    }

    #[test]
    fn test_timestamp_set_from_entry() {
        // 1000 ms → 1_000_000_000 ns
        let lr = parse(r#"{"msg":"hi"}"#, LogPlatform::Unknown, ParserType::Json);
        assert_eq!(lr.time_unix_nano, 1_000_000_000u64);
    }

    #[test]
    fn test_parse_json_type() {
        use opentelemetry_proto::tonic::logs::v1::SeverityNumber;
        let lr = parse(
            r#"{"level":"info","msg":"hello","user":"alice"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert_eq!(lr.severity_number, SeverityNumber::Info as i32);
        assert_eq!(lr.severity_text, "INFO");
        if let Some(body) = &lr.body {
            if let Some(AnyValueInner::StringValue(s)) = &body.value {
                assert_eq!(s, "hello");
            } else {
                panic!("expected string body");
            }
        } else {
            panic!("body should be set");
        }
        assert!(lr.attributes.iter().any(|kv| kv.key == "user"));
    }

    #[test]
    fn test_parse_keyvalue_type() {
        use opentelemetry_proto::tonic::logs::v1::SeverityNumber;
        let lr = parse(
            r#"time="2025-01-01T00:00:00Z" level=warn msg="something happened" host=myhost"#,
            LogPlatform::Eks,
            ParserType::KeyValue,
        );
        assert_eq!(lr.severity_number, SeverityNumber::Warn as i32);
        if let Some(body) = &lr.body {
            if let Some(AnyValueInner::StringValue(s)) = &body.value {
                assert_eq!(s, "something happened");
            } else {
                panic!("expected string body");
            }
        } else {
            panic!("body should be set");
        }
        assert!(lr.attributes.iter().any(|kv| kv.key == "host"));
    }

    #[test]
    fn test_parse_unknown_auto_detects_json() {
        use opentelemetry_proto::tonic::logs::v1::SeverityNumber;
        let lr = parse(
            r#"{"level":"debug","msg":"auto detected"}"#,
            LogPlatform::Unknown,
            ParserType::Unknown,
        );
        assert_eq!(lr.severity_number, SeverityNumber::Debug as i32);
        assert!(lr.body.is_some());
    }

    #[test]
    fn test_parse_unknown_plain_text_fallback() {
        let msg = "just a plain log line";
        let lr = parse(msg, LogPlatform::Unknown, ParserType::Unknown);
        if let Some(body) = &lr.body {
            if let Some(AnyValueInner::StringValue(s)) = &body.value {
                assert_eq!(s, msg);
            } else {
                panic!("expected string body");
            }
        } else {
            panic!("body should be set for plain text");
        }
    }

    #[test]
    fn test_parse_invalid_json_falls_back_to_plain_text() {
        let bad_json = r#"{ this is not valid json }"#;
        let lr = parse(bad_json, LogPlatform::Unknown, ParserType::Json);
        if let Some(body) = &lr.body {
            if let Some(AnyValueInner::StringValue(s)) = &body.value {
                assert_eq!(s, bad_json);
            } else {
                panic!("expected string body on failure");
            }
        } else {
            panic!("body should be set on parse failure");
        }
    }

    #[test]
    fn test_vpc_log_body_always_set() {
        // With no parsed fields the VPC branch should still set the body.
        let raw = "2 123456789012 eni-abc 10.0.0.1 10.0.0.2 80 443 6 10 1000 0 0 ACCEPT OK";
        let parser = RecordParser::new(LogPlatform::VpcFlowLog, ParserType::VpcLog, None);
        let lr = parser.parse(123_456_789, make_entry("", 1000, raw));
        if let Some(body) = &lr.body {
            if let Some(AnyValueInner::StringValue(s)) = &body.value {
                assert_eq!(s, raw);
            } else {
                panic!("expected string body for VPC log");
            }
        } else {
            panic!("body should always be set for VPC logs");
        }
    }

    #[test]
    fn test_parse_json_with_level() {
        use opentelemetry_proto::tonic::logs::v1::SeverityNumber;
        let lr = parse(
            r#"{"level":"info","msg":"test message"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert_eq!(lr.severity_number, SeverityNumber::Info as i32);
        assert_eq!(lr.severity_text, "INFO");
        assert!(lr.body.is_some());
    }

    #[test]
    fn test_parse_json_body_fields() {
        // Test 'msg' field
        let lr = parse(
            r#"{"msg":"test message"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert!(lr.body.is_some());
        if let Some(body) = &lr.body {
            if let Some(AnyValueInner::StringValue(s)) = &body.value {
                assert_eq!(s, "test message");
            } else {
                panic!("expected string body");
            }
        }

        // Test 'log' field (when 'msg' not present)
        let lr = parse(
            r#"{"log":"test log"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert!(lr.body.is_some());

        // Test 'message' field (when 'msg' and 'log' not present)
        let lr = parse(
            r#"{"message":"test message field"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert!(lr.body.is_some());
    }

    #[test]
    fn test_parse_json_timestamp_float() {
        let lr = parse(
            r#"{"ts":1234567890.5,"msg":"test"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        // Should convert seconds to nanoseconds
        assert_eq!(lr.time_unix_nano, 1234567890500000000);
    }

    #[test]
    fn test_parse_json_timestamp_rfc3339() {
        let lr = parse(
            r#"{"timestamp":"2024-01-01T12:00:00Z","msg":"test"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        // Should parse RFC3339 timestamp
        assert!(lr.time_unix_nano > 0);
    }

    #[test]
    fn test_parse_json_attributes() {
        let lr = parse(
            r#"{"level":"error","msg":"test","user_id":123,"session":"abc123","active":true}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        // Should have attributes for fields not extracted (including cloudwatch.id)
        assert!(!lr.attributes.is_empty());
        assert!(lr.attributes.iter().any(|kv| kv.key == "user_id"));
        assert!(lr.attributes.iter().any(|kv| kv.key == "session"));
        assert!(lr.attributes.iter().any(|kv| kv.key == "active"));
    }

    #[test]
    fn test_parse_json_with_trace_id() {
        let lr = parse(
            r#"{"msg":"test","trace_id":"0123456789abcdef0123456789abcdef"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert_eq!(lr.trace_id.len(), 16);
    }

    #[test]
    fn test_parse_json_with_span_id() {
        let lr = parse(
            r#"{"msg":"test","span_id":"0123456789abcdef"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert_eq!(lr.span_id.len(), 8);
    }

    #[test]
    fn test_parse_json_with_both_trace_and_span_id() {
        let lr = parse(
            r#"{"msg":"test","trace_id":"0123456789abcdef0123456789abcdef","span_id":"0123456789abcdef"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert_eq!(lr.trace_id.len(), 16);
        assert_eq!(lr.span_id.len(), 8);
    }

    #[test]
    fn test_parse_json_with_invalid_trace_id_length() {
        let lr = parse(
            r#"{"msg":"test","trace_id":"0123456789abcdef"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        // trace_id should be empty because it's not 16 bytes
        assert_eq!(lr.trace_id.len(), 0);
    }

    #[test]
    fn test_parse_json_with_invalid_span_id_length() {
        let lr = parse(
            r#"{"msg":"test","span_id":"0123"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        // span_id should be empty because it's not 8 bytes
        assert_eq!(lr.span_id.len(), 0);
    }

    #[test]
    fn test_parse_json_with_non_hex_trace_id() {
        let lr = parse(
            r#"{"msg":"test","trace_id":"not-a-hex-string-xxxxxxx"}"#,
            LogPlatform::Unknown,
            ParserType::Json,
        );
        assert_eq!(lr.trace_id.len(), 0);
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
        let lr = parse(json_msg, LogPlatform::Cloudtrail, ParserType::Json);

        // Verify the body is set to "AwsVpceEvent::ListKeys"
        assert!(lr.body.is_some());
        if let Some(body) = &lr.body {
            if let Some(AnyValueInner::StringValue(s)) = &body.value {
                assert_eq!(s, "AwsVpceEvent::ListKeys");
            } else {
                panic!("Expected StringValue in body");
            }
        }

        // Verify that eventType and eventName are still in attributes
        assert!(lr.attributes.iter().any(|kv| kv.key == "eventType"));
        assert!(lr.attributes.iter().any(|kv| kv.key == "eventName"));

        // Verify other fields are present as attributes
        assert!(lr.attributes.iter().any(|kv| kv.key == "eventSource"));
        assert!(lr.attributes.iter().any(|kv| kv.key == "awsRegion"));
    }
}
