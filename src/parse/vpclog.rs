//! EC2 Flow Log Parser Module
//!
//! This module provides parsing functionality for AWS EC2 Flow Logs.
//! It converts EC2 Flow Log records into `serde_json::Map` structures for unified processing.
//!
//! EC2 Flow Logs are space-separated records that capture network traffic flow information.
//! The format can be customized, so this parser dynamically determines the fields based on
//! the LogFormat string retrieved from the EC2 DescribeFlowLogs API.
//!
//! Reference: https://docs.aws.amazon.com/vpc/latest/userguide/flow-log-records.html

use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::flowlogs::{ParsedFieldType, ParsedFields};
use crate::parse::{cwlogs::ParserError, record_parser::RecordParserError};

/// Parse an EC2 Flow Log record from a string using pre-parsed field names
/// Fields with value "-" are excluded from the result
/// Returns an error if the record doesn't have the expected number of fields
pub(crate) fn parse_vpclog_to_map(
    input: String,
    parsed_fields: Arc<ParsedFields>,
) -> Result<serde_json::Map<String, JsonValue>, RecordParserError> {
    match parsed_fields.as_ref() {
        ParsedFields::Success(parsed_fields) => {
            let field_values = parse_vpclog_fields(&input);

            if field_values.len() != parsed_fields.len() {
                return Err(RecordParserError(
                    ParserError::FormatParseError(format!(
                        "Expected {} fields but found {}",
                        parsed_fields.len(),
                        field_values.len()
                    )),
                    input,
                ));
            }

            let mut map = serde_json::Map::new();
            for (parsed_field, value) in parsed_fields.iter().zip(field_values.iter()) {
                // Skip fields with "-" value (not present)
                if value == "-" {
                    continue;
                }

                match parsed_field.field_type {
                    ParsedFieldType::String => {
                        map.insert(
                            parsed_field.field_name.clone(),
                            JsonValue::String(value.clone()),
                        );
                    }
                    ParsedFieldType::Int32 | ParsedFieldType::Int64 => match value.parse::<i64>() {
                        Ok(num) => {
                            map.insert(
                                parsed_field.field_name.clone(),
                                JsonValue::Number(serde_json::Number::from(num)),
                            );
                        }
                        Err(e) => {
                            return Err(RecordParserError(
                                ParserError::FormatParseError(format!(
                                    "Field {} unable to be parsed to integer: {}",
                                    parsed_field.field_name, e
                                )),
                                value.clone(),
                            ));
                        }
                    },
                }
            }

            Ok(map)
        }
        ParsedFields::Error(msg) => Err(RecordParserError(
            ParserError::FlowLogFormatError,
            msg.clone(),
        )),
    }
}

/// Parse EC2 Flow Log fields from a string
/// Fields are space-separated
fn parse_vpclog_fields(input: &str) -> Vec<String> {
    input.split_whitespace().map(|s| s.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flowlogs::parse_log_format;
    use crate::parse::cwlogs::{LogPlatform, ParserType};
    use crate::parse::record_parser::RecordParser;
    use aws_lambda_events::cloudwatch_logs::LogEntry;
    use opentelemetry_proto::tonic::{common::v1::any_value::Value, logs::v1::LogRecord};

    const DEFAULT_FORMAT: &'static str = "${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}";

    /// Test utility: Create a LogEntry from a message string
    fn create_log_entry(message: &str) -> LogEntry {
        let mut log_entry = LogEntry::default();
        log_entry.id = "test-id".to_string();
        log_entry.timestamp = 1000;
        log_entry.message = message.to_string();
        log_entry
    }

    /// Test utility: Parse a log message and return the LogRecord
    fn parse_log_msg(message: &str) -> LogRecord {
        let log_entry = create_log_entry(message);
        let dflt_fields = parse_log_format(DEFAULT_FORMAT);
        let parser = RecordParser::new(
            LogPlatform::Unknown,
            ParserType::VpcLog,
            Some(Arc::new(ParsedFields::Success(dflt_fields))),
        );
        parser.parse(123456789, log_entry.into())
    }

    fn parse_vpclog_to_map_default(
        input: String,
    ) -> Result<serde_json::Map<String, JsonValue>, RecordParserError> {
        let dflt_fields = parse_log_format(DEFAULT_FORMAT);
        parse_vpclog_to_map(input, Arc::new(ParsedFields::Success(dflt_fields)))
    }

    #[test]
    fn test_parse_vpclog_fields_basic() {
        let input = "2 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK";
        let fields = parse_vpclog_fields(input);

        assert_eq!(fields.len(), 14);
        assert_eq!(fields[0], "2");
        assert_eq!(fields[1], "123456789010");
        assert_eq!(fields[2], "eni-1235b8ca123456789");
        assert_eq!(fields[3], "172.31.16.139");
        assert_eq!(fields[12], "ACCEPT");
        assert_eq!(fields[13], "OK");
    }

    #[test]
    fn test_parse_vpclog_fields_with_dashes() {
        let input =
            "2 123456789010 eni-1235b8ca123456789 - - - - - - - 1431280876 1431280934 - NODATA";
        let fields = parse_vpclog_fields(input);

        assert_eq!(fields.len(), 14);
        assert_eq!(fields[3], "-");
        assert_eq!(fields[4], "-");
        assert_eq!(fields[12], "-");
        assert_eq!(fields[13], "NODATA");
    }

    #[test]
    fn test_parse_vpclog_to_map_basic_tcp() {
        let input = "2 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK";
        let result = parse_vpclog_to_map_default(input.to_string());

        assert!(result.is_ok());
        let map = result.unwrap();

        assert_eq!(map.len(), 14); // All fields present
        assert_eq!(
            map.get("version").unwrap(),
            &JsonValue::Number(serde_json::Number::from(2))
        );
        assert_eq!(
            map.get("account-id").unwrap(),
            &JsonValue::String("123456789010".to_string())
        );
        assert_eq!(
            map.get("interface-id").unwrap(),
            &JsonValue::String("eni-1235b8ca123456789".to_string())
        );
        assert_eq!(
            map.get("srcaddr").unwrap(),
            &JsonValue::String("172.31.16.139".to_string())
        );
        assert_eq!(
            map.get("dstaddr").unwrap(),
            &JsonValue::String("172.31.16.21".to_string())
        );
        assert_eq!(
            map.get("srcport").unwrap(),
            &JsonValue::Number(serde_json::Number::from(20641))
        );
        assert_eq!(
            map.get("dstport").unwrap(),
            &JsonValue::Number(serde_json::Number::from(22))
        );
        assert_eq!(
            map.get("protocol").unwrap(),
            &JsonValue::Number(serde_json::Number::from(6))
        );
        assert_eq!(
            map.get("packets").unwrap(),
            &JsonValue::Number(serde_json::Number::from(20))
        );
        assert_eq!(
            map.get("bytes").unwrap(),
            &JsonValue::Number(serde_json::Number::from(4249))
        );
        assert_eq!(
            map.get("start").unwrap(),
            &JsonValue::Number(serde_json::Number::from(1418530010i64))
        );
        assert_eq!(
            map.get("end").unwrap(),
            &JsonValue::Number(serde_json::Number::from(1418530070i64))
        );
        assert_eq!(
            map.get("action").unwrap(),
            &JsonValue::String("ACCEPT".to_string())
        );
        assert_eq!(
            map.get("log-status").unwrap(),
            &JsonValue::String("OK".to_string())
        );
    }

    #[test]
    fn test_parse_vpclog_to_map_with_format_custom() {
        let log_format = "${version} ${account-id} ${srcaddr} ${dstaddr}";
        let input = "2 123456789010 172.31.16.139 172.31.16.21";
        let parsed_fields = parse_log_format(log_format);
        let result = parse_vpclog_to_map(
            input.to_string(),
            Arc::new(ParsedFields::Success(parsed_fields)),
        );

        assert!(result.is_ok());
        let map = result.unwrap();

        assert_eq!(map.len(), 4);
        assert_eq!(
            map.get("version").unwrap(),
            &JsonValue::Number(serde_json::Number::from(2))
        );
        assert_eq!(
            map.get("account-id").unwrap(),
            &JsonValue::String("123456789010".to_string())
        );
        assert_eq!(
            map.get("srcaddr").unwrap(),
            &JsonValue::String("172.31.16.139".to_string())
        );
        assert_eq!(
            map.get("dstaddr").unwrap(),
            &JsonValue::String("172.31.16.21".to_string())
        );
    }

    #[test]
    fn test_parse_vpclog_to_map_with_format_excludes_dashes() {
        let log_format = "${version} ${account-id} ${srcaddr} ${dstaddr}";
        let input = "2 123456789010 - 172.31.16.21";
        let parsed_fields = parse_log_format(log_format);
        let result = parse_vpclog_to_map(
            input.to_string(),
            Arc::new(ParsedFields::Success(parsed_fields)),
        );

        assert!(result.is_ok());
        let map = result.unwrap();

        assert_eq!(map.len(), 3); // srcaddr should be excluded
        assert_eq!(
            map.get("version").unwrap(),
            &JsonValue::Number(serde_json::Number::from(2))
        );
        assert_eq!(
            map.get("account-id").unwrap(),
            &JsonValue::String("123456789010".to_string())
        );
        assert!(map.get("srcaddr").is_none()); // Should be excluded
        assert_eq!(
            map.get("dstaddr").unwrap(),
            &JsonValue::String("172.31.16.21".to_string())
        );
    }

    #[test]
    fn test_parse_vpclog_to_map_reject() {
        let input = "2 123456789010 eni-1235b8ca123456789 172.31.9.69 172.31.9.12 49761 3389 6 20 4249 1418530010 1418530070 REJECT OK";
        let result = parse_vpclog_to_map_default(input.to_string());

        assert!(result.is_ok());
        let map = result.unwrap();

        assert_eq!(
            map.get("action").unwrap(),
            &JsonValue::String("REJECT".to_string())
        );
        assert_eq!(
            map.get("srcport").unwrap(),
            &JsonValue::Number(serde_json::Number::from(49761))
        );
        assert_eq!(
            map.get("dstport").unwrap(),
            &JsonValue::Number(serde_json::Number::from(3389))
        );
    }

    #[test]
    fn test_parse_vpclog_to_map_with_dashes() {
        let input =
            "2 123456789010 eni-1235b8ca123456789 - - - - - - - 1431280876 1431280934 - NODATA";
        let result = parse_vpclog_to_map_default(input.to_string());

        assert!(result.is_ok());
        let map = result.unwrap();

        // Only non-dash fields should be present
        assert_eq!(map.len(), 6);
        assert_eq!(
            map.get("version").unwrap(),
            &JsonValue::Number(serde_json::Number::from(2))
        );
        assert_eq!(
            map.get("account-id").unwrap(),
            &JsonValue::String("123456789010".to_string())
        );
        assert_eq!(
            map.get("interface-id").unwrap(),
            &JsonValue::String("eni-1235b8ca123456789".to_string())
        );
        assert_eq!(
            map.get("start").unwrap(),
            &JsonValue::Number(serde_json::Number::from(1431280876i64))
        );
        assert_eq!(
            map.get("end").unwrap(),
            &JsonValue::Number(serde_json::Number::from(1431280934i64))
        );

        // Dash fields should not be present
        assert!(map.get("srcaddr").is_none());
        assert!(map.get("dstaddr").is_none());
        assert!(map.get("action").is_none());
        assert_eq!(
            map.get("log-status").unwrap(),
            &JsonValue::String("NODATA".to_string())
        );
    }

    #[test]
    fn test_parse_vpclog_to_map_icmp() {
        let input = "2 123456789010 eni-1235b8ca123456789 203.0.113.12 172.31.16.139 0 0 1 4 336 1432917027 1432917142 ACCEPT OK";
        let result = parse_vpclog_to_map_default(input.to_string());

        assert!(result.is_ok());
        let map = result.unwrap();

        assert_eq!(
            map.get("protocol").unwrap(),
            &JsonValue::Number(serde_json::Number::from(1))
        ); // ICMP
        assert_eq!(
            map.get("srcport").unwrap(),
            &JsonValue::Number(serde_json::Number::from(0))
        );
        assert_eq!(
            map.get("dstport").unwrap(),
            &JsonValue::Number(serde_json::Number::from(0))
        );
        assert_eq!(
            map.get("srcaddr").unwrap(),
            &JsonValue::String("203.0.113.12".to_string())
        );
    }

    #[test]
    fn test_parse_vpclog_to_map_ipv6() {
        let input = "2 123456789010 eni-1235b8ca123456789 2001:db8:1234:a100:8d6e:3477:df66:f105 2001:db8:1234:a102:3304:8879:34cf:4071 34892 22 6 54 8855 1477913708 1477913820 ACCEPT OK";
        let result = parse_vpclog_to_map_default(input.to_string());

        assert!(result.is_ok());
        let map = result.unwrap();

        assert_eq!(
            map.get("srcaddr").unwrap(),
            &JsonValue::String("2001:db8:1234:a100:8d6e:3477:df66:f105".to_string())
        );
        assert_eq!(
            map.get("dstaddr").unwrap(),
            &JsonValue::String("2001:db8:1234:a102:3304:8879:34cf:4071".to_string())
        );
        assert_eq!(
            map.get("srcport").unwrap(),
            &JsonValue::Number(serde_json::Number::from(34892))
        );
        assert_eq!(
            map.get("dstport").unwrap(),
            &JsonValue::Number(serde_json::Number::from(22))
        );
        assert_eq!(
            map.get("protocol").unwrap(),
            &JsonValue::Number(serde_json::Number::from(6))
        );
    }

    #[test]
    fn test_parse_vpclog_to_map_invalid_field_count() {
        let input = "2 123456789010 eni-1235b8ca123456789 172.31.16.139";
        let result = parse_vpclog_to_map_default(input.to_string());

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_vpclog_to_map_empty() {
        let input = "";
        let result = parse_vpclog_to_map_default(input.to_string());

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_log_record_basic_tcp() {
        let input = "2 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK";
        let log_record = parse_log_msg(input);

        // Check attributes exist
        let attrs: std::collections::HashMap<_, _> = log_record
            .attributes
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_ref()))
            .collect();

        assert!(attrs.contains_key("version"));
        assert!(attrs.contains_key("account-id"));
        assert!(attrs.contains_key("interface-id"));
        assert!(attrs.contains_key("srcaddr"));
        assert!(attrs.contains_key("dstaddr"));
        assert!(attrs.contains_key("srcport"));
        assert!(attrs.contains_key("dstport"));
        assert!(attrs.contains_key("protocol"));
        assert!(attrs.contains_key("packets"));
        assert!(attrs.contains_key("bytes"));
        assert!(attrs.contains_key("start"));
        assert!(attrs.contains_key("end"));
        assert!(attrs.contains_key("action"));
        assert!(attrs.contains_key("log-status"));

        // Verify specific values
        if let Some(Some(any_val)) = attrs.get("srcaddr") {
            if let Some(Value::StringValue(s)) = &any_val.value {
                assert_eq!(s, "172.31.16.139");
            }
        }

        if let Some(Some(any_val)) = attrs.get("action") {
            if let Some(Value::StringValue(s)) = &any_val.value {
                assert_eq!(s, "ACCEPT");
            }
        }

        if let Some(Some(any_val)) = attrs.get("protocol") {
            if let Some(Value::IntValue(i)) = &any_val.value {
                assert_eq!(*i, 6); // TCP
            }
        }
    }

    #[test]
    fn test_parse_log_record_reject() {
        let input = "2 123456789010 eni-1235b8ca123456789 172.31.9.69 172.31.9.12 49761 3389 6 20 4249 1418530010 1418530070 REJECT OK";
        let log_record = parse_log_msg(input);

        let attrs: std::collections::HashMap<_, _> = log_record
            .attributes
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_ref()))
            .collect();

        if let Some(Some(any_val)) = attrs.get("action") {
            if let Some(Value::StringValue(s)) = &any_val.value {
                assert_eq!(s, "REJECT");
            }
        }
    }

    #[test]
    fn test_parse_log_record_with_dashes() {
        let input =
            "2 123456789010 eni-1235b8ca123456789 - - - - - - - 1431280876 1431280934 - NODATA";
        let log_record = parse_log_msg(input);

        let attrs: std::collections::HashMap<_, _> = log_record
            .attributes
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_ref()))
            .collect();

        // Should have only non-dash fields plus cloudwatch.id
        assert!(attrs.contains_key("version"));
        assert!(attrs.contains_key("account-id"));
        assert!(attrs.contains_key("interface-id"));
        assert!(attrs.contains_key("start"));
        assert!(attrs.contains_key("end"));
        assert!(attrs.contains_key("log-status"));
        assert!(attrs.contains_key("cloudwatch.id"));

        // Dash fields should not be present
        assert!(!attrs.contains_key("srcaddr"));
        assert!(!attrs.contains_key("dstaddr"));
        assert!(!attrs.contains_key("srcport"));
        assert!(!attrs.contains_key("dstport"));
        assert!(!attrs.contains_key("protocol"));
        assert!(!attrs.contains_key("packets"));
        assert!(!attrs.contains_key("bytes"));
        assert!(!attrs.contains_key("action"));
    }

    #[test]
    fn test_parse_log_record_icmp() {
        let input = "2 123456789010 eni-1235b8ca123456789 203.0.113.12 172.31.16.139 0 0 1 4 336 1432917027 1432917142 ACCEPT OK";
        let log_record = parse_log_msg(input);

        let attrs: std::collections::HashMap<_, _> = log_record
            .attributes
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_ref()))
            .collect();

        if let Some(Some(any_val)) = attrs.get("protocol") {
            if let Some(Value::IntValue(i)) = &any_val.value {
                assert_eq!(*i, 1); // ICMP
            }
        }

        if let Some(Some(any_val)) = attrs.get("srcport") {
            if let Some(Value::IntValue(i)) = &any_val.value {
                assert_eq!(*i, 0);
            }
        }
    }

    #[test]
    fn test_parse_log_record_ipv6() {
        let input = "2 123456789010 eni-1235b8ca123456789 2001:db8:1234:a100:8d6e:3477:df66:f105 2001:db8:1234:a102:3304:8879:34cf:4071 34892 22 6 54 8855 1477913708 1477913820 ACCEPT OK";
        let log_record = parse_log_msg(input);

        let attrs: std::collections::HashMap<_, _> = log_record
            .attributes
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_ref()))
            .collect();

        if let Some(Some(any_val)) = attrs.get("srcaddr") {
            if let Some(Value::StringValue(s)) = &any_val.value {
                assert_eq!(s, "2001:db8:1234:a100:8d6e:3477:df66:f105");
            }
        }

        if let Some(Some(any_val)) = attrs.get("dstaddr") {
            if let Some(Value::StringValue(s)) = &any_val.value {
                assert_eq!(s, "2001:db8:1234:a102:3304:8879:34cf:4071");
            }
        }
    }

    #[test]
    fn test_parse_log_record_timestamps() {
        let input = "2 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK";
        let log_record = parse_log_msg(input);

        let attrs: std::collections::HashMap<_, _> = log_record
            .attributes
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.as_ref()))
            .collect();

        // Check that start and end timestamps are present (Int64)
        if let Some(Some(any_val)) = attrs.get("start") {
            if let Some(Value::IntValue(i)) = &any_val.value {
                assert_eq!(*i, 1418530010);
            }
        }

        if let Some(Some(any_val)) = attrs.get("end") {
            if let Some(Value::IntValue(i)) = &any_val.value {
                assert_eq!(*i, 1418530070);
            }
        }
    }

    #[test]
    fn test_all_example_logs() {
        // Test all examples from the user's request
        let examples = vec![
            "2 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK",
            "2 123456789010 eni-1235b8ca123456789 172.31.9.69 172.31.9.12 49761 3389 6 20 4249 1418530010 1418530070 REJECT OK",
            "2 123456789010 eni-1235b8ca123456789 - - - - - - - 1431280876 1431280934 - NODATA",
            "2 123456789010 eni-1235b8ca123456789 203.0.113.12 172.31.16.139 0 0 1 4 336 1432917027 1432917142 ACCEPT OK",
            "2 123456789010 eni-1235b8ca123456789 2001:db8:1234:a100:8d6e:3477:df66:f105 2001:db8:1234:a102:3304:8879:34cf:4071 34892 22 6 54 8855 1477913708 1477913820 ACCEPT OK",
        ];

        for example in examples {
            let result = parse_vpclog_to_map_default(example.to_string());
            assert!(result.is_ok(), "Failed to parse: {}", example);

            let log_record = parse_log_msg(example);
            assert!(
                !log_record.attributes.is_empty(),
                "No attributes parsed for: {}",
                example
            );
        }
    }

    #[test]
    fn test_parse_vpclog_to_map_invalid_int32() {
        // Test parsing a field that should be Int32 but contains non-integer data
        let log_format = "${version} ${account-id} ${srcport} ${dstport}";
        let input = "2 123456789010 invalid-port 22";
        let parsed_fields = parse_log_format(log_format);
        let result = parse_vpclog_to_map(
            input.to_string(),
            Arc::new(ParsedFields::Success(parsed_fields)),
        );

        // Should return an error because srcport (Int32) cannot parse "invalid-port"
        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{:?}", e);
            assert!(error_msg.contains("srcport"));
            assert!(error_msg.contains("integer"));
        }
    }

    #[test]
    fn test_parse_vpclog_to_map_invalid_int64() {
        // Test parsing an Int64 field with non-integer data
        let log_format = "${version} ${account-id} ${bytes} ${packets}";
        let input = "2 123456789010 not-a-number 100";
        let parsed_fields = parse_log_format(log_format);
        let result = parse_vpclog_to_map(
            input.to_string(),
            Arc::new(ParsedFields::Success(parsed_fields)),
        );

        // Should return an error because bytes (Int64) cannot parse "not-a-number"
        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{:?}", e);
            assert!(error_msg.contains("bytes"));
            assert!(error_msg.contains("integer"));
        }
    }
}
