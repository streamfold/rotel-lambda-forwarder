use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::LogRecord,
};
use tracing::{debug, warn};

use crate::parse::cwlogs::LogPlatform;

use super::json::map_log_level;

/// Parse a key-value formatted log entry and populate a LogRecord
/// Format: key=value key="quoted value" key=value
pub fn parse_keyvalue_log_entry(_platform: LogPlatform, msg: String, lr: &mut LogRecord) {
    let pairs = parse_keyvalue_pairs(&msg);

    if pairs.is_empty() {
        // If parsing failed, treat as plain text
        lr.body = Some(AnyValue {
            value: Some(Value::StringValue(msg)),
        });
        return;
    }

    // Process special keys and collect remaining as attributes
    let mut body_set = false;
    let mut level_set = false;
    let mut time_set = false;

    for (key, value) in pairs {
        match key.as_str() {
            "msg" | "message" => {
                if !body_set {
                    lr.body = Some(AnyValue {
                        value: Some(Value::StringValue(value)),
                    });
                    body_set = true;
                }
            }
            "level" => {
                if !level_set {
                    if let Some((severity_number, severity_text)) = map_log_level(&value) {
                        lr.severity_number = severity_number as i32;
                        lr.severity_text = severity_text;
                        level_set = true;
                    } else {
                        // Add as an attriute
                        lr.attributes.push(KeyValue {
                            key,
                            value: Some(AnyValue {
                                value: Some(Value::StringValue(value)),
                            }),
                        });
                    }
                }
            }
            "time" | "timestamp" => {
                if !time_set && let Some(parsed_nanos) = parse_rfc3339_timestamp(&value) {
                    lr.time_unix_nano = parsed_nanos;
                    time_set = true;
                }
            }
            _ => {
                // Add as attribute
                lr.attributes.push(KeyValue {
                    key,
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(value)),
                    }),
                });
            }
        }
    }
}

/// Parse key-value pairs from a string
/// Supports: key=value and key="quoted value"
fn parse_keyvalue_pairs(input: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut chars = input.chars().peekable();

    while chars.peek().is_some() {
        // Skip whitespace
        while chars.peek() == Some(&' ') || chars.peek() == Some(&'\t') {
            chars.next();
        }

        if chars.peek().is_none() {
            break;
        }

        // Parse key
        let mut key = String::new();
        while let Some(&ch) = chars.peek() {
            if ch == '=' {
                break;
            }
            if ch == ' ' || ch == '\t' {
                break;
            }
            key.push(ch);
            chars.next();
        }

        if key.is_empty() {
            break;
        }

        // Skip whitespace before =
        while chars.peek() == Some(&' ') || chars.peek() == Some(&'\t') {
            chars.next();
        }

        // Expect '='
        if chars.peek() != Some(&'=') {
            debug!("Expected '=' after key '{}', skipping", key);
            // Skip to next space to try to recover
            while let Some(&ch) = chars.peek() {
                chars.next();
                if ch == ' ' || ch == '\t' {
                    break;
                }
            }
            continue;
        }
        chars.next(); // consume '='

        // Skip whitespace after =
        while chars.peek() == Some(&' ') || chars.peek() == Some(&'\t') {
            chars.next();
        }

        // Parse value
        let value = if chars.peek() == Some(&'"') {
            // Quoted value
            chars.next(); // consume opening quote
            let mut val = String::new();
            let mut escaped = false;

            for ch in chars.by_ref() {
                if escaped {
                    // Handle escaped characters
                    match ch {
                        'n' => val.push('\n'),
                        't' => val.push('\t'),
                        'r' => val.push('\r'),
                        '\\' => val.push('\\'),
                        '"' => val.push('"'),
                        _ => {
                            val.push('\\');
                            val.push(ch);
                        }
                    }
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    // End of quoted value
                    break;
                } else {
                    val.push(ch);
                }
            }
            val
        } else {
            // Unquoted value - read until space or end
            let mut val = String::new();
            while let Some(&ch) = chars.peek() {
                if ch == ' ' || ch == '\t' {
                    break;
                }
                val.push(ch);
                chars.next();
            }
            val
        };

        if !key.is_empty() {
            pairs.push((key, value));
        }
    }

    pairs
}

/// Parse RFC3339 timestamp to nanoseconds since epoch
fn parse_rfc3339_timestamp(value: &str) -> Option<u64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
        Some(dt.timestamp_nanos_opt().unwrap_or(0) as u64)
    } else {
        warn!("Failed to parse timestamp as RFC3339: {}", value);
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::logs::v1::SeverityNumber;

    #[test]
    fn test_parse_keyvalue_pairs_plain_text() {
        let input = "This is just plain text without any structured format";
        let pairs = parse_keyvalue_pairs(input);

        // Should return empty when no valid key=value pairs found
        assert_eq!(pairs.len(), 0);
    }

    #[test]
    fn test_parse_keyvalue_pairs_simple() {
        let input = "key1=value1 key2=value2 key3=value3";
        let pairs = parse_keyvalue_pairs(input);

        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("key1".to_string(), "value1".to_string()));
        assert_eq!(pairs[1], ("key2".to_string(), "value2".to_string()));
        assert_eq!(pairs[2], ("key3".to_string(), "value3".to_string()));
    }

    #[test]
    fn test_parse_keyvalue_pairs_quoted() {
        let input = r#"key1="quoted value" key2=simple key3="another quoted""#;
        let pairs = parse_keyvalue_pairs(input);

        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("key1".to_string(), "quoted value".to_string()));
        assert_eq!(pairs[1], ("key2".to_string(), "simple".to_string()));
        assert_eq!(pairs[2], ("key3".to_string(), "another quoted".to_string()));
    }

    #[test]
    fn test_parse_keyvalue_pairs_escaped_quotes() {
        let input = r#"key1="value with \"quotes\"" key2=normal"#;
        let pairs = parse_keyvalue_pairs(input);

        assert_eq!(pairs.len(), 2);
        assert_eq!(
            pairs[0],
            ("key1".to_string(), r#"value with "quotes""#.to_string())
        );
        assert_eq!(pairs[1], ("key2".to_string(), "normal".to_string()));
    }

    #[test]
    fn test_parse_keyvalue_pairs_empty_brackets() {
        let input = r#"groups="[]" method=POST"#;
        let pairs = parse_keyvalue_pairs(input);

        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("groups".to_string(), "[]".to_string()));
        assert_eq!(pairs[1], ("method".to_string(), "POST".to_string()));
    }

    #[test]
    fn test_parse_keyvalue_authenticator_log() {
        let input = r#"time="2025-12-24T19:48:32Z" level=info msg="access granted" arn="arn:aws:iam::927209226484:role/AWSWesleyClusterManagerLambda-Add-AddonManagerRole-1CRTQUJF13T5U" client="127.0.0.1:54812" groups="[]" method=POST path=/authenticate stsendpoint=sts.us-east-1.amazonaws.com uid="aws-iam-authenticator:927209226484:AROA5PYP2AD2FVXU23CA6" username="eks:addon-manager""#;
        let pairs = parse_keyvalue_pairs(input);

        assert!(pairs.len() >= 10);

        // Check specific keys
        let time_pair = pairs.iter().find(|(k, _)| k == "time");
        assert!(time_pair.is_some());
        assert_eq!(time_pair.unwrap().1, "2025-12-24T19:48:32Z");

        let level_pair = pairs.iter().find(|(k, _)| k == "level");
        assert!(level_pair.is_some());
        assert_eq!(level_pair.unwrap().1, "info");

        let msg_pair = pairs.iter().find(|(k, _)| k == "msg");
        assert!(msg_pair.is_some());
        assert_eq!(msg_pair.unwrap().1, "access granted");
    }

    #[test]
    fn test_parse_keyvalue_sts_response_log() {
        let input = r#"time="2025-12-24T19:52:38Z" level=info msg="STS response" accesskeyid=ASIA5PYP2AD2ANOSYXTN accountid=927209226484 arn="arn:aws:sts::927209226484:assumed-role/AWSWesleyClusterManagerLambda-NodeManagerRole-1W15HHFYBJTFL/EKSAuthTokenSession" client="127.0.0.1:38940" method=POST path=/authenticate session=EKSAuthTokenSession stsendpoint=sts.us-east-1.amazonaws.com userid=AROA5PYP2AD2O47IG7477"#;
        let pairs = parse_keyvalue_pairs(input);

        assert!(pairs.len() >= 11);

        let msg_pair = pairs.iter().find(|(k, _)| k == "msg");
        assert!(msg_pair.is_some());
        assert_eq!(msg_pair.unwrap().1, "STS response");
    }

    #[test]
    fn test_parse_keyvalue_log_entry_with_level() {
        let input = r#"time="2025-12-24T19:48:32Z" level=info msg="test message" user=john"#;
        let mut log_record = LogRecord::default();

        parse_keyvalue_log_entry(LogPlatform::Unknown, input.to_string(), &mut log_record);

        assert_eq!(log_record.severity_number, SeverityNumber::Info as i32);
        assert_eq!(log_record.severity_text, "INFO");
        assert!(log_record.time_unix_nano > 0);

        // Check body
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "test message");
            } else {
                panic!("Body is not a string value");
            }
        }

        // Check attributes
        let user_attr = log_record.attributes.iter().find(|kv| kv.key == "user");
        assert!(user_attr.is_some());
    }

    #[test]
    fn test_parse_keyvalue_log_entry_warning_level() {
        let input = r#"level=warning msg="something went wrong""#;
        let mut log_record = LogRecord::default();

        parse_keyvalue_log_entry(LogPlatform::Unknown, input.to_string(), &mut log_record);

        assert_eq!(log_record.severity_number, SeverityNumber::Warn as i32);
        assert_eq!(log_record.severity_text, "WARN");
    }

    #[test]
    fn test_parse_keyvalue_log_entry_attributes() {
        let input = r#"msg="test" key1=value1 key2="value 2" key3=123"#;
        let mut log_record = LogRecord::default();

        parse_keyvalue_log_entry(LogPlatform::Unknown, input.to_string(), &mut log_record);

        assert_eq!(log_record.attributes.len(), 3);

        let key1 = log_record.attributes.iter().find(|kv| kv.key == "key1");
        let key2 = log_record.attributes.iter().find(|kv| kv.key == "key2");
        let key3 = log_record.attributes.iter().find(|kv| kv.key == "key3");

        assert!(key1.is_some());
        assert!(key2.is_some());
        assert!(key3.is_some());
    }

    #[test]
    fn test_parse_keyvalue_log_entry_timestamp() {
        let input = r#"time="2024-01-01T12:00:00Z" msg="test""#;
        let mut log_record = LogRecord::default();

        parse_keyvalue_log_entry(LogPlatform::Unknown, input.to_string(), &mut log_record);

        // Should have parsed the timestamp
        assert!(log_record.time_unix_nano > 0);
        // The timestamp for 2024-01-01T12:00:00Z in nanoseconds
        let expected = chrono::DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap() as u64;
        assert_eq!(log_record.time_unix_nano, expected);
    }

    #[test]
    fn test_parse_keyvalue_log_entry_plain_text_fallback() {
        let input = "This is just plain text without any structured format";
        let mut log_record = LogRecord::default();

        parse_keyvalue_log_entry(LogPlatform::Unknown, input.to_string(), &mut log_record);

        // Should treat as plain text
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "This is just plain text without any structured format");
            } else {
                panic!("Body is not a string value");
            }
        }
    }

    #[test]
    fn test_parse_keyvalue_log_entry_full_authenticator() {
        let input = r#"time="2025-12-24T19:48:32Z" level=info msg="access granted" arn="arn:aws:iam::927209226484:role/AWSWesleyClusterManagerLambda-Add-AddonManagerRole-1CRTQUJF13T5U" client="127.0.0.1:54812" groups="[]" method=POST path=/authenticate stsendpoint=sts.us-east-1.amazonaws.com uid="aws-iam-authenticator:927209226484:AROA5PYP2AD2FVXU23CA6" username="eks:addon-manager""#;
        let mut log_record = LogRecord::default();

        parse_keyvalue_log_entry(LogPlatform::Unknown, input.to_string(), &mut log_record);

        // Check severity
        assert_eq!(log_record.severity_number, SeverityNumber::Info as i32);
        assert_eq!(log_record.severity_text, "INFO");

        // Check timestamp
        assert!(log_record.time_unix_nano > 0);

        // Check body
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "access granted");
            }
        }

        // Check that we have attributes
        assert!(log_record.attributes.len() >= 7);

        // Check specific attributes
        let arn_attr = log_record.attributes.iter().find(|kv| kv.key == "arn");
        assert!(arn_attr.is_some());

        let method_attr = log_record.attributes.iter().find(|kv| kv.key == "method");
        assert!(method_attr.is_some());
        if let Some(method) = method_attr {
            if let Some(val) = &method.value {
                if let Some(Value::StringValue(s)) = &val.value {
                    assert_eq!(s, "POST");
                }
            }
        }
    }
}
