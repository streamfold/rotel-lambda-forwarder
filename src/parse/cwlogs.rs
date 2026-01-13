use aws_lambda_events::cloudwatch_logs::{LogEntry, LogsEvent};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use tracing::debug;

use super::json::parse_json_log_entry;
use super::keyvalue::parse_keyvalue_log_entry;
use crate::aws_attributes::AwsAttributes;

/// Parser handles the conversion of AWS CloudWatch Logs events into OpenTelemetry ResourceLogs
pub struct Parser<'a> {
    aws_attributes: &'a AwsAttributes,
    request_id: &'a String,
}

impl<'a> Parser<'a> {
    pub fn new(aws_attributes: &'a AwsAttributes, request_id: &'a String) -> Self {
        Self {
            aws_attributes,
            request_id,
        }
    }

    /// Parse an AWS CloudWatch Logs event and return ResourceLogs
    pub fn parse(&self, logs_event: LogsEvent) -> Result<Vec<ResourceLogs>, ParserError> {
        debug!(
            request_id = %self.request_id,
            "Starting to parse CloudWatch Logs event"
        );

        // Parse the CloudWatch Logs event into ResourceLogs
        let resource_logs = self.parse_logs_event(logs_event)?;

        debug!(
            request_id = %self.request_id,
            count = resource_logs.len(),
            "Successfully parsed CloudWatch Logs event into ResourceLogs"
        );

        Ok(resource_logs)
    }

    /// Internal method to parse the LogsEvent
    fn parse_logs_event(&self, logs_event: LogsEvent) -> Result<Vec<ResourceLogs>, ParserError> {
        let mut resource_logs_list = Vec::new();

        debug!(
            request_id = %self.request_id,
            "Parsing LogsEvent: {:?}",
            logs_event
        );

        let log_data = logs_event.aws_logs.data;

        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Detect platform and parser type
        let (log_platform, parser_type) =
            detect_log_type(&log_data.log_group, &log_data.log_stream);

        // Build base attributes
        let mut attributes = vec![
            KeyValue {
                key: "cloud.provider".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("aws".to_string())),
                }),
            },
            KeyValue {
                key: "cloud.region".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(self.aws_attributes.region.clone())),
                }),
            },
            KeyValue {
                key: "cloud.account.id".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(self.aws_attributes.account_id.clone())),
                }),
            },
            KeyValue {
                key: "cloudwatch.log.group.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(log_data.log_group)),
                }),
            },
            KeyValue {
                key: "cloudwatch.log.stream.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(log_data.log_stream)),
                }),
            },
        ];

        // Add cloud.platform attribute based on detected platform
        if log_platform != LogPlatform::Unknown {
            attributes.push(KeyValue {
                key: "cloud.platform".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(log_platform.as_str().to_string())),
                }),
            });
        }

        let log_records = log_data
            .log_events
            .into_iter()
            .map(|log| Parser::parse_log_entry(now_nanos, log, parser_type))
            .collect();

        let resource_logs = ResourceLogs {
            resource: Some(Resource {
                attributes,
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "rotel-lambda-forwarder".to_string(),
                    version: "0.0.1".to_string(), // TODO
                    attributes: vec![KeyValue {
                        key: "aws.lambda.invoked_arn".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(
                                self.aws_attributes.invoked_function_arn.clone(),
                            )),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                log_records,
                schema_url: "".to_string(),
            }],
            schema_url: String::new(),
        };

        resource_logs_list.push(resource_logs);

        Ok(resource_logs_list)
    }

    fn parse_log_entry(now_nanos: u64, log_entry: LogEntry, parser_type: ParserType) -> LogRecord {
        let mut lr = LogRecord {
            time_unix_nano: (log_entry.timestamp * 1_000_000) as u64,
            observed_time_unix_nano: now_nanos,
            attributes: vec![KeyValue {
                key: "id".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(log_entry.id)),
                }),
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        match parser_type {
            ParserType::Json => {
                parse_json_log_entry(log_entry.message, &mut lr);
            }
            ParserType::KeyValue => {
                parse_keyvalue_log_entry(log_entry.message, &mut lr);
            }
            ParserType::Unknown => {
                // Auto-detect: try JSON first, otherwise plain text
                if log_entry.message.len() > 2 && log_entry.message.starts_with("{") {
                    parse_json_log_entry(log_entry.message, &mut lr);
                } else {
                    lr.body = Some(AnyValue {
                        value: Some(Value::StringValue(log_entry.message)),
                    });
                }
            }
        }

        lr
    }
}

/// Detect AWS platform from log group name
/// Represents the AWS platform/service that generated the logs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogPlatform {
    Eks,
    Ecs,
    Rds,
    Lambda,
    Codebuild,
    Unknown,
}

impl LogPlatform {
    /// Returns the platform string used in cloud.platform attribute
    pub fn as_str(&self) -> &'static str {
        match self {
            LogPlatform::Eks => "aws_eks",
            LogPlatform::Ecs => "aws_ecs",
            LogPlatform::Rds => "aws_rds",
            LogPlatform::Lambda => "aws_lambda",
            LogPlatform::Codebuild => "aws_codebuild",
            LogPlatform::Unknown => "aws_unknown",
        }
    }
}

/// Represents the type of parser to use for log entries
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserType {
    Json,
    KeyValue,
    #[default]
    Unknown,
}

/// Detects the log platform and parser type based on log group and stream names
fn detect_log_type(log_group_name: &str, log_stream_name: &str) -> (LogPlatform, ParserType) {
    // First, detect the platform from the log group name
    let platform = if let Some(rest) = log_group_name.strip_prefix("/aws/") {
        if rest.starts_with("eks/") {
            LogPlatform::Eks
        } else if rest.starts_with("ecs/") {
            LogPlatform::Ecs
        } else if rest.starts_with("rds/") {
            LogPlatform::Rds
        } else if rest.starts_with("lambda/") {
            LogPlatform::Lambda
        } else if rest.starts_with("codebuild/") {
            LogPlatform::Codebuild
        } else {
            LogPlatform::Unknown
        }
    } else {
        LogPlatform::Unknown
    };

    // Then, determine the parser type based on platform and log stream name
    let parser_type = match platform {
        LogPlatform::Eks => {
            if log_stream_name.starts_with("authenticator-") {
                ParserType::KeyValue
            } else {
                ParserType::Unknown
            }
        }
        _ => ParserType::Unknown,
    };

    (platform, parser_type)
}

/// Errors that can occur during parsing
#[derive(Debug, thiserror::Error)]
pub enum ParserError {
    #[error("Failed to decode CloudWatch Logs data: {0}")]
    DecodeError(String),

    #[error("Failed to decompress CloudWatch Logs data: {0}")]
    DecompressionError(String),

    #[error("Failed to parse JSON: {0}")]
    JsonParseError(String),

    #[error("Invalid log format: {0}")]
    InvalidFormat(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_event() {
        let logs_event = LogsEvent::default();
        let request_id = "test-request-id".to_string();
        let aws_attributes = AwsAttributes::default();

        let parser = Parser::new(&aws_attributes, &request_id);
        let result = parser.parse(logs_event);

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        // Implementation creates one ResourceLogs structure
        assert_eq!(resource_logs.len(), 1);
    }

    #[test]
    fn test_parse_eks_authenticator_log() {
        let log_msg = r#"time="2025-12-24T19:48:32Z" level=info msg="access granted" arn="arn:aws:iam::927209226484:role/AWSWesleyClusterManagerLambda-Add-AddonManagerRole-1CRTQUJF13T5U" client="127.0.0.1:54812" groups="[]" method=POST path=/authenticate stsendpoint=sts.us-east-1.amazonaws.com uid="aws-iam-authenticator:927209226484:AROA5PYP2AD2FVXU23CA6" username="eks:addon-manager""#;

        // Test that EKS authenticator logs are detected as KeyValue parser type
        let (platform, parser_type) =
            detect_log_type("/aws/eks/cluster-name", "authenticator-12345");
        assert_eq!(platform, LogPlatform::Eks);
        assert_eq!(parser_type, ParserType::KeyValue);

        // Test parsing the log entry
        let mut log_entry = LogEntry::default();
        log_entry.id = "test-id".to_string();
        log_entry.timestamp = 1000;
        log_entry.message = log_msg.to_string();

        let log_record = Parser::parse_log_entry(123456789, log_entry, parser_type);

        // Verify the log was parsed correctly
        assert_eq!(log_record.severity_number, 9); // Info
        assert_eq!(log_record.severity_text, "INFO");

        // Verify body contains the msg field
        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "access granted");
            }
        }

        // Verify timestamp was parsed
        assert!(log_record.time_unix_nano > 0);

        // Verify attributes were extracted
        let has_arn = log_record.attributes.iter().any(|kv| kv.key == "arn");
        let has_method = log_record.attributes.iter().any(|kv| kv.key == "method");
        let has_username = log_record.attributes.iter().any(|kv| kv.key == "username");

        assert!(has_arn);
        assert!(has_method);
        assert!(has_username);
    }

    #[test]
    fn test_detect_log_type_eks() {
        let (platform, parser_type) = detect_log_type("/aws/eks/cluster-name", "application-pod");
        assert_eq!(platform, LogPlatform::Eks);
        assert_eq!(parser_type, ParserType::Unknown);
    }

    #[test]
    fn test_detect_log_type_eks_authenticator() {
        let (platform, parser_type) =
            detect_log_type("/aws/eks/cluster-name", "authenticator-12345");
        assert_eq!(platform, LogPlatform::Eks);
        assert_eq!(parser_type, ParserType::KeyValue);
    }

    #[test]
    fn test_detect_log_type_ecs() {
        let (platform, parser_type) = detect_log_type("/aws/ecs/cluster-name", "task/service");
        assert_eq!(platform, LogPlatform::Ecs);
        assert_eq!(parser_type, ParserType::Unknown);
    }

    #[test]
    fn test_detect_log_type_rds() {
        let (platform, parser_type) = detect_log_type("/aws/rds/instance-name", "error");
        assert_eq!(platform, LogPlatform::Rds);
        assert_eq!(parser_type, ParserType::Unknown);
    }

    #[test]
    fn test_detect_log_type_lambda() {
        let (platform, parser_type) =
            detect_log_type("/aws/lambda/function-name", "2024/01/01/[$LATEST]abc123");
        assert_eq!(platform, LogPlatform::Lambda);
        assert_eq!(parser_type, ParserType::Unknown);
    }

    #[test]
    fn test_detect_log_type_no_match() {
        let (platform, parser_type) = detect_log_type("/aws/route53/function-name", "stream");
        assert_eq!(platform, LogPlatform::Unknown);
        assert_eq!(parser_type, ParserType::Unknown);

        let (platform, parser_type) = detect_log_type("/custom/log/group", "stream");
        assert_eq!(platform, LogPlatform::Unknown);
        assert_eq!(parser_type, ParserType::Unknown);

        let (platform, parser_type) = detect_log_type("no-prefix", "stream");
        assert_eq!(platform, LogPlatform::Unknown);
        assert_eq!(parser_type, ParserType::Unknown);
    }
}
