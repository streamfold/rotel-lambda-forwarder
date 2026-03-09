use regex::Regex;
use std::sync::LazyLock;
use std::{collections::HashMap, sync::Arc};

use aws_lambda_events::cloudwatch_logs::LogsEvent;
use opentelemetry_proto::tonic::{
    common::v1::InstrumentationScope,
    logs::v1::{ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use tracing::debug;

use crate::parse::platform::{LogPlatform, ParserError};
use crate::parse::utils::string_kv;
use crate::{
    aws_attributes::AwsAttributes,
    flowlogs::{FlowLogManager, ParsedFields},
    tags::TagManager,
};

pub mod record_parser;

use record_parser::RecordParser;

/// Parser handles the conversion of AWS CloudWatch Logs events into OpenTelemetry ResourceLogs
pub struct Parser<'a> {
    aws_attributes: &'a AwsAttributes,
    request_id: &'a String,
    tag_manager: &'a mut TagManager,
    flow_log_manager: &'a mut FlowLogManager,
}

impl<'a> Parser<'a> {
    pub fn new(
        aws_attributes: &'a AwsAttributes,
        request_id: &'a String,
        tag_manager: &'a mut TagManager,
        flow_log_manager: &'a mut FlowLogManager,
    ) -> Self {
        Self {
            aws_attributes,
            request_id,
            tag_manager,
            flow_log_manager,
        }
    }

    /// Parse an AWS CloudWatch Logs event and return ResourceLogs
    pub async fn parse(&mut self, logs_event: LogsEvent) -> Result<Vec<ResourceLogs>, ParserError> {
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
        let (log_platform, parser_type, flow_log_parsed_fields, flow_log_tags) = self
            .detect_log_type(&log_data.log_group, &log_data.log_stream)
            .await;

        // Fetch tags for the log group
        let log_group_tags = self
            .tag_manager
            .get_tags(
                &log_data.log_group,
                &self.aws_attributes.region,
                &self.aws_attributes.account_id,
            )
            .await
            .unwrap_or_else(|e| {
                debug!(
                    request_id = %self.request_id,
                    log_group = %log_data.log_group,
                    error = %e,
                    "Failed to fetch tags for log group"
                );
                std::collections::HashMap::new()
            });

        // Build base attributes
        let mut attributes = vec![
            string_kv("cloud.provider", "aws"),
            string_kv("cloud.region", self.aws_attributes.region.clone()),
            string_kv("cloud.account.id", self.aws_attributes.account_id.clone()),
            string_kv("cloudwatch.log.group.name", log_data.log_group),
            string_kv("cloudwatch.log.stream.name", log_data.log_stream),
        ];

        // Add cloud.platform attribute based on detected platform
        if log_platform != LogPlatform::Unknown {
            attributes.push(string_kv("cloud.platform", log_platform.as_str()));
        }

        // Add CloudWatch log group tags as resource attributes
        for (tag_key, tag_value) in log_group_tags {
            attributes.push(string_kv(
                &format!("cloudwatch.log.tags.{}", tag_key),
                tag_value,
            ));
        }

        // Add EC2 Flow Log tags as resource attributes
        for (tag_key, tag_value) in flow_log_tags {
            attributes.push(string_kv(
                &format!("ec2.flow-logs.tags.{}", tag_key),
                tag_value,
            ));
        }

        let rec_parser = RecordParser::new(log_platform, parser_type, flow_log_parsed_fields);
        let log_records = log_data
            .log_events
            .into_iter()
            .map(|log| rec_parser.parse(now_nanos, log))
            .collect();

        let resource_logs = ResourceLogs {
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
                        self.aws_attributes.invoked_function_arn.clone(),
                    )],
                    dropped_attributes_count: 0,
                }),
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        };

        resource_logs_list.push(resource_logs);

        Ok(resource_logs_list)
    }
}

/// Represents the type of parser to use for log entries
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParserType {
    Json,
    KeyValue,
    VpcLog,
    #[default]
    Unknown,
}

static CLOUDTRAIL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{12}_CloudTrail_").unwrap());

fn detect_log_platform(log_group_name: &str, log_stream_name: &str) -> LogPlatform {
    if CLOUDTRAIL_REGEX.is_match(log_stream_name) {
        return LogPlatform::Cloudtrail;
    }

    if let Some(rest) = log_group_name.strip_prefix("/aws/") {
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
    }
}

impl<'a> Parser<'a> {
    /// Detects the log platform and parser type based on log group and stream names.
    /// Returns (platform, parser_type, optional_flow_log_parsed_fields, flow_log_tags)
    async fn detect_log_type(
        &mut self,
        log_group_name: &str,
        log_stream_name: &str,
    ) -> (
        LogPlatform,
        ParserType,
        Option<Arc<ParsedFields>>,
        HashMap<String, String>,
    ) {
        // First check if this is an EC2 Flow Log
        if let Some(flow_log_config) = self.flow_log_manager.get_config(log_group_name).await {
            debug!(
                log_group = %log_group_name,
                flow_log_id = %flow_log_config.flow_log_id,
                "Detected EC2 Flow Log"
            );

            let parsed_fields = flow_log_config.parsed_fields.as_ref().cloned();

            return (
                LogPlatform::VpcFlowLog,
                ParserType::VpcLog,
                parsed_fields,
                flow_log_config.tags,
            );
        }

        // Otherwise, detect the platform normally
        let platform = detect_log_platform(log_group_name, log_stream_name);

        // Determine the parser type based on platform and log stream name
        let parser_type = match platform {
            LogPlatform::Eks => {
                if log_stream_name.starts_with("authenticator-") {
                    ParserType::KeyValue
                } else {
                    ParserType::Unknown
                }
            }
            LogPlatform::Cloudtrail => ParserType::Json,
            _ => ParserType::Unknown,
        };

        (platform, parser_type, None, HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;
    use aws_lambda_events::cloudwatch_logs::LogEntry;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;

    #[tokio::test]
    async fn test_parse_empty_event() {
        let logs_event = LogsEvent::default();
        let request_id = "test-request-id".to_string();
        let aws_attributes = AwsAttributes::default();

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let cw_client = aws_sdk_cloudwatchlogs::Client::new(&config);
        let mut tag_manager = crate::tags::TagManager::new(cw_client, None, None);

        let ec2_client = aws_sdk_ec2::Client::new(&config);
        let mut flow_log_manager = crate::flowlogs::FlowLogManager::new(ec2_client, None, None);

        let mut parser = Parser::new(
            &aws_attributes,
            &request_id,
            &mut tag_manager,
            &mut flow_log_manager,
        );
        let result = parser.parse(logs_event).await;

        assert!(result.is_ok());
        let resource_logs = result.unwrap();
        assert_eq!(resource_logs.len(), 1);
    }

    #[tokio::test]
    async fn test_parse_eks_authenticator_log() {
        let log_msg = r#"time="2025-12-24T19:48:32Z" level=info msg="access granted" arn="arn:aws:iam::927209226484:role/AWSWesleyClusterManagerLambda-Add-AddonManagerRole-1CRTQUJF13T5U" client="127.0.0.1:54812" groups="[]" method=POST path=/authenticate stsendpoint=sts.us-east-1.amazonaws.com uid="aws-iam-authenticator:927209226484:AROA5PYP2AD2FVXU23CA6" username="eks:addon-manager""#;

        let mut log_entry = LogEntry::default();
        log_entry.id = "test-id".to_string();
        log_entry.timestamp = 1000;
        log_entry.message = log_msg.to_string();

        let rec_parser = RecordParser::new(LogPlatform::Eks, ParserType::KeyValue, None);
        let log_record = rec_parser.parse(123456789, log_entry);

        assert_eq!(log_record.severity_number, 9); // Info
        assert_eq!(log_record.severity_text, "INFO");

        assert!(log_record.body.is_some());
        if let Some(body) = &log_record.body {
            if let Some(Value::StringValue(s)) = &body.value {
                assert_eq!(s, "access granted");
            }
        }

        assert!(log_record.time_unix_nano > 0);

        assert!(log_record.attributes.iter().any(|kv| kv.key == "arn"));
        assert!(log_record.attributes.iter().any(|kv| kv.key == "method"));
        assert!(log_record.attributes.iter().any(|kv| kv.key == "username"));
    }

    #[test]
    fn test_detect_log_platform_eks() {
        let platform = detect_log_platform("/aws/eks/cluster-name", "authenticator-12345");
        assert_eq!(platform, LogPlatform::Eks);
    }

    #[test]
    fn test_detect_log_platform_ecs() {
        let platform = detect_log_platform("/aws/ecs/cluster-name", "task/service");
        assert_eq!(platform, LogPlatform::Ecs);
    }

    #[test]
    fn test_detect_log_platform_rds() {
        let platform = detect_log_platform("/aws/rds/instance-name", "error");
        assert_eq!(platform, LogPlatform::Rds);
    }

    #[test]
    fn test_detect_log_platform_lambda() {
        let platform =
            detect_log_platform("/aws/lambda/function-name", "2024/01/01/[$LATEST]abc123");
        assert_eq!(platform, LogPlatform::Lambda);
    }

    #[test]
    fn test_detect_log_platform_no_match() {
        let platform = detect_log_platform("/aws/route53/function-name", "stream");
        assert_eq!(platform, LogPlatform::Unknown);

        let platform = detect_log_platform("/custom/log/group", "stream");
        assert_eq!(platform, LogPlatform::Unknown);

        let platform = detect_log_platform("no-prefix", "stream");
        assert_eq!(platform, LogPlatform::Unknown);
    }

    #[test]
    fn test_detect_log_platform_cloudtrail() {
        let platform =
            detect_log_platform("/aws/cloudtrail/logs", "123456789012_CloudTrail_us-east-1");
        assert_eq!(platform, LogPlatform::Cloudtrail);
    }

    #[test]
    fn test_detect_log_platform_codebuild() {
        let platform = detect_log_platform("/aws/codebuild/project-name", "stream");
        assert_eq!(platform, LogPlatform::Codebuild);
    }
}
