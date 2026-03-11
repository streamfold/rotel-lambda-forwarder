use aws_sdk_ec2::{Client as Ec2Client, types::LogDestinationType};
use std::collections::HashMap;
use thiserror::Error;
use tracing::{debug, info, warn};

use super::cache::FlowLogConfig;

/// Errors that can occur during EC2 operations
#[derive(Debug, Error)]
pub enum Ec2Error {
    #[error("EC2 API error: {0}")]
    ApiError(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Invalid flow log format: {0}")]
    InvalidFormat(String),
}

/// The result of fetching all EC2 Flow Log configurations.
///
/// Flow logs are partitioned by their delivery destination type:
/// - `by_log_group`: keyed by CloudWatch log group name
/// - `by_bucket`:    keyed by S3 bucket name (extracted from the `log-destination` ARN).
///                   Multiple flow logs may share a bucket, differentiated by folder prefix.
#[derive(Debug, Default)]
pub struct FetchedFlowLogs {
    /// CloudWatch-delivered flow logs: log_group_name → config
    pub by_log_group: HashMap<String, FlowLogConfig>,
    /// S3-delivered flow logs: bucket_name → list of configs (one per distinct folder prefix)
    pub by_bucket: HashMap<String, Vec<FlowLogConfig>>,
}

/// Fetcher for EC2 Flow Log configurations from EC2 API
///
/// This fetcher queries the EC2 DescribeFlowLogs API to retrieve EC2 Flow Log
/// configurations, including the log format and tags. Tags are extracted and
/// will be applied to logs as resource attributes with the prefix
/// `ec2.flow-logs.tags.<key>`.
pub struct Ec2FlowLogFetcher {
    client: Ec2Client,
}

impl Ec2FlowLogFetcher {
    /// Create a new EC2 flow log fetcher
    pub fn new(client: Ec2Client) -> Self {
        Self { client }
    }

    /// Fetch all EC2 Flow Logs, partitioned by destination type.
    ///
    /// - CloudWatch Logs destinations are keyed by log group name.
    /// - S3 destinations are keyed by bucket name extracted from the destination ARN
    ///   (`arn:aws:s3:::bucket-name[/optional-prefix]`).
    ///
    /// Flow logs with missing or unrecognised destination information are skipped with a
    /// warning so that a single misconfigured flow log cannot block the others.
    pub async fn fetch_all_flow_logs(&self) -> Result<FetchedFlowLogs, Ec2Error> {
        debug!("Fetching VPC flow logs from EC2 API");

        let result = self.client.describe_flow_logs().send().await;

        let response = match result {
            Ok(resp) => resp,
            Err(e) => {
                let err_msg = format!("{:?}", e);
                if err_msg.contains("UnauthorizedOperation") || err_msg.contains("AccessDenied") {
                    return Err(Ec2Error::AccessDenied(err_msg));
                } else {
                    return Err(Ec2Error::ApiError(err_msg));
                }
            }
        };

        let mut fetched = FetchedFlowLogs::default();

        for flow_log in response.flow_logs() {
            let destination_type = match flow_log.log_destination_type() {
                Some(t) => t,
                None => {
                    debug!(
                        flow_log_id = ?flow_log.flow_log_id(),
                        "Skipping flow log with unset log_destination_type"
                    );
                    continue;
                }
            };

            let log_format = match flow_log.log_format().map(|s| s.to_string()) {
                Some(f) => f,
                None => {
                    warn!(
                        flow_log_id = ?flow_log.flow_log_id(),
                        "Flow log missing log format, skipping"
                    );
                    continue;
                }
            };

            let flow_log_id = flow_log.flow_log_id().unwrap_or("unknown").to_string();

            // Extract tags
            let mut tags = HashMap::new();
            for tag in flow_log.tags() {
                if let (Some(key), Some(value)) = (tag.key(), tag.value()) {
                    tags.insert(key.to_string(), value.to_string());
                }
            }

            let config = FlowLogConfig {
                log_format,
                flow_log_id: flow_log_id.clone(),
                tags,
                folder_prefix: None, // set below for S3 destinations
                parsed_fields: None,
            };

            match destination_type {
                LogDestinationType::CloudWatchLogs => {
                    let log_group_name = match flow_log.log_group_name() {
                        Some(name) => name,
                        None => {
                            warn!(
                                flow_log_id = %flow_log_id,
                                "CloudWatch flow log missing log group name, skipping"
                            );
                            continue;
                        }
                    };

                    debug!(
                        log_group = %log_group_name,
                        flow_log_id = %flow_log_id,
                        tag_count = config.tags.len(),
                        "Found CloudWatch EC2 flow log configuration"
                    );

                    fetched
                        .by_log_group
                        .insert(log_group_name.to_string(), config);
                }

                LogDestinationType::S3 => {
                    let destination_arn = match flow_log.log_destination() {
                        Some(arn) => arn,
                        None => {
                            warn!(
                                flow_log_id = %flow_log_id,
                                "S3 flow log missing log_destination ARN, skipping"
                            );
                            continue;
                        }
                    };

                    let (bucket_name, folder_prefix) = match extract_bucket_and_prefix_from_arn(
                        destination_arn,
                    ) {
                        Some(parts) => parts,
                        None => {
                            warn!(
                                flow_log_id = %flow_log_id,
                                destination_arn = %destination_arn,
                                "Unable to extract bucket name from S3 flow log destination ARN, skipping"
                            );
                            continue;
                        }
                    };

                    debug!(
                        bucket = %bucket_name,
                        folder_prefix = ?folder_prefix,
                        flow_log_id = %flow_log_id,
                        tag_count = config.tags.len(),
                        "Found S3 EC2 flow log configuration"
                    );

                    let s3_config = FlowLogConfig {
                        folder_prefix,
                        ..config
                    };
                    fetched
                        .by_bucket
                        .entry(bucket_name)
                        .or_default()
                        .push(s3_config);
                }

                other => {
                    debug!(
                        flow_log_id = %flow_log_id,
                        log_destination_type = other.to_string(),
                        "Skipping flow log with unsupported destination type"
                    );
                }
            }
        }

        let s3_config_count: usize = fetched.by_bucket.values().map(|v| v.len()).sum();
        info!(
            cloudwatch_count = fetched.by_log_group.len(),
            s3_bucket_count = fetched.by_bucket.len(),
            s3_config_count,
            "Fetched EC2 flow log configurations from EC2"
        );

        Ok(fetched)
    }
}

/// Extract the bucket name and optional folder prefix from an S3 ARN of the form
/// `arn:aws:s3:::bucket-name` or `arn:aws:s3:::bucket-name/optional/prefix`.
///
/// Returns `Some((bucket, folder_prefix))` where `folder_prefix` is `None` when no
/// prefix path is present in the ARN, or `Some(prefix)` otherwise.
/// Returns `None` if the ARN does not have the expected structure.
pub(crate) fn extract_bucket_and_prefix_from_arn(arn: &str) -> Option<(String, Option<String>)> {
    // Expected format: arn:aws:s3:::bucket-name[/prefix]
    // Split on ":::" and take everything after it.
    let resource = arn.splitn(2, ":::").nth(1)?;
    if resource.is_empty() {
        return None;
    }
    // The bucket name is the first path component; anything after the first '/' is the prefix.
    match resource.splitn(2, '/').collect::<Vec<_>>().as_slice() {
        [bucket] => {
            if bucket.is_empty() {
                None
            } else {
                Some((bucket.to_string(), None))
            }
        }
        [bucket, prefix] => {
            if bucket.is_empty() {
                None
            } else if prefix.is_empty() {
                Some((bucket.to_string(), None))
            } else {
                Some((bucket.to_string(), Some(prefix.to_string())))
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;

    #[tokio::test]
    async fn test_ec2_fetcher_creation() {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let ec2_client = Ec2Client::new(&config);

        let fetcher = Ec2FlowLogFetcher::new(ec2_client);
        // Just verify we can create the fetcher
        assert!(std::mem::size_of_val(&fetcher) > 0);
    }

    #[test]
    fn test_extract_bucket_and_prefix_from_arn_simple() {
        let arn = "arn:aws:s3:::my-flow-logs-bucket";
        assert_eq!(
            extract_bucket_and_prefix_from_arn(arn),
            Some(("my-flow-logs-bucket".to_string(), None))
        );
    }

    #[test]
    fn test_extract_bucket_and_prefix_from_arn_with_prefix() {
        let arn = "arn:aws:s3:::my-flow-logs-bucket/vpc-flow-logs/";
        assert_eq!(
            extract_bucket_and_prefix_from_arn(arn),
            Some((
                "my-flow-logs-bucket".to_string(),
                Some("vpc-flow-logs/".to_string())
            ))
        );
    }

    #[test]
    fn test_extract_bucket_and_prefix_from_arn_with_deep_prefix() {
        let arn = "arn:aws:s3:::my-bucket/AWSLogs/123456789012/vpcflowlogs/us-east-1/";
        assert_eq!(
            extract_bucket_and_prefix_from_arn(arn),
            Some((
                "my-bucket".to_string(),
                Some("AWSLogs/123456789012/vpcflowlogs/us-east-1/".to_string())
            ))
        );
    }

    #[test]
    fn test_extract_bucket_and_prefix_from_arn_trailing_slash_only() {
        // arn:aws:s3:::bucket/ — slash present but prefix is empty string
        let arn = "arn:aws:s3:::my-bucket/";
        assert_eq!(
            extract_bucket_and_prefix_from_arn(arn),
            Some(("my-bucket".to_string(), None))
        );
    }

    #[test]
    fn test_extract_bucket_and_prefix_from_arn_invalid() {
        assert_eq!(extract_bucket_and_prefix_from_arn("not-an-arn"), None);
        assert_eq!(extract_bucket_and_prefix_from_arn("arn:aws:s3:::"), None);
        assert_eq!(extract_bucket_and_prefix_from_arn(""), None);
    }
}
