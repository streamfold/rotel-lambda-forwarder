use aws_sdk_ec2::Client as Ec2Client;
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

    /// Fetch all EC2 Flow Logs that are delivered to CloudWatch Logs
    ///
    /// Returns a map of log group name to flow log configuration.
    /// Only includes flow logs with destination type "cloud-watch-logs".
    ///
    pub async fn fetch_all_flow_logs(&self) -> Result<HashMap<String, FlowLogConfig>, Ec2Error> {
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

        let mut flow_log_configs = HashMap::new();

        // flow_logs() returns a slice, not an Option
        for flow_log in response.flow_logs() {
            // Only process CloudWatch Logs destinations
            let destination_type = flow_log
                .log_destination_type()
                .map(|t| t.as_str())
                .unwrap_or("");

            if destination_type != "cloud-watch-logs" {
                debug!(
                    flow_log_id = ?flow_log.flow_log_id(),
                    destination_type = %destination_type,
                    "Skipping flow log with non-CloudWatch destination"
                );
                continue;
            }

            // Get the log group name
            let log_group_name = match flow_log.log_group_name() {
                Some(name) => name,
                None => {
                    warn!(
                        flow_log_id = ?flow_log.flow_log_id(),
                        "Flow log missing log group name, skipping"
                    );
                    continue;
                }
            };

            let log_format = match flow_log.log_format().map(|s| s.to_string()) {
                Some(log_format) => log_format,
                None => {
                    warn!(
                        flow_log_id = ?flow_log.flow_log_id(),
                        "Flow log missing log format, skipping"
                    );

                    continue;
                }
            };

            let flow_log_id = flow_log.flow_log_id().unwrap_or("unknown").to_string();

            // Extract tags from the flow log
            // These tags will be applied to logs as resource attributes
            // with the prefix "ec2.flow-logs.tags.<key>"
            let mut tags = HashMap::new();
            for tag in flow_log.tags() {
                if let (Some(key), Some(value)) = (tag.key(), tag.value()) {
                    tags.insert(key.to_string(), value.to_string());
                }
            }

            let config = FlowLogConfig {
                log_format,
                destination_type: destination_type.to_string(),
                flow_log_id: flow_log_id.clone(),
                tags,
                parsed_fields: None,
            };

            debug!(
                log_group = %log_group_name,
                flow_log_id = %flow_log_id,
                tag_count = config.tags.len(),
                "Found EC2 flow log configuration with tags"
            );

            flow_log_configs.insert(log_group_name.to_string(), config);
        }

        info!(
            count = flow_log_configs.len(),
            "Fetched EC2 flow log configurations from EC2"
        );

        Ok(flow_log_configs)
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
}
