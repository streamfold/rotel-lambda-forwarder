use aws_sdk_cloudwatchlogs::Client as CloudWatchLogsClient;
use std::collections::HashMap;
use thiserror::Error;
use tracing::{debug, error};

/// Errors that can occur during CloudWatch operations
#[derive(Debug, Error)]
pub enum CloudWatchError {
    #[error("CloudWatch API error: {0}")]
    ApiError(String),

    #[error("Invalid ARN format: {0}")]
    InvalidArn(String),

    #[error("Resource not found: {0}")]
    ResourceNotFound(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),
}

/// CloudWatch Logs client for fetching log group tags
pub struct CloudWatchTagFetcher {
    client: CloudWatchLogsClient,
}

impl CloudWatchTagFetcher {
    /// Create a new CloudWatch tag fetcher
    pub fn new(client: CloudWatchLogsClient) -> Self {
        Self { client }
    }

    /// Fetch tags for a log group by calling ListTagsForResource
    pub async fn fetch_tags(
        &self,
        log_group_arn: &str,
    ) -> Result<HashMap<String, String>, CloudWatchError> {
        debug!(arn = %log_group_arn, "Fetching tags from CloudWatch");

        let result = self
            .client
            .list_tags_for_resource()
            .resource_arn(log_group_arn)
            .send()
            .await;

        match result {
            Ok(output) => {
                let tags = output.tags().map(|t| t.clone()).unwrap_or_default();

                debug!(
                    arn = %log_group_arn,
                    tag_count = tags.len(),
                    "Successfully fetched tags from CloudWatch"
                );

                Ok(tags)
            }
            Err(e) => {
                let err_msg = format!("{:?}", e);

                if err_msg.contains("ResourceNotFoundException") {
                    error!(arn = %log_group_arn, "Log group not found");
                    Err(CloudWatchError::ResourceNotFound(log_group_arn.to_string()))
                } else if err_msg.contains("AccessDeniedException") {
                    error!(arn = %log_group_arn, "Access denied performing ListTagsForResource");
                    Err(CloudWatchError::AccessDenied(log_group_arn.to_string()))
                } else {
                    error!(arn = %log_group_arn, error = %err_msg, "Failed to fetch tags");
                    Err(CloudWatchError::ApiError(err_msg))
                }
            }
        }
    }

    /// Build a CloudWatch Logs ARN from components
    pub fn build_log_group_arn(region: &str, account_id: &str, log_group_name: &str) -> String {
        format!(
            "arn:aws:logs:{}:{}:log-group:{}",
            region, account_id, log_group_name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_log_group_arn() {
        let arn = CloudWatchTagFetcher::build_log_group_arn(
            "us-east-1",
            "123456789012",
            "/aws/lambda/my-function",
        );
        assert_eq!(
            arn,
            "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/my-function"
        );
    }

    #[test]
    fn test_build_log_group_arn_with_special_chars() {
        let arn = CloudWatchTagFetcher::build_log_group_arn(
            "eu-west-1",
            "987654321098",
            "/aws/eks/cluster-name/cluster",
        );
        assert_eq!(
            arn,
            "arn:aws:logs:eu-west-1:987654321098:log-group:/aws/eks/cluster-name/cluster"
        );
    }
}
