use aws_lambda_events::cloudwatch_logs::LogsEvent;
use aws_lambda_events::s3::S3Event;
use lambda_runtime::Context;
use serde::{Deserialize, Serialize};

use crate::aws_attributes::AwsAttributes;

/// LambdaEvent is an enum that can deserialize multiple AWS event types
/// based on their characteristics. The deserialization will attempt to match
/// the incoming JSON structure to one of the supported event types.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum LambdaPayload {
    /// S3 event notifications
    S3Logs(S3Event),
    /// CloudWatch Logs event (AwsLogs)
    AwsLogs(LogsEvent),
}

pub struct LambdaEvent {
    pub payload: LambdaPayload,
    pub attributes: AwsAttributes,
    pub context: Context,
}

impl LambdaEvent {
    pub fn new(payload: LambdaPayload, attributes: AwsAttributes, context: Context) -> Self {
        Self {
            payload,
            attributes,
            context,
        }
    }
}
