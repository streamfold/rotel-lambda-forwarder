use std::time::Instant;

use aws_lambda_events::cloudwatch_logs::LogsEvent;
use aws_lambda_events::s3::S3Event;
use aws_sdk_s3::Client as S3Client;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use rotel::bounded_channel::BoundedSender;
use rotel::topology::payload::{Message, MessageMetadata};
use tracing::{debug, error, info};

use crate::aws_attributes::AwsAttributes;
use crate::events::{LambdaEvent, LambdaPayload};
use crate::flowlogs::FlowLogManager;
use crate::forward::{AckerBuilder, AckerWaiter};
use crate::parse::cwlogs;
use crate::s3logs;
use crate::tags::TagManager;

pub struct Forwarder {
    logs_tx: BoundedSender<Message<ResourceLogs>>,
    tag_manager: TagManager,
    flow_log_manager: FlowLogManager,
    s3_client: Option<S3Client>,
}

impl Forwarder {
    pub fn new(
        logs_tx: BoundedSender<Message<ResourceLogs>>,
        tag_manager: TagManager,
        flow_log_manager: FlowLogManager,
        s3_client: Option<S3Client>,
    ) -> Self {
        Self {
            logs_tx,
            tag_manager,
            flow_log_manager,
            s3_client,
        }
    }
}

impl Forwarder {
    pub async fn handle_event(
        &mut self,
        _deadline: Instant,
        event: LambdaEvent,
    ) -> Result<AckerWaiter, lambda_runtime::Error> {
        match event.payload {
            LambdaPayload::S3Logs(s3_event) => {
                self.handle_s3_logs(s3_event, &event.attributes, &event.context)
                    .await
            }
            LambdaPayload::AwsLogs(logs_event) => {
                self.handle_aws_logs(logs_event, &event.attributes, &event.context)
                    .await
            }
        }
    }

    async fn handle_aws_logs(
        &mut self,
        logs_event: LogsEvent,
        aws_attributes: &AwsAttributes,
        context: &lambda_runtime::Context,
    ) -> Result<AckerWaiter, lambda_runtime::Error> {
        debug!(
            request_id = %context.request_id,
            "Handling CloudWatch Logs event"
        );

        let mut parser = cwlogs::Parser::new(
            aws_attributes,
            &context.request_id,
            &mut self.tag_manager,
            &mut self.flow_log_manager,
        );

        // Parse the logs
        let resource_logs = match parser.parse(logs_event).await {
            Ok(logs) => logs,
            Err(e) => {
                error!(
                    request_id = %context.request_id,
                    error = %e,
                    "Failed to parse logs"
                );
                return Err(lambda_runtime::Error::from(e.to_string()));
            }
        };

        // Send each ResourceLogs to the channel
        let count = resource_logs.len();
        let mut acker = AckerBuilder::new(context.request_id.clone(), count);

        for log in resource_logs {
            let md = MessageMetadata::forwarder(acker.increment());
            if let Err(e) = self
                .logs_tx
                .send(Message::new(Some(md), vec![log], None))
                .await
            {
                error!(
                    request_id = %context.request_id,
                    error = %e,
                    "Failed to send logs to channel"
                );
                return Err(lambda_runtime::Error::from(format!(
                    "Failed to send logs: {}",
                    e
                )));
            }
        }

        debug!(
            request_id = %context.request_id,
            count = count,
            "Successfully parsed and sent logs"
        );

        Ok(acker.finish())
    }

    async fn handle_s3_logs(
        &mut self,
        s3_event: S3Event,
        aws_attributes: &AwsAttributes,
        context: &lambda_runtime::Context,
    ) -> Result<AckerWaiter, lambda_runtime::Error> {
        info!(
            request_id = %context.request_id,
            records_count = s3_event.records.len(),
            "Handling S3 event notification"
        );

        // Ensure we have an S3 client
        let s3_client = self.s3_client.as_ref().ok_or_else(|| {
            lambda_runtime::Error::from("S3 client not initialized for S3 event processing")
        })?;

        let parser = s3logs::Parser::new(aws_attributes, &context.request_id, s3_client);

        // Parse the S3 event and load log files
        let resource_logs = match parser.parse(s3_event).await {
            Ok(logs) => logs,
            Err(e) => {
                error!(
                    request_id = %context.request_id,
                    error = %e,
                    "Failed to parse S3 logs"
                );
                return Err(lambda_runtime::Error::from(e.to_string()));
            }
        };

        // Send each ResourceLogs to the channel
        let count = resource_logs.len();
        let mut acker = AckerBuilder::new(context.request_id.clone(), count);

        for log in resource_logs {
            let md = MessageMetadata::forwarder(acker.increment());
            if let Err(e) = self
                .logs_tx
                .send(Message::new(Some(md), vec![log], None))
                .await
            {
                error!(
                    request_id = %context.request_id,
                    error = %e,
                    "Failed to send logs to channel"
                );
                return Err(lambda_runtime::Error::from(format!(
                    "Failed to send logs: {}",
                    e
                )));
            }
        }

        info!(
            request_id = %context.request_id,
            count = count,
            "Successfully parsed and sent S3 logs"
        );

        Ok(acker.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;

    #[tokio::test]
    async fn test_handle_aws_logs() {
        let (logs_tx, _logs_rx) = rotel::bounded_channel::bounded(10);

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let cw_client = aws_sdk_cloudwatchlogs::Client::new(&config);
        let tag_manager = TagManager::new(cw_client, None, None);

        let ec2_client = aws_sdk_ec2::Client::new(&config);
        let flow_log_manager = FlowLogManager::new(ec2_client, None, None);

        let s3_client = aws_sdk_s3::Client::new(&config);

        let mut forwarder = Forwarder::new(logs_tx, tag_manager, flow_log_manager, Some(s3_client));
        let logs_event = LogsEvent::default();
        let context = lambda_runtime::Context::default();
        let aws_attributes = AwsAttributes::new(&context);

        let result = forwarder
            .handle_aws_logs(logs_event, &aws_attributes, &context)
            .await;
        assert!(result.is_ok());
    }
}
