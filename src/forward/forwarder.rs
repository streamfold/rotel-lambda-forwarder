use std::time::Instant;

use aws_lambda_events::cloudwatch_logs::LogsEvent;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use rotel::bounded_channel::BoundedSender;
use rotel::topology::payload::{Message, MessageMetadata};
use tracing::{debug, error};

use crate::aws_attributes::AwsAttributes;
use crate::events::{LambdaEvent, LambdaPayload};
use crate::flowlogs::FlowLogManager;
use crate::forward::{AckerBuilder, AckerWaiter};
use crate::parse::cwlogs;
use crate::tags::TagManager;

pub struct Forwarder {
    logs_tx: BoundedSender<Message<ResourceLogs>>,
    tag_manager: TagManager,
    flow_log_manager: FlowLogManager,
}

impl Forwarder {
    pub fn new(
        logs_tx: BoundedSender<Message<ResourceLogs>>,
        tag_manager: TagManager,
        flow_log_manager: FlowLogManager,
    ) -> Self {
        Self {
            logs_tx,
            tag_manager,
            flow_log_manager,
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

        let mut forwarder = Forwarder::new(logs_tx, tag_manager, flow_log_manager);
        let logs_event = LogsEvent::default();
        let context = lambda_runtime::Context::default();
        let aws_attributes = AwsAttributes::new(&context);

        let result = forwarder
            .handle_aws_logs(logs_event, &aws_attributes, &context)
            .await;
        assert!(result.is_ok());
    }
}
