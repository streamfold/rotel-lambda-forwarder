use std::time::Instant;

use aws_lambda_events::cloudwatch_logs::LogsEvent;
use aws_lambda_events::s3::S3Event;
use aws_sdk_s3::Client as S3Client;
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use rotel::bounded_channel::BoundedSender;
use rotel::topology::payload::{Message, MessageMetadata};
use tracing::{debug, error};

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

        // Split into a counter (incremented per message) and a waiter (joined by the runtime).
        // The drain task is spawned inside into_parts(), so ack senders are never blocked.
        let (mut counter, waiter) = AckerBuilder::new(context.request_id.clone()).into_parts();
        let count = resource_logs.len();

        for log in resource_logs {
            let md = MessageMetadata::forwarder(counter.increment());
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

        // counter is dropped here (before the return value reaches the caller),
        // allowing the drain task to observe the channel closing once all
        // ForwarderMetadata senders have also been consumed downstream.
        Ok(waiter)
    }

    async fn handle_s3_logs(
        &mut self,
        s3_event: S3Event,
        aws_attributes: &AwsAttributes,
        context: &lambda_runtime::Context,
    ) -> Result<AckerWaiter, lambda_runtime::Error> {
        debug!(
            request_id = %context.request_id,
            records_count = s3_event.records.len(),
            "Handling S3 event notification"
        );

        // Ensure we have an S3 client
        let s3_client = self.s3_client.as_ref().ok_or_else(|| {
            lambda_runtime::Error::from("S3 client not initialized for S3 event processing")
        })?;

        let parser = s3logs::Parser::new(aws_attributes, &context.request_id, s3_client);

        // Create a channel so the parse task can stream batches of ResourceLogs to us as each
        // S3 object completes, rather than waiting for all objects before forwarding anything.
        // Buffer depth matches typical max-parallel-objects (default 5) with some headroom.
        let (result_tx, mut result_rx) = tokio::sync::mpsc::channel::<Vec<ResourceLogs>>(32);

        // Spawn the parse task so it runs concurrently with the forwarding loop below.
        let parse_handle = tokio::spawn(async move { parser.parse(s3_event, result_tx).await });

        // Split into counter + waiter. The drain task starts immediately inside into_parts(),
        // so ack senders are never blocked regardless of how many messages are in flight.
        let (mut counter, waiter) = AckerBuilder::new(context.request_id.clone()).into_parts();
        let mut count: usize = 0;

        // Forward each batch to logs_tx as it arrives — this runs concurrently with the parse
        // task, giving downstream batching / exporting a head-start before parsing is complete.
        while let Some(logs) = result_rx.recv().await {
            for log in logs {
                count += 1;
                let md = MessageMetadata::forwarder(counter.increment());
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
        }

        // The result_rx loop above ends only after result_tx is dropped, which happens when the
        // parse task exits. Join here to surface any parse errors.
        match parse_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(
                    request_id = %context.request_id,
                    error = %e,
                    "Failed to parse S3 logs"
                );
                return Err(lambda_runtime::Error::from(e.to_string()));
            }
            Err(e) => {
                error!(
                    request_id = %context.request_id,
                    error = %e,
                    "S3 parse task panicked"
                );
                return Err(lambda_runtime::Error::from(e.to_string()));
            }
        }

        debug!(
            request_id = %context.request_id,
            count = count,
            "Successfully parsed and sent S3 logs"
        );

        // counter is dropped here (before the return value reaches the caller),
        // allowing the drain task to observe the channel closing once all
        // ForwarderMetadata senders have also been consumed downstream.
        Ok(waiter)
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
