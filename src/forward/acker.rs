use rotel::bounded_channel::{self, BoundedReceiver, BoundedSender};
use rotel::topology::payload::{ForwarderAcknowledgement, ForwarderMetadata};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error)]
pub enum AckerError {
    #[error("Received Nack acknowledgement for request {request_id}")]
    Nack { request_id: String },

    #[error("Senders exited before full acknowledgement for request {request_id}")]
    Closed { request_id: String },
}

/// Builder for creating an Acker instance
pub struct AckerBuilder {
    request_id: String,
    ref_count: Arc<AtomicUsize>,
    tx: BoundedSender<ForwarderAcknowledgement>,
    rx: BoundedReceiver<ForwarderAcknowledgement>,
}

impl AckerBuilder {
    /// Create a new AckerBuilder with the given request ID
    pub fn new(request_id: String, len: usize) -> Self {
        let (tx, rx) = bounded_channel::bounded(len + 1); // Add 1 just in case?
        Self {
            request_id,
            ref_count: Arc::new(AtomicUsize::new(0)),
            tx,
            rx,
        }
    }

    /// Increment the reference count and return a ForwarderMetadata with the sender
    pub fn increment(&mut self) -> ForwarderMetadata {
        self.ref_count.fetch_add(1, Ordering::SeqCst);

        ForwarderMetadata::new(self.request_id.clone(), Some(self.tx.clone()))
    }

    /// Finish building and return an AckerWaiter
    pub fn finish(self) -> AckerWaiter {
        AckerWaiter {
            request_id: self.request_id,
            ref_count: self.ref_count,
            rx: self.rx,
        }
    }
}

/// Handle for waiting on acknowledgements
pub struct AckerWaiter {
    request_id: String,
    ref_count: Arc<AtomicUsize>,
    rx: BoundedReceiver<ForwarderAcknowledgement>,
}

impl AckerWaiter {
    /// Wait for all acknowledgements to be received
    /// Returns Ok(()) when all refs are acknowledged with Ack
    /// Returns Err if any Nack is received
    pub async fn wait(mut self) -> Result<(), AckerError> {
        loop {
            if self.ref_count.load(Ordering::SeqCst) == 0 {
                return Ok(());
            }

            match self.rx.next().await {
                Some(ForwarderAcknowledgement::Ack(ack)) => {
                    // Should not happen?
                    if ack.request_id != self.request_id {
                        warn!(
                            expected_request_id = self.request_id,
                            got_request_id = ack.request_id,
                            "Received ack for invalid request id"
                        );
                        continue;
                    }

                    self.ref_count.fetch_sub(1, Ordering::SeqCst);
                }
                Some(ForwarderAcknowledgement::Nack(nack)) => {
                    // Should not happen?
                    if nack.request_id != self.request_id {
                        warn!(
                            expected_request_id = self.request_id,
                            got_request_id = nack.request_id,
                            "Received nack for invalid request id"
                        );
                        continue;
                    }

                    return Err(AckerError::Nack {
                        request_id: self.request_id,
                    });
                }
                None => {
                    // Channel closed - this shouldn't happen if senders are properly held
                    // but we'll check the ref count one more time
                    if self.ref_count.load(Ordering::SeqCst) == 0 {
                        return Ok(());
                    }

                    // If ref count is not zero but channel is closed, we have an issue
                    return Err(AckerError::Closed {
                        request_id: self.request_id,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use rotel::topology::payload::{Ack, ExporterError, MessageMetadata};
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_acker_all_acks() {
        let mut builder = AckerBuilder::new("test-request-1".to_string(), 3);

        let fm1 = MessageMetadata::forwarder(builder.increment());
        let fm2 = MessageMetadata::forwarder(builder.increment());
        let fm3 = MessageMetadata::forwarder(builder.increment());

        let waiter = builder.finish();

        // Spawn a task to send acks
        tokio::spawn(async move {
            fm1.ack().await.unwrap();
            fm2.ack().await.unwrap();
            fm3.ack().await.unwrap();
        });

        let result = waiter.wait().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_acker_with_nack() {
        let mut builder = AckerBuilder::new("test-request-2".to_string(), 2);

        let fm1 = MessageMetadata::forwarder(builder.increment());
        let fm2 = MessageMetadata::forwarder(builder.increment());

        let waiter = builder.finish();

        // Spawn a task to send ack then nack
        tokio::spawn(async move {
            fm1.ack().await.unwrap();
            fm2.nack(ExporterError::Cancelled).await.unwrap();
        });

        let result = waiter.wait().await;
        assert!(result.is_err());
        match result {
            Err(AckerError::Nack { request_id }) => {
                assert_eq!(request_id, "test-request-2");
            }
            _ => panic!("Expected Nack error"),
        }
    }

    #[tokio::test]
    async fn test_acker_missing_ack() {
        let mut builder = AckerBuilder::new("test-request-1".to_string(), 3);

        let fm1 = MessageMetadata::forwarder(builder.increment());
        let fm2 = MessageMetadata::forwarder(builder.increment());
        let _fm3 = MessageMetadata::forwarder(builder.increment());

        let waiter = builder.finish();

        // Spawn a task to send acks
        tokio::spawn(async move {
            fm1.ack().await.unwrap();
            fm2.ack().await.unwrap();
        });

        let result = timeout(Duration::from_millis(50), waiter.wait()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_acker_zero_refs() {
        let builder = AckerBuilder::new("test-request-3".to_string(), 2);
        let waiter = builder.finish();

        // Should return immediately with Ok since ref count is 0
        let result = waiter.wait().await;
        assert!(result.is_ok());
    }
}
