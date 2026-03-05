use rotel::bounded_channel::{self, BoundedReceiver, BoundedSender};
use rotel::topology::payload::{ForwarderAcknowledgement, ForwarderMetadata};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::warn;

#[derive(Debug, Error)]
pub enum AckerError {
    #[error("Received Nack acknowledgement for request {request_id}")]
    Nack { request_id: String },

    #[error("Senders exited before full acknowledgement for request {request_id}")]
    Closed { request_id: String },
}

/// Builder for an [`AckerCounter`] / [`AckerWaiter`] pair. Call [`into_parts`](AckerBuilder::into_parts).
pub struct AckerBuilder {
    request_id: String,
    ref_count: Arc<AtomicUsize>,
    tx: BoundedSender<ForwarderAcknowledgement>,
    rx: BoundedReceiver<ForwarderAcknowledgement>,
}

impl AckerBuilder {
    /// Create a builder for `request_id`. The ack back-channel has a fixed capacity;
    /// the drain task spawned by [`into_parts`](AckerBuilder::into_parts) keeps it drained
    /// continuously so senders are never blocked.
    pub fn new(request_id: String) -> Self {
        let (tx, rx) = bounded_channel::bounded(64);
        Self {
            request_id,
            ref_count: Arc::new(AtomicUsize::new(0)),
            tx,
            rx,
        }
    }

    /// Split into a counter and a waiter, spawning the background drain task immediately.
    ///
    /// **Contract:** the [`AckerCounter`] holds one sender clone; the drain task only
    /// completes once the counter *and* all [`ForwarderMetadata`] handles returned by
    /// [`increment`](AckerCounter::increment) have been dropped. When both live in the same
    /// async scope, call `drop(counter)` before awaiting the waiter.
    pub fn into_parts(self) -> (AckerCounter, AckerWaiter) {
        let counter = AckerCounter {
            request_id: self.request_id.clone(),
            ref_count: self.ref_count.clone(),
            tx: self.tx,
        };
        let waiter = AckerWaiter::spawn(self.request_id, self.ref_count, self.rx);
        (counter, waiter)
    }
}

// ---------------------------------------------------------------------------
// AckerCounter
// ---------------------------------------------------------------------------

/// Sending side of the ack pair. Call [`increment`](AckerCounter::increment) once per
/// message sent downstream, then drop this handle when no more messages will be sent.
pub struct AckerCounter {
    request_id: String,
    ref_count: Arc<AtomicUsize>,
    tx: BoundedSender<ForwarderAcknowledgement>,
}

impl AckerCounter {
    /// Increment the outstanding ref count and return [`ForwarderMetadata`] to attach
    /// to the outgoing message. The downstream exporter acks or nacks through it when done.
    pub fn increment(&mut self) -> ForwarderMetadata {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        ForwarderMetadata::new(self.request_id.clone(), Some(self.tx.clone()))
    }
}

// ---------------------------------------------------------------------------
// AckerWaiter
// ---------------------------------------------------------------------------

/// Waiting side of the ack pair. The drain task was spawned inside [`AckerBuilder::into_parts`];
/// call [`wait`](AckerWaiter::wait) to join it.
pub struct AckerWaiter {
    request_id: String,
    handle: JoinHandle<Result<(), AckerError>>,
}

impl AckerWaiter {
    fn spawn(
        request_id: String,
        ref_count: Arc<AtomicUsize>,
        rx: BoundedReceiver<ForwarderAcknowledgement>,
    ) -> Self {
        let rid = request_id.clone();
        let handle = tokio::spawn(drain_acks(rid, ref_count, rx));
        Self { request_id, handle }
    }

    /// Join the drain task.
    ///
    /// - `Ok(())` — all messages acknowledged, ref count zero.
    /// - `Err(Nack)` — a downstream exporter nacked; the receiver is dropped immediately
    ///   so remaining senders unblock without deadlock.
    /// - `Err(Closed)` — all senders dropped before ref count reached zero.
    pub async fn wait(self) -> Result<(), AckerError> {
        match self.handle.await {
            Ok(result) => result,
            Err(_join_err) => Err(AckerError::Closed {
                request_id: self.request_id,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Background drain task
// ---------------------------------------------------------------------------

/// Drains the ack/nack channel until all sender clones are dropped.
/// Ack → decrement ref count. Nack → drop rx (unblocks senders) and return Err.
/// None → return Ok if ref count is zero, else Err(Closed).
async fn drain_acks(
    request_id: String,
    ref_count: Arc<AtomicUsize>,
    mut rx: BoundedReceiver<ForwarderAcknowledgement>,
) -> Result<(), AckerError> {
    loop {
        match rx.next().await {
            Some(ForwarderAcknowledgement::Ack(ack)) => {
                if ack.request_id != request_id {
                    warn!(
                        expected_request_id = %request_id,
                        got_request_id = %ack.request_id,
                        "Received ack for unexpected request id; ignoring"
                    );
                    continue;
                }
                ref_count.fetch_sub(1, Ordering::SeqCst);
            }

            Some(ForwarderAcknowledgement::Nack(nack)) => {
                if nack.request_id != request_id {
                    warn!(
                        expected_request_id = %request_id,
                        got_request_id = %nack.request_id,
                        "Received nack for unexpected request id; ignoring"
                    );
                    continue;
                }
                // Drop rx so remaining senders get an immediate send error.
                drop(rx);
                return Err(AckerError::Nack { request_id });
            }

            None => {
                if ref_count.load(Ordering::SeqCst) == 0 {
                    return Ok(());
                }
                return Err(AckerError::Closed { request_id });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use rotel::topology::payload::{Ack, ExporterError, MessageMetadata};
    use tokio::time::timeout;

    fn make_pair(request_id: &str) -> (AckerCounter, AckerWaiter) {
        AckerBuilder::new(request_id.to_string()).into_parts()
    }

    #[tokio::test]
    async fn test_acker_all_acks() {
        let (mut counter, waiter) = make_pair("test-request-1");

        let fm1 = MessageMetadata::forwarder(counter.increment());
        let fm2 = MessageMetadata::forwarder(counter.increment());
        let fm3 = MessageMetadata::forwarder(counter.increment());
        drop(counter);

        tokio::spawn(async move {
            fm1.ack().await.unwrap();
            fm2.ack().await.unwrap();
            fm3.ack().await.unwrap();
        });

        assert!(waiter.wait().await.is_ok());
    }

    #[tokio::test]
    async fn test_acker_with_nack() {
        let (mut counter, waiter) = make_pair("test-request-2");

        let fm1 = MessageMetadata::forwarder(counter.increment());
        let fm2 = MessageMetadata::forwarder(counter.increment());
        drop(counter);

        tokio::spawn(async move {
            fm1.ack().await.unwrap();
            // nack may fail if drain task already dropped rx — ignore.
            let _ = fm2.nack(ExporterError::Cancelled).await;
        });

        let result = waiter.wait().await;
        assert!(
            matches!(result, Err(AckerError::Nack { request_id }) if request_id == "test-request-2")
        );
    }

    #[tokio::test]
    async fn test_acker_missing_ack() {
        let (mut counter, waiter) = make_pair("test-request-3");

        let fm1 = MessageMetadata::forwarder(counter.increment());
        let fm2 = MessageMetadata::forwarder(counter.increment());
        let _fm3 = MessageMetadata::forwarder(counter.increment()); // never acked; kept alive
        drop(counter);

        tokio::spawn(async move {
            fm1.ack().await.unwrap();
            fm2.ack().await.unwrap();
        });

        // drain task waits for _fm3's sender clone; wait() must time out.
        let result = timeout(Duration::from_millis(50), waiter.wait()).await;
        assert!(result.is_err(), "expected timeout but wait returned early");
    }

    #[tokio::test]
    async fn test_acker_zero_refs() {
        let (counter, waiter) = make_pair("test-request-4");
        drop(counter); // channel closes immediately with ref_count == 0
        assert!(waiter.wait().await.is_ok());
    }

    #[tokio::test]
    async fn test_acker_nack_unblocks_remaining_senders() {
        let (mut counter, waiter) = make_pair("test-request-5");

        let fm1 = MessageMetadata::forwarder(counter.increment());
        let fm2 = MessageMetadata::forwarder(counter.increment());
        let fm3 = MessageMetadata::forwarder(counter.increment());
        drop(counter);

        let sender_task = tokio::spawn(async move {
            let _ = fm1.nack(ExporterError::Cancelled).await;
            // rx is dropped after nack; these should return errors promptly, not block.
            let _ = fm2.ack().await;
            let _ = fm3.ack().await;
        });

        let (wait_result, sender_result) = tokio::join!(waiter.wait(), sender_task);
        assert!(
            matches!(wait_result, Err(AckerError::Nack { .. })),
            "expected Nack, got {wait_result:?}"
        );
        assert!(sender_result.is_ok(), "sender task panicked");
    }
}
