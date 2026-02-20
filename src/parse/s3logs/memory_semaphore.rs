use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

/// A wrapper around Tokio's Semaphore that provides safe acquisition of permits
/// without panicking when the requested size exceeds the maximum capacity.
#[derive(Clone)]
pub struct MemorySemaphore {
    semaphore: Arc<Semaphore>,
    max_permits: usize,
}

impl MemorySemaphore {
    /// Creates a new MemorySemaphore with the specified maximum number of permits.
    ///
    /// # Arguments
    /// * `max_permits` - The maximum number of permits (typically bytes) available
    pub fn new(max_permits: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_permits)),
            max_permits,
        }
    }

    /// Attempts to acquire permits from the semaphore.
    ///
    /// If the requested size exceeds the maximum capacity, this method will:
    /// 1. Log a warning
    /// 2. Acquire the maximum number of permits instead to avoid panicking
    ///
    /// Returns `None` if the semaphore is closed or if the requested size is 0.
    ///
    /// # Arguments
    /// * `requested_size` - The number of permits to acquire
    /// * `request_id` - An identifier for logging purposes
    pub async fn acquire(
        &self,
        requested_size: usize,
        request_id: &str,
    ) -> Option<OwnedSemaphorePermit> {
        if requested_size == 0 {
            return None;
        }

        let permits_to_acquire = if requested_size > self.max_permits {
            warn!(
                request_id = %request_id,
                requested_size = requested_size,
                max_permits = self.max_permits,
                "Requested size exceeds memory semaphore capacity, acquiring maximum instead"
            );
            self.max_permits as u32
        } else {
            requested_size as u32
        };

        match self
            .semaphore
            .clone()
            .acquire_many_owned(permits_to_acquire)
            .await
        {
            Ok(permit) => Some(permit),
            Err(e) => {
                warn!(
                    request_id = %request_id,
                    error = %e,
                    requested_size = requested_size,
                    "Failed to acquire memory permit"
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_within_limit() {
        let sem = MemorySemaphore::new(1000);
        let permit = sem.acquire(500, "test-1").await;
        assert!(permit.is_some());
    }

    #[tokio::test]
    async fn test_acquire_at_limit() {
        let sem = MemorySemaphore::new(1000);
        let permit = sem.acquire(1000, "test-2").await;
        assert!(permit.is_some());
    }

    #[tokio::test]
    async fn test_acquire_exceeds_limit() {
        let sem = MemorySemaphore::new(1000);
        // Should not panic, should acquire max_permits instead
        let permit = sem.acquire(2000, "test-3").await;
        assert!(permit.is_some());
    }

    #[tokio::test]
    async fn test_acquire_zero() {
        let sem = MemorySemaphore::new(1000);
        let permit = sem.acquire(0, "test-4").await;
        assert!(permit.is_none());
    }

    #[tokio::test]
    async fn test_multiple_acquires() {
        let sem = MemorySemaphore::new(1000);
        let permit1 = sem.acquire(400, "test-5a").await;
        let permit2 = sem.acquire(400, "test-5b").await;
        assert!(permit1.is_some());
        assert!(permit2.is_some());

        // This should block in real scenario, but for this test we just verify it compiles
        drop(permit1);
        drop(permit2);
    }
}
