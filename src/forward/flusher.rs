use std::{
    fmt::{self},
    time::{Duration, Instant},
};

use rotel::topology::flush_control::FlushSender;
use tokio::time::Instant as TokioInstant;
use tracing::debug;

pub struct Flusher {
    flush_logs_tx: FlushSender,
    flush_pipeline_tx: FlushSender,
    flush_exporters_tx: FlushSender,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlusherError {
    ConcurrentAccess,
    Timeout(String),
    Failure(String),
}

impl fmt::Display for FlusherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlusherError::ConcurrentAccess => write!(f, "concurrent access"),
            FlusherError::Timeout(msg) => write!(f, "timeout: {}", msg),
            FlusherError::Failure(msg) => write!(f, "failure: {}", msg),
        }
    }
}

impl std::error::Error for FlusherError {}

impl Flusher {
    pub fn new(
        flush_logs_tx: FlushSender,
        flush_pipeline_tx: FlushSender,
        flush_exporters_tx: FlushSender,
    ) -> Self {
        Flusher {
            flush_logs_tx,
            flush_pipeline_tx,
            flush_exporters_tx,
        }
    }

    pub async fn flush(&mut self, req_deadline: Instant) -> Result<(), FlusherError> {
        // Ensure we've flushed 100ms before function deadline
        let flush_deadline = req_deadline - Duration::from_millis(100);

        //
        // flush logs channel
        //
        let start = TokioInstant::now();
        match tokio::time::timeout_at(
            req_deadline.into(),
            self.flush_logs_tx.broadcast(Some(flush_deadline)),
        )
        .await
        {
            Err(_) => {
                return Err(FlusherError::Timeout("flush logs channel".to_string()));
            }
            Ok(Err(e)) => {
                return Err(FlusherError::Failure(format!(
                    "flushing logs channel: {}",
                    e
                )));
            }
            _ => {}
        }
        let duration = TokioInstant::now().duration_since(start);
        debug!(?duration, "finished flushing logs channel");

        //
        // flush pipeline
        //
        let start = TokioInstant::now();
        match tokio::time::timeout_at(
            req_deadline.into(),
            self.flush_pipeline_tx.broadcast(Some(flush_deadline)),
        )
        .await
        {
            Err(_) => {
                return Err(FlusherError::Timeout("flush pipeline".to_string()));
            }
            Ok(Err(e)) => return Err(FlusherError::Failure(format!("flushing pipeline: {}", e))),
            _ => {}
        }
        let duration = TokioInstant::now().duration_since(start);
        debug!(?duration, "finished flushing pipeline");

        //
        // flush exporters
        //
        let start = TokioInstant::now();
        match tokio::time::timeout_at(
            req_deadline.into(),
            self.flush_exporters_tx.broadcast(Some(flush_deadline)),
        )
        .await
        {
            Err(_) => {
                return Err(FlusherError::Timeout("flush exporters".to_string()));
            }
            Ok(Err(e)) => return Err(FlusherError::Failure(format!("flushing exporters: {}", e))),
            _ => {}
        }
        let duration = TokioInstant::now().duration_since(start);
        debug!(?duration, "finished flushing exporters");
        Ok(())
    }
}
