//! Async runtime utilities for DAOS.
//!
//! Available only with `async` feature; sync API is always the baseline.
//!
//! # Overview
//!
//! This module provides utilities for integrating DAOS operations with
//! async Rust runtimes. The primary entry point is [`spawn_blocking_daos`],
//! which wraps synchronous DAOS operations for use with Tokio.
//!
//! # Example
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let result = daoxide::r#async::spawn_blocking_daos(|| {
//!         // Perform sync DAOS operations here
//!         Ok(42)
//!     }).await?;
//!     
//!     println!("Result: {}", result);
//!     Ok(())
//! }
//! ```

use crate::error::{DaosError, Result};
use crate::unsafe_inner::ffi::{daos_eq_create, daos_eq_destroy};
use tokio::sync::oneshot;

/// Spawns a blocking DAOS operation on a thread pool.
///
/// This function wraps a synchronous DAOS operation in `spawn_blocking`,
/// allowing it to be used safely with Tokio's async runtime without
/// blocking the async executor thread.
///
/// # Example
///
/// ```ignore
/// let result = spawn_blocking_daos(|| {
///     // Sync DAOS code here
///     Ok(42)
/// }).await?;
/// ```
pub async fn spawn_blocking_daos<F, T>(op: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(op)
        .await
        .map_err(|_| DaosError::Internal("spawn_blocking task panicked".to_string()))?
}

/// Outcome of polling an event.
#[derive(Debug)]
pub enum PollOutcome {
    /// Event completed successfully.
    Completed,
    /// Event timed out.
    Timeout,
    /// Event failed with an error.
    Error(DaosError),
}

/// Status of a DAOS event.
#[derive(Debug)]
pub enum EventStatus {
    /// Event completed successfully.
    Completed,
    /// Event is still pending.
    Pending,
    /// Event failed with an error.
    Error(DaosError),
}

/// Event queue for asynchronous DAOS operations.
///
/// Currently a placeholder for future event queue support.
pub struct EventQueue {
    #[allow(dead_code)]
    handle: crate::unsafe_inner::handle::DaosHandle,
}

impl EventQueue {
    /// Creates a new event queue asynchronously.
    pub async fn new() -> Result<Self> {
        spawn_blocking_daos(|| {
            let handle = daos_eq_create()?;
            Ok(Self { handle })
        })
        .await
    }

    /// Creates a new event queue synchronously.
    pub fn new_sync() -> Result<Self> {
        let handle = daos_eq_create()?;
        Ok(Self { handle })
    }

    #[allow(dead_code)]
    pub(crate) fn as_raw_handle(&self) -> crate::unsafe_inner::handle::DaosHandle {
        self.handle
    }
}

impl Drop for EventQueue {
    fn drop(&mut self) {
        if let Err(e) = daos_eq_destroy(self.handle) {
            eprintln!(
                "EventQueue::drop: daos_eq_destroy() failed with {:?}, continuing with drop anyway",
                e
            );
        }
    }
}

/// Errors from async event operations.
#[derive(Debug)]
pub enum EventError {
    /// A DAOS error occurred.
    Daos(DaosError),
    /// The event was cancelled.
    Cancelled,
}

/// A future that resolves when an event completes.
#[must_use = "futures do nothing unless polled"]
pub struct EventFuture {
    inner: oneshot::Receiver<Result<()>>,
}

impl std::fmt::Debug for EventFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventFuture {{ .. }}")
    }
}

impl std::future::Future for EventFuture {
    type Output = Result<()>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.inner).poll(cx) {
            std::task::Poll::Ready(Ok(Ok(()))) => std::task::Poll::Ready(Ok(())),
            std::task::Poll::Ready(Ok(Err(e))) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Ready(Err(_)) => {
                std::task::Poll::Ready(Err(DaosError::Internal("event sender dropped".to_string())))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl EventQueue {
    /// Creates a oneshot channel for event completion.
    pub fn completion_channel<T>() -> (EventSender<T>, EventFutureGeneric<T>) {
        let (tx, rx) = oneshot::channel();
        (EventSender { tx }, EventFutureGeneric { inner: rx })
    }
}

/// Sender for event completion notifications.
pub struct EventSender<T> {
    tx: oneshot::Sender<Result<T>>,
}

impl<T> std::fmt::Debug for EventSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventSender {{ .. }}")
    }
}

impl<T> EventSender<T> {
    /// Sends the event result.
    #[inline]
    pub fn send(
        self,
        result: std::result::Result<T, DaosError>,
    ) -> std::result::Result<(), std::result::Result<T, DaosError>> {
        self.tx.send(result)
    }
}

/// A generic future for event results.
#[must_use = "futures do nothing unless polled"]
pub struct EventFutureGeneric<T> {
    inner: oneshot::Receiver<Result<T>>,
}

impl<T> std::fmt::Debug for EventFutureGeneric<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventFutureGeneric {{ .. }}")
    }
}

impl<T> std::future::Future for EventFutureGeneric<T> {
    type Output = Result<T>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.inner).poll(cx) {
            std::task::Poll::Ready(Ok(Ok(v))) => std::task::Poll::Ready(Ok(v)),
            std::task::Poll::Ready(Ok(Err(e))) => std::task::Poll::Ready(Err(e)),
            std::task::Poll::Ready(Err(_)) => {
                std::task::Poll::Ready(Err(DaosError::Internal("sender dropped".to_string())))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poll_outcome_debug() {
        let outcome = PollOutcome::Completed;
        assert_eq!(format!("{:?}", outcome), "Completed");
        let outcome = PollOutcome::Timeout;
        assert_eq!(format!("{:?}", outcome), "Timeout");
        let outcome = PollOutcome::Error(DaosError::NotFound);
        assert_eq!(format!("{:?}", outcome), "Error(NotFound)");
    }

    #[test]
    fn test_event_status_debug() {
        let status = EventStatus::Completed;
        assert_eq!(format!("{:?}", status), "Completed");
        let status = EventStatus::Pending;
        assert_eq!(format!("{:?}", status), "Pending");
        let status = EventStatus::Error(DaosError::Busy);
        assert_eq!(format!("{:?}", status), "Error(Busy)");
    }

    #[test]
    fn test_event_queue_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EventQueue>();
    }

    #[test]
    fn test_event_sender_debug() {
        let (tx, _rx) = oneshot::channel::<Result<()>>();
        let sender = EventSender { tx };
        assert_eq!(format!("{:?}", sender), "EventSender { .. }");
    }

    #[test]
    fn test_event_future_debug() {
        let (_tx, rx) = oneshot::channel::<Result<()>>();
        let future = EventFutureGeneric { inner: rx };
        assert_eq!(format!("{:?}", future), "EventFutureGeneric { .. }");
    }
}
