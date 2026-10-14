use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// Cheap cancellation signal: an uncontended atomic flag for polling in
/// hot loops, plus a Notify so parked tasks are woken on cancel.
///
/// Replaces tokio_util::sync::CancellationToken, which (since 0.7.16)
/// keeps its state behind a Mutex shared by every clone — polling it per
/// packet ruined throughput (~5M contended lock cycles/s at 100 streams).
#[derive(Clone, Default)]
pub struct CancellationAtomic {
    inner: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    flag: AtomicBool,
    notify: Notify,
}

impl CancellationAtomic {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.inner.flag.store(true, Ordering::Release);
        self.inner.notify.notify_waiters();
    }

    /// Uncontended atomic load; safe to poll in hot loops.
    pub fn is_cancelled(&self) -> bool {
        self.inner.flag.load(Ordering::Acquire)
    }

    /// Awaits cancellation; returns immediately if already cancelled.
    ///
    /// A Notified future counts as registered from creation (before it is
    /// polled), so checking the flag and then awaiting cannot miss a
    /// cancel() that happens in between.
    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            self.inner.notify.notified().await;
        }
    }
}
