use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

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

    pub fn is_cancelled(&self) -> bool {
        self.inner.flag.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        while !self.is_cancelled() {
            self.inner.notify.notified().await;
        }
    }
}
