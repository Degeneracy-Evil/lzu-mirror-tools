use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, Weak},
};

use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone, Default)]
pub struct MirrorLocks {
    entries: Arc<StdMutex<HashMap<String, Weak<Mutex<()>>>>>,
}

impl MirrorLocks {
    pub async fn acquire(&self, mirror: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut entries = self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            entries.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = entries.get(mirror).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                entries.insert(mirror.to_owned(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn same_mirror_serializes_while_different_mirrors_progress() {
        let locks = MirrorLocks::default();
        let first = locks.acquire("mirror-a").await;
        let entered_same = Arc::new(AtomicBool::new(false));
        let same_flag = entered_same.clone();
        let same_locks = locks.clone();
        let same = tokio::spawn(async move {
            let _guard = same_locks.acquire("mirror-a").await;
            same_flag.store(true, Ordering::SeqCst);
        });
        tokio::task::yield_now().await;
        assert!(!entered_same.load(Ordering::SeqCst));

        let _different = locks.acquire("mirror-b").await;
        drop(first);
        same.await.expect("same-mirror waiter");
        assert!(entered_same.load(Ordering::SeqCst));
    }
}
