use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};

#[derive(Debug, Default)]
pub struct PoolRefusalLog {
    refusals: AtomicU64,
    last: Mutex<Option<String>>,
}

impl PoolRefusalLog {
    #[must_use]
    pub fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn last_refusal(&self) -> Option<String> {
        match self.last.lock() {
            Ok(slot) => slot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn record(&self, error: &DataFusionError) {
        let text = error.to_string();
        match self.last.lock() {
            Ok(mut slot) => *slot = Some(text),
            Err(poisoned) => *poisoned.into_inner() = Some(text),
        }
        self.refusals.fetch_add(1, Ordering::Release);
    }
}

pub struct RefusalRecordingPool {
    inner: Arc<dyn MemoryPool>,
    log: Arc<PoolRefusalLog>,
}

impl RefusalRecordingPool {
    #[must_use]
    pub fn new(inner: Arc<dyn MemoryPool>, log: Arc<PoolRefusalLog>) -> Self {
        Self { inner, log }
    }
}

impl fmt::Debug for RefusalRecordingPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, formatter)
    }
}

impl fmt::Display for RefusalRecordingPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, formatter)
    }
}

impl MemoryPool for RefusalRecordingPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        self.inner
            .try_grow(reservation, additional)
            .inspect_err(|error| self.log.record(error))
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[must_use]
pub fn pool_refusal_log(pool: &dyn MemoryPool) -> Option<Arc<PoolRefusalLog>> {
    pool.downcast_ref::<RefusalRecordingPool>()
        .map(|recording| Arc::clone(&recording.log))
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::{
        FairSpillPool, MemoryConsumer, MemoryLimit, UnboundedMemoryPool,
    };

    use super::*;

    fn recording(bytes: usize) -> (Arc<dyn MemoryPool>, Arc<PoolRefusalLog>) {
        let log = Arc::new(PoolRefusalLog::default());
        let pool: Arc<dyn MemoryPool> = Arc::new(RefusalRecordingPool::new(
            Arc::new(FairSpillPool::new(bytes)),
            Arc::clone(&log),
        ));
        (pool, log)
    }

    #[test]
    fn a_refused_grow_is_recorded_with_the_engine_text_that_named_the_pool() {
        let (pool, log) = recording(1024);
        let reservation = MemoryConsumer::new("probe").register(&pool);
        reservation
            .try_grow(1024 * 1024)
            .expect_err("a 1 KiB pool refuses a 1 MiB reservation");
        assert_eq!(log.refusals(), 1, "one refusal, counted once");
        let message = log
            .last_refusal()
            .expect("a counted refusal carries its text");
        assert!(
            message.contains("fair("),
            "the pool names itself: {message}"
        );
        assert!(
            message.to_lowercase().contains("resources exhausted"),
            "the recorded text is the typed refusal: {message}"
        );
    }

    #[test]
    fn a_granted_grow_records_nothing() {
        let (pool, log) = recording(1024 * 1024);
        let reservation = MemoryConsumer::new("probe").register(&pool);
        reservation.try_grow(1024).expect("1 KiB fits a 1 MiB pool");
        assert_eq!(log.refusals(), 0, "a granted allocation is not a refusal");
        assert!(log.last_refusal().is_none());
    }

    #[test]
    fn the_wrapper_delegates_every_observable_property_of_the_inner_pool() {
        let (pool, _log) = recording(4096);
        assert_eq!(pool.name(), "fair", "the pool name is the inner one");
        assert!(
            matches!(pool.memory_limit(), MemoryLimit::Finite(4096)),
            "the finite limit survives the wrapper"
        );
        assert_eq!(
            pool.to_string(),
            FairSpillPool::new(4096).to_string(),
            "the Display text is the inner pool's, so refusal messages are unchanged"
        );
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn the_log_is_reachable_from_the_pool_and_absent_from_a_bare_one() {
        let (pool, log) = recording(4096);
        let found = pool_refusal_log(pool.as_ref()).expect("the wrapper carries its log");
        assert!(Arc::ptr_eq(&found, &log), "the same log, not a copy");
        let bare: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        assert!(
            pool_refusal_log(bare.as_ref()).is_none(),
            "an unwrapped pool has no log"
        );
    }
}
