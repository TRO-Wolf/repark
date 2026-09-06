use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool};

use crate::{ReparkSession, pool_refusal_log};

#[tokio::test]
async fn a_bounded_session_installs_a_pool_whose_refusals_are_recorded() {
    let session = ReparkSession::builder()
        .memory_limit_bytes(1024 * 1024)
        .build()
        .expect("a 1 MiB pool builds");
    let pool: Arc<dyn MemoryPool> = Arc::clone(&session.context().runtime_env().memory_pool);
    assert!(
        matches!(pool.memory_limit(), MemoryLimit::Finite(bytes) if bytes == 1024 * 1024),
        "the wrapper still reports the finite pool size"
    );
    let log = pool_refusal_log(pool.as_ref()).expect("a bounded session carries a refusal log");
    assert_eq!(log.refusals(), 0, "a fresh session has refused nothing");

    let reservation = MemoryConsumer::new("probe").register(&pool);
    reservation
        .try_grow(64 * 1024 * 1024)
        .expect_err("64 MiB does not fit a 1 MiB pool");
    assert_eq!(log.refusals(), 1, "the session's own pool records it");
    assert!(
        log.last_refusal()
            .expect("the refusal carries its text")
            .contains("fair("),
        "the refusal text still names the FairSpillPool"
    );
}

#[tokio::test]
async fn an_unbounded_session_installs_no_refusal_log() {
    let session = ReparkSession::builder()
        .memory_limit_bytes(0)
        .build()
        .expect("an unbounded session builds");
    let pool: Arc<dyn MemoryPool> = Arc::clone(&session.context().runtime_env().memory_pool);
    assert!(
        pool_refusal_log(pool.as_ref()).is_none(),
        "opting out of the pool opts out of the containment"
    );
}
