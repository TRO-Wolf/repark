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

#[tokio::test]
async fn a_runtime_pool_resize_keeps_the_refusal_log_alive() {
    let session = ReparkSession::builder()
        .memory_limit_bytes(64 * 1024 * 1024)
        .build()
        .expect("a 64 MiB pool builds");
    let before = Arc::clone(&session.context().runtime_env().memory_pool);
    let carried = pool_refusal_log(before.as_ref()).expect("the build-time pool carries a log");

    session
        .sql("SET datafusion.runtime.memory_limit = '8M'")
        .await
        .expect("the runtime resize applies");

    let after: Arc<dyn MemoryPool> = Arc::clone(&session.context().runtime_env().memory_pool);
    assert!(
        matches!(after.memory_limit(), MemoryLimit::Finite(bytes) if bytes == 8 * 1024 * 1024),
        "the swap installed the new size"
    );
    let survivor = pool_refusal_log(after.as_ref()).expect("the swapped pool still records");
    assert!(
        Arc::ptr_eq(&survivor, &carried),
        "one log per session, carried across the swap — the facade forwards the builder key as a \
         runtime SET, so a fresh log here would disarm the containment on every bounded session"
    );

    let reservation = MemoryConsumer::new("probe").register(&after);
    reservation
        .try_grow(64 * 1024 * 1024)
        .expect_err("64 MiB does not fit an 8 MiB pool");
    assert_eq!(carried.refusals(), 1, "the carried log sees the new pool");
}

#[tokio::test]
async fn a_runtime_opt_out_drops_the_refusal_log_with_the_pool() {
    let session = ReparkSession::builder()
        .memory_limit_bytes(64 * 1024 * 1024)
        .build()
        .expect("a 64 MiB pool builds");
    session
        .sql("SET datafusion.runtime.memory_limit = '0'")
        .await
        .expect("the opt-out applies");
    let after: Arc<dyn MemoryPool> = Arc::clone(&session.context().runtime_env().memory_pool);
    assert!(
        pool_refusal_log(after.as_ref()).is_none(),
        "an unbounded pool has nothing to refuse"
    );
}
