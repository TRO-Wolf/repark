use std::sync::Arc;

use tokio::runtime::Runtime;

use super::EngineRuntime;

/// EC-5 (design §4 Q7): the handle is a SHARED pointer — cloning it must not mint a second
/// executor. This is the property the binding's process-wide `OnceLock<EngineRuntime>` relies
/// on (`sequential_sessions_share_one_tokio_runtime` observes the same fact one layer up), and
/// it is what makes "the type in core, the instance in the embedding" honest: a clone that
/// silently built a fresh runtime would give every session its own thread pool.
#[test]
fn engine_runtime_clone_shares_one_executor_and_drives_futures() {
    let owned = Arc::new(Runtime::new().expect("embedder builds the runtime"));
    let handle = EngineRuntime::new(Arc::clone(&owned));
    let cloned = handle.clone();

    assert!(
        Arc::ptr_eq(handle.runtime(), cloned.runtime()),
        "cloning an EngineRuntime shares the embedder's executor, never builds a new one"
    );
    assert!(
        Arc::ptr_eq(handle.runtime(), &owned),
        "the handle borrows the runtime the EMBEDDER owns — core constructs none"
    );
    assert_eq!(
        cloned.block_on(async { 40 + 2 }),
        42,
        "block_on drives a future on the handled runtime"
    );
}
