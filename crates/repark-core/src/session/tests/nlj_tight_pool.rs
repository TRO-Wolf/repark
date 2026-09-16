use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryPool;

use crate::{ReparkSession, pool_refusal_log};

const POOL_BYTES: usize = 8 * 1024 * 1024;
const ITERATIONS: usize = 10;

fn left_sql(limit: u64) -> String {
    format!(
        "SELECT value AS id, md5(CAST(value AS VARCHAR)) AS h, value % 1024 AS g, \
         concat(md5(CAST(value AS VARCHAR)), md5(CAST((value + 1) AS VARCHAR))) AS payload, \
         CAST(value AS DOUBLE) * 1.5 AS v FROM generate_series(0, {limit})"
    )
}

fn join_sql() -> String {
    format!(
        "SELECT l.id, r.v FROM ({}) l JOIN ({}) r ON l.v < r.v",
        left_sql(999_999),
        left_sql(63)
    )
}

fn tight_session() -> ReparkSession {
    ReparkSession::builder()
        .memory_limit_bytes(POOL_BYTES)
        .target_partitions(4)
        .build()
        .expect("an 8 MiB pool builds")
}

fn refusal_count(session: &ReparkSession) -> u64 {
    let pool: Arc<dyn MemoryPool> = Arc::clone(&session.context().runtime_env().memory_pool);
    pool_refusal_log(pool.as_ref())
        .expect("a bounded session carries a refusal log")
        .refusals()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tight_pool_refuses_or_spills_a_nested_loop_join_without_a_panic() {
    let shape = tight_session();
    let explained = shape
        .sql(&format!("EXPLAIN {}", join_sql()))
        .await
        .expect("the join plans");
    let plan_text = format!("{:?}", explained.collect().await.expect("explain collects"));
    assert!(
        plan_text.contains("NestedLoopJoinExec"),
        "the pin must exercise the nested-loop join path, planned: {plan_text}"
    );
    for _ in 0..ITERATIONS {
        let session = tight_session();
        let outcome = session
            .sql(&format!("EXPLAIN ANALYZE {}", join_sql()))
            .await
            .expect("the join plans")
            .collect()
            .await;
        assert!(
            refusal_count(&session) >= 1,
            "the pool must refuse during the run, or the pin passes vacuously"
        );
        match outcome {
            Ok(_) => {}
            Err(error) => {
                let message = error.to_string();
                assert!(
                    message.to_lowercase().contains("resources exhausted")
                        && message.contains("fair("),
                    "a refused join is the typed pool refusal, got: {message}"
                );
                assert!(
                    !message.to_lowercase().contains("panic"),
                    "a refused join never surfaces a panic payload, got: {message}"
                );
            }
        }
    }
}
