use std::sync::Arc;

use datafusion::execution::memory_pool::MemoryPool;

use crate::{ReparkSession, pool_refusal_log};

const POOL_BYTES: usize = 8 * 1024 * 1024;

fn left_sql(limit: u64) -> String {
    format!(
        "SELECT value AS id, md5(CAST(value AS VARCHAR)) AS h, value % 1024 AS g, \
         concat(md5(CAST(value AS VARCHAR)), md5(CAST((value + 1) AS VARCHAR))) AS payload, \
         CAST(value AS DOUBLE) * 1.5 AS v FROM generate_series(0, {limit})"
    )
}

fn inner_sql() -> String {
    format!(
        "SELECT l.id, r.v FROM ({}) l JOIN ({}) r ON l.v < r.v",
        left_sql(999_999),
        left_sql(63)
    )
}

fn left_join_sql() -> String {
    format!(
        "SELECT l.id, r.v FROM ({}) l LEFT JOIN ({}) r ON l.v < r.v",
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

async fn assert_nested_loop_shape(session: &ReparkSession, sql: &str) {
    let explained = session
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .expect("the join plans");
    let plan_text = format!("{:?}", explained.collect().await.expect("explain collects"));
    assert!(
        plan_text.contains("NestedLoopJoinExec"),
        "the pin must exercise the nested-loop join path, planned: {plan_text}"
    );
}

fn assert_typed_refusal(message: &str) {
    assert!(
        message.to_lowercase().contains("resources exhausted") && message.contains("fair("),
        "a refused join is the typed pool refusal, got: {message}"
    );
    assert!(
        !message.to_lowercase().contains("panic"),
        "a refused join never surfaces a panic payload, got: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tight_pool_spills_an_inner_nested_loop_join_with_exact_values() {
    assert_nested_loop_shape(&tight_session(), &inner_sql()).await;
    for _ in 0..3 {
        let session = tight_session();
        let outcome = session
            .sql(&format!(
                "SELECT count(*) AS n, sum(id) AS s FROM ({}) t",
                inner_sql()
            ))
            .await
            .expect("the join plans")
            .collect()
            .await;
        assert!(
            refusal_count(&session) >= 1,
            "the pool must refuse during the run, or the pin passes vacuously"
        );
        match outcome {
            Ok(batches) => {
                let total: usize = batches.iter().map(|batch| batch.num_rows()).sum();
                assert_eq!(total, 1, "the aggregate answers one row");
                let counts = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("count(*) is Int64");
                assert_eq!(counts.value(0), 2016, "the spilled join keeps every match");
                let sums = batches[0]
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("sum(id) is Int64");
                assert_eq!(sums.value(0), 41664, "the spilled join keeps every sum");
            }
            Err(error) => assert_typed_refusal(&error.to_string()),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tight_pool_refuses_a_left_nested_loop_join_with_the_typed_exception() {
    assert_nested_loop_shape(&tight_session(), &left_join_sql()).await;
    for _ in 0..3 {
        let session = tight_session();
        let error = session
            .sql(&format!("EXPLAIN ANALYZE {}", left_join_sql()))
            .await
            .expect("the join plans")
            .collect()
            .await
            .expect_err("a left join with four right partitions must not spill rows");
        assert!(
            refusal_count(&session) >= 1,
            "the pool must refuse during the run, or the pin passes vacuously"
        );
        assert_typed_refusal(&error.to_string());
    }
}
