#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use repark_core::ReparkSession;
use repark_distributed::{
    DistributedExecutor, JobStatus, LocalDataFusionExecutor, ReparkClusterExecutor,
    ReparkSessionProvider,
};

fn register_table_t(context: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
    let batch_one = match RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    ) {
        Ok(batch) => batch,
        Err(error) => panic!("batch_one: {error}"),
    };
    let batch_two = match RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![4_i64, 5]))],
    ) {
        Ok(batch) => batch,
        Err(error) => panic!("batch_two: {error}"),
    };
    let table = match MemTable::try_new(schema, vec![vec![batch_one], vec![batch_two]]) {
        Ok(table) => table,
        Err(error) => panic!("MemTable: {error}"),
    };
    if let Err(error) = context.register_table("t", Arc::new(table)) {
        panic!("register_table t: {error}");
    }
}

async fn physical_plan(context: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    let frame = match context.sql(sql).await {
        Ok(frame) => frame,
        Err(error) => panic!("sql {sql}: {error}"),
    };
    match frame.create_physical_plan().await {
        Ok(plan) => plan,
        Err(error) => panic!("create_physical_plan {sql}: {error}"),
    }
}

async fn drain_stream(
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(batch) => batches.push(batch),
            Err(error) => panic!("stream: {error}"),
        }
    }
    batches
}

fn bind_address() -> SocketAddr {
    match "127.0.0.1:0".parse() {
        Ok(address) => address,
        Err(error) => panic!("bind address: {error}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_executors_sum_matches_local_and_status_walks_queued_running_completed() {
    let session = match ReparkSession::new() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::new: {error}"),
    };
    let context = session.context().clone();
    register_table_t(&context);
    let sql = "SELECT sum(x) FROM t";
    let plan = physical_plan(&context, sql).await;

    let local = LocalDataFusionExecutor::new(context.clone());
    let local_handle = match local.execute(Arc::clone(&plan)).await {
        Ok(handle) => handle,
        Err(error) => panic!("local execute: {error}"),
    };
    let expected = drain_stream(local_handle.stream()).await;

    let provider = ReparkSessionProvider::from_session(&session);
    let cluster = match ReparkClusterExecutor::new(2, bind_address(), provider).await {
        Ok(cluster) => cluster,
        Err(error) => panic!("ReparkClusterExecutor::new: {error}"),
    };
    let handle = match cluster.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("cluster execute: {error}"),
    };
    let job = handle.id;

    let mut seen = Vec::new();
    let started = Instant::now();
    loop {
        let status = match cluster.status(job).await {
            Ok(status) => status,
            Err(error) => panic!("status poll: {error}"),
        };
        if seen.last() != Some(&status) {
            seen.push(status.clone());
        }
        if matches!(status, JobStatus::Running { .. }) {
            break;
        }
        if matches!(
            status,
            JobStatus::Completed | JobStatus::Failed(_) | JobStatus::Cancelled
        ) {
            break;
        }
        assert!(
            started.elapsed() <= Duration::from_secs(15),
            "timed out waiting for Running, seen {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        seen.iter()
            .any(|status| matches!(status, JobStatus::Queued)),
        "status walk missing Queued: {seen:?}"
    );
    assert!(
        seen.iter()
            .any(|status| matches!(status, JobStatus::Running { .. })),
        "status walk missing Running: {seen:?}"
    );

    let got = tokio::time::timeout(Duration::from_secs(30), drain_stream(handle.stream()))
        .await
        .expect("cluster drain timed out after 30s");
    assert!(got == expected, "got {got:?} != expected {expected:?}");

    let after = match cluster.status(job).await {
        Ok(status) => status,
        Err(error) => panic!("status after: {error}"),
    };
    assert!(
        after == JobStatus::Completed,
        "after drain: {after:?}, walk {seen:?}"
    );

    let counts = match cluster.executor_task_counts(job).await {
        Ok(counts) => counts,
        Err(error) => panic!("executor_task_counts: {error}"),
    };
    assert!(
        counts.len() >= 2,
        "expected tasks on both executors, got {counts:?}"
    );
    for (executor_id, count) in &counts {
        assert!(
            *count >= 1,
            "executor {executor_id} ran {count} tasks, want >= 1; {counts:?}"
        );
    }
}
