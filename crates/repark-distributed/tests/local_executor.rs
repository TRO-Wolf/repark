#![allow(clippy::disallowed_methods)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use repark_core::ReparkSession;
use repark_distributed::{DistributedExecutor, JobStatus, LocalDataFusionExecutor};

fn session_context() -> SessionContext {
    let session = match ReparkSession::new() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::new: {error}"),
    };
    session.context().clone()
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

async fn collect_sql(context: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let frame = match context.sql(sql).await {
        Ok(frame) => frame,
        Err(error) => panic!("sql {sql}: {error}"),
    };
    match frame.collect().await {
        Ok(batches) => batches,
        Err(error) => panic!("collect {sql}: {error}"),
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

#[tokio::test]
async fn range_1000_sum_through_local_executor_equals_direct_datafusion_answer() {
    let context = session_context();
    let sql = "SELECT SUM(value) FROM range(1000)";
    let expected = collect_sql(&context, sql).await;
    let executor = LocalDataFusionExecutor::new(context.clone());
    let plan = physical_plan(&context, sql).await;
    let handle = match executor.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("execute: {error}"),
    };
    let got = drain_stream(handle.stream()).await;
    assert!(got == expected, "got {got:?} != expected {expected:?}");
}

#[tokio::test]
async fn status_is_queued_or_running_before_drain_and_completed_after() {
    let context = session_context();
    let sql = "SELECT SUM(value) FROM range(1000)";
    let executor = LocalDataFusionExecutor::new(context.clone());
    let plan = physical_plan(&context, sql).await;
    let handle = match executor.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("execute: {error}"),
    };
    let job = handle.id;
    let before = match executor.status(job).await {
        Ok(status) => status,
        Err(error) => panic!("status before: {error}"),
    };
    assert!(
        matches!(before, JobStatus::Queued | JobStatus::Running { .. }),
        "before drain: {before:?}"
    );
    let mut stream = handle.stream();
    let first = stream.next().await;
    assert!(first.is_some(), "sum should yield a batch");
    if let Some(Err(error)) = first {
        panic!("stream: {error}");
    }
    let during = match executor.status(job).await {
        Ok(status) => status,
        Err(error) => panic!("status during: {error}"),
    };
    assert!(
        matches!(
            during,
            JobStatus::Running {
                completed_stages: _,
                total_stages: _
            } | JobStatus::Completed
        ),
        "during drain: {during:?}"
    );
    while let Some(item) = stream.next().await {
        if let Err(error) = item {
            panic!("stream: {error}");
        }
    }
    let after = match executor.status(job).await {
        Ok(status) => status,
        Err(error) => panic!("status after: {error}"),
    };
    assert!(after == JobStatus::Completed, "after drain: {after:?}");
}

#[tokio::test]
async fn cancel_on_a_long_range_returns_within_one_second_and_status_is_cancelled() {
    let context = session_context();
    let sql = "SELECT value FROM range(100000000)";
    let executor = LocalDataFusionExecutor::new(context.clone());
    let plan = physical_plan(&context, sql).await;
    let handle = match executor.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("execute: {error}"),
    };
    let job = handle.id;
    let mut stream = handle.stream();
    let started = Instant::now();
    let drain = async { while stream.next().await.is_some() {} };
    let cancel = async {
        match executor.cancel(job).await {
            Ok(()) => (),
            Err(error) => panic!("cancel: {error}"),
        }
    };
    tokio::select! {
        () = drain => panic!("long range drained before cancel"),
        () = cancel => (),
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "cancel took {elapsed:?}, want < 1s"
    );
    let status = match executor.status(job).await {
        Ok(status) => status,
        Err(error) => panic!("status: {error}"),
    };
    assert!(status == JobStatus::Cancelled, "after cancel: {status:?}");
}
