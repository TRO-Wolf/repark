#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use repark_core::ReparkSession;
use repark_distributed::{
    DistributedExecutor, JobStatus, LocalDataFusionExecutor, ReparkClusterExecutor,
    ReparkLogicalExtensionCodec, ReparkPhysicalExtensionCodec, ReparkSessionProvider,
    repark_ballista_codec,
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

fn times_ten(args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
    let ColumnarValue::Array(array) = &args[0] else {
        return Err(datafusion::error::DataFusionError::Execution(
            "repark_times_ten expected an array".to_owned(),
        ));
    };
    let Some(ints) = array.as_any().downcast_ref::<Int64Array>() else {
        return Err(datafusion::error::DataFusionError::Execution(
            "repark_times_ten expected Int64".to_owned(),
        ));
    };
    let scaled: Int64Array = ints.iter().map(|value| value.map(|n| n * 10)).collect();
    Ok(ColumnarValue::Array(Arc::new(scaled)))
}

fn register_repark_times_ten(context: &SessionContext) {
    context.register_udf(create_udf(
        "repark_times_ten",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(times_ten),
    ));
}

async fn wait_until_running(
    cluster: &ReparkClusterExecutor,
    job: repark_distributed::JobId,
    bound: Duration,
) -> Vec<JobStatus> {
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
            return seen;
        }
        if matches!(
            status,
<<<<<<< HEAD
            JobStatus::Completed { .. } | JobStatus::Failed(_) | JobStatus::Cancelled
=======
            JobStatus::Completed | JobStatus::Failed(_) | JobStatus::Cancelled
>>>>>>> origin/main
        ) {
            panic!("job left Running before the wait finished, seen {seen:?}");
        }
        assert!(
            started.elapsed() <= bound,
            "timed out waiting for Running, seen {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
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
<<<<<<< HEAD
            JobStatus::Completed { .. } | JobStatus::Failed(_) | JobStatus::Cancelled
=======
            JobStatus::Completed | JobStatus::Failed(_) | JobStatus::Cancelled
>>>>>>> origin/main
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
<<<<<<< HEAD
        matches!(after, JobStatus::Completed { .. }),
=======
        after == JobStatus::Completed,
>>>>>>> origin/main
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repark_udf_registered_through_session_provider_resolves_on_two_executors() {
    let session = match ReparkSession::new() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::new: {error}"),
    };
    let context = session.context().clone();
    register_table_t(&context);
    register_repark_times_ten(&context);
    let sql = "SELECT sum(repark_times_ten(x)) FROM t";
    let plan = physical_plan(&context, sql).await;

    let local = LocalDataFusionExecutor::new(context.clone());
    let local_handle = match local.execute(Arc::clone(&plan)).await {
        Ok(handle) => handle,
        Err(error) => panic!("local execute: {error}"),
    };
    let expected = drain_stream(local_handle.stream()).await;

    let provider = ReparkSessionProvider::from_context(&context);
    let has_udf = provider
        .function_registry()
        .scalar_functions
        .contains_key("repark_times_ten");
    assert!(
        has_udf,
        "ReparkSessionProvider registry missing repark_times_ten"
    );

    let vanilla_provider = ReparkSessionProvider::from_context(&SessionContext::new());
    let vanilla_has_udf = vanilla_provider
        .function_registry()
        .scalar_functions
        .contains_key("repark_times_ten");
    assert!(
        !vanilla_has_udf,
        "vanilla DataFusion session must not carry repark_times_ten"
    );

    let cluster = match ReparkClusterExecutor::new(2, bind_address(), provider).await {
        Ok(cluster) => cluster,
        Err(error) => panic!("ReparkClusterExecutor::new: {error}"),
    };
    let handle = match cluster.execute(Arc::clone(&plan)).await {
        Ok(handle) => handle,
        Err(error) => panic!("cluster execute: {error}"),
    };
    let got = tokio::time::timeout(Duration::from_secs(30), drain_stream(handle.stream()))
        .await
        .expect("cluster drain timed out after 30s");
    assert!(got == expected, "got {got:?} != expected {expected:?}");

    let vanilla_cluster =
        match ReparkClusterExecutor::new(1, bind_address(), vanilla_provider).await {
            Ok(cluster) => cluster,
            Err(error) => panic!("vanilla ReparkClusterExecutor::new: {error}"),
        };
    let vanilla_result = vanilla_cluster.execute(plan).await;
    match vanilla_result {
        Ok(handle) => {
            let (batches, errors) = tokio::time::timeout(Duration::from_secs(30), async {
                let mut stream = handle.stream();
                let mut batches = Vec::new();
                let mut errors = Vec::new();
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(batch) => batches.push(batch),
                        Err(error) => errors.push(error.to_string()),
                    }
                }
                (batches, errors)
            })
            .await
            .expect("vanilla drain timed out after 30s");
            assert!(
                batches.is_empty(),
                "vanilla cluster must not produce rows for repark_times_ten, got {batches:?}"
            );
            assert!(
                !errors.is_empty(),
                "vanilla cluster must fail to resolve repark_times_ten"
            );
            let joined = errors.join("; ");
            assert!(
                joined.contains("repark_times_ten"),
                "vanilla cluster error should name repark_times_ten, got {joined}"
            );
        }
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.contains("repark_times_ten"),
                "vanilla cluster error should name repark_times_ten, got {message}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_mid_flight_sets_cancelled_and_no_running_tasks_within_five_seconds() {
    let session = match ReparkSession::new() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::new: {error}"),
    };
    let context = session.context().clone();
    let sql = "SELECT value FROM range(100000000)";
    let plan = physical_plan(&context, sql).await;
    let provider = ReparkSessionProvider::from_context(&context);
    let cluster = match ReparkClusterExecutor::new(2, bind_address(), provider).await {
        Ok(cluster) => cluster,
        Err(error) => panic!("ReparkClusterExecutor::new: {error}"),
    };
    let handle = match cluster.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("cluster execute: {error}"),
    };
    let job = handle.id;
    let seen = wait_until_running(&cluster, job, Duration::from_secs(15)).await;
    assert!(
        seen.iter()
            .any(|status| matches!(status, JobStatus::Queued | JobStatus::Running { .. })),
        "status walk before cancel: {seen:?}"
    );

    tokio::time::timeout(Duration::from_secs(5), cluster.cancel(job))
        .await
        .expect("cancel timed out after 5s")
        .unwrap_or_else(|error| panic!("cancel: {error}"));

    let status = match cluster.status(job).await {
        Ok(status) => status,
        Err(error) => panic!("status after cancel: {error}"),
    };
    assert!(
        status == JobStatus::Cancelled,
        "after cancel: {status:?}, walk {seen:?}"
    );

    let started = Instant::now();
    loop {
        let running = match cluster.running_executor_task_counts(job).await {
            Ok(counts) => counts,
            Err(error) => panic!("running_executor_task_counts: {error}"),
        };
        let total: usize = running.values().sum();
        if total == 0 {
            break;
        }
        assert!(
            started.elapsed() <= Duration::from_secs(5),
            "executors still running after 5s: {running:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn repark_ballista_codec_installs_ballista_default_physical_and_logical_codecs() {
    let installed = repark_ballista_codec();
    let physical = ReparkPhysicalExtensionCodec::new().into_inner();
    let logical = ReparkLogicalExtensionCodec::new().into_inner();
    assert!(
        format!("{:?}", installed.physical_extension_codec()) == format!("{physical:?}"),
        "physical codec {:?} != wrapped Ballista default {physical:?}",
        installed.physical_extension_codec()
    );
    assert!(
        format!("{:?}", installed.logical_extension_codec()) == format!("{logical:?}"),
        "logical codec {:?} != wrapped Ballista default {logical:?}",
        installed.logical_extension_codec()
    );
}
