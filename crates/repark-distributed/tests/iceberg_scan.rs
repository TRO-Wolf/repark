#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_methods)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datafusion::arrow::array::Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use repark_core::{CatalogKind, CatalogSpec, ReparkSession};
use repark_distributed::{
    DistributedExecutor, IcebergScanSpec, JobStatus, LocalDataFusionExecutor,
    ReparkClusterExecutor, ReparkSessionProvider,
};

const DATA_FILE_COUNT: usize = 8;
const EXECUTOR_COUNT: usize = 2;
const CATALOG_NAME: &str = "ice";
const NAMESPACE: &str = "sales";
const TABLE: &str = "orders";

fn bind_address() -> SocketAddr {
    match "127.0.0.1:0".parse() {
        Ok(address) => address,
        Err(error) => panic!("bind address: {error}"),
    }
}

fn unique_warehouse() -> PathBuf {
    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos(),
        Err(_) => 0,
    };
    let path = std::env::temp_dir().join(format!("repark-m1d-ice-{}-{nanos}", std::process::id()));
    if let Err(error) = std::fs::create_dir_all(&path) {
        panic!("create warehouse {}: {error}", path.display());
    }
    path
}

fn memory_catalog_spec(warehouse: &str) -> CatalogSpec {
    let mut props = HashMap::new();
    props.insert("warehouse".to_owned(), warehouse.to_owned());
    CatalogSpec {
        name: CATALOG_NAME.to_owned(),
        kind: CatalogKind::Memory,
        props,
    }
}

fn scan_spec(warehouse: &str, snapshot_id: Option<i64>, filters: Vec<String>) -> IcebergScanSpec {
    IcebergScanSpec::new(
        memory_catalog_spec(warehouse),
        vec![
            CATALOG_NAME.to_owned(),
            NAMESPACE.to_owned(),
            TABLE.to_owned(),
        ],
        snapshot_id,
        Some(vec!["id".to_owned()]),
        filters,
    )
}

async fn iceberg_session(warehouse: &str) -> (ReparkSession, SessionContext) {
    let session = match ReparkSession::builder()
        .target_partitions(EXECUTOR_COUNT)
        .build()
    {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::build: {error}"),
    };
    if let Err(error) = session
        .register_memory_catalog(CATALOG_NAME, warehouse)
        .await
    {
        panic!("register_memory_catalog: {error}");
    }
    if let Err(error) = session
        .create_namespace(CATALOG_NAME, NAMESPACE, HashMap::new())
        .await
    {
        panic!("create_namespace: {error}");
    }
    if let Err(error) = session
        .testing_oob_create_table(CATALOG_NAME, NAMESPACE, TABLE, warehouse)
        .await
    {
        panic!("testing_oob_create_table: {error}");
    }
    if let Err(error) = session.refresh_catalog_provider(CATALOG_NAME).await {
        panic!("refresh_catalog_provider: {error}");
    }
    let context = session.context().clone();
    (session, context)
}

async fn insert_id_rows(context: &SessionContext, count: usize) {
    for value in 0..count {
        let sql = format!("INSERT INTO {CATALOG_NAME}.{NAMESPACE}.{TABLE} VALUES ({value})");
        let frame = match context.sql(&sql).await {
            Ok(frame) => frame,
            Err(error) => panic!("insert sql {sql}: {error}"),
        };
        if let Err(error) = frame.collect().await {
            panic!("insert collect {sql}: {error}");
        }
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

fn sorted_row_keys(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for column_index in 0..batch.num_columns() {
                let array = batch.column(column_index);
                if array.is_null(row_index) {
                    cells.push("NULL".to_owned());
                    continue;
                }
                match ScalarValue::try_from_array(array.as_ref(), row_index) {
                    Ok(value) => cells.push(value.to_string()),
                    Err(error) => panic!("scalar row {row_index} col {column_index}: {error}"),
                }
            }
            rows.push(cells.join("\t"));
        }
    }
    rows.sort();
    rows
}

fn plan_text(plan: &dyn ExecutionPlan) -> String {
    datafusion::physical_plan::displayable(plan)
        .indent(false)
        .to_string()
}

fn count_named(plan: &dyn ExecutionPlan, name: &str) -> usize {
    let mut count = 0_usize;
    let mut stack: Vec<&dyn ExecutionPlan> = vec![plan];
    while let Some(node) = stack.pop() {
        if node.name() == name {
            count += 1;
        }
        for child in node.children() {
            stack.push(child.as_ref());
        }
    }
    count
}

async fn local_answer(context: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    let local = LocalDataFusionExecutor::new(context.clone());
    let handle = match local.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("local execute: {error}"),
    };
    drain_stream(handle.stream()).await
}

async fn cluster_answer(
    session: &ReparkSession,
    plan: Arc<dyn ExecutionPlan>,
) -> (Vec<RecordBatch>, HashMap<String, usize>, JobStatus) {
    let provider = ReparkSessionProvider::from_session(session);
    let cluster = match ReparkClusterExecutor::new(EXECUTOR_COUNT, bind_address(), provider).await {
        Ok(cluster) => cluster,
        Err(error) => panic!("ReparkClusterExecutor::new: {error}"),
    };
    let handle = match cluster.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("cluster execute: {error}"),
    };
    let job = handle.id;
    let batches = tokio::time::timeout(Duration::from_secs(30), drain_stream(handle.stream()))
        .await
        .expect("cluster drain timed out after 30s");
    let counts = match cluster.executor_task_counts(job).await {
        Ok(counts) => counts,
        Err(error) => panic!("executor_task_counts: {error}"),
    };
    let status = match cluster.status(job).await {
        Ok(status) => status,
        Err(error) => panic!("status after drain: {error}"),
    };
    (batches, counts, status)
}

#[test]
fn iceberg_scan_spec_round_trips_catalog_table_snapshot_projection_and_filters() {
    let spec = scan_spec(
        "/tmp/repark-m1d-warehouse",
        Some(42),
        vec!["id >= 4".to_owned()],
    );
    let encoded = match spec.encode() {
        Ok(bytes) => bytes,
        Err(error) => panic!("encode: {error}"),
    };
    let decoded = match IcebergScanSpec::decode(&encoded) {
        Ok(decoded) => decoded,
        Err(error) => panic!("decode: {error}"),
    };
    assert!(decoded == spec, "decoded {decoded:?} != spec {spec:?}");
    assert!(decoded.catalog.kind == CatalogKind::Memory);
    assert!(decoded.snapshot_id == Some(42));
    assert!(decoded.projection.as_deref() == Some(&["id".to_owned()][..]));
    assert!(decoded.filters == ["id >= 4"]);
}

#[test]
fn iceberg_scan_spec_decode_rejects_truncated_payload() {
    let spec = scan_spec("/tmp/repark-m1d-warehouse", None, Vec::new());
    let encoded = match spec.encode() {
        Ok(bytes) => bytes,
        Err(error) => panic!("encode: {error}"),
    };
    let truncated = &encoded[..encoded.len().saturating_sub(1)];
    let error = match IcebergScanSpec::decode(truncated) {
        Ok(decoded) => panic!("truncated payload decoded: {decoded:?}"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("truncated") || error.contains("trailing"),
        "truncated decode should name the fault, got {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn iceberg_scan_spec_rebuilds_provider_from_session_not_ambient_catalog() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = iceberg_session(&warehouse_text).await;
    insert_id_rows(&context, DATA_FILE_COUNT).await;
    let spec = scan_spec(&warehouse_text, None, Vec::new());
    let provider = match spec.rebuild_provider(&context).await {
        Ok(provider) => provider,
        Err(error) => panic!("rebuild_provider: {error}"),
    };
    let names: Vec<String> = provider
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert!(
        names.iter().any(|name| name == "id"),
        "rebuilt provider schema missing id: {names:?}"
    );
    let files = match spec.data_file_paths(&context).await {
        Ok(files) => files,
        Err(error) => panic!("data_file_paths: {error}"),
    };
    assert!(
        files.len() == DATA_FILE_COUNT,
        "expected {DATA_FILE_COUNT} Iceberg data files, got {}: {files:?}",
        files.len()
    );
    let scan = match spec.scan(&context).await {
        Ok(plan) => plan,
        Err(error) => panic!("spec.scan: {error}"),
    };
    assert!(
        count_named(scan.as_ref(), "IcebergTableScan") == 1,
        "rebuilt scan must be IcebergTableScan, plan:\n{}",
        plan_text(scan.as_ref())
    );
    let vanilla = SessionContext::new();
    let missing = match spec.rebuild_provider(&vanilla).await {
        Ok(_) => panic!("vanilla session rebuilt the Iceberg provider from ambient state"),
        Err(error) => error.to_string(),
    };
    assert!(
        missing.contains("ReparkSessionProvider") || missing.contains("not registered"),
        "rebuild refusal should name the session provider, got {missing}"
    );
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_executors_iceberg_count_sum_and_filtered_scan_match_local_and_each_opened_a_file() {
    let warehouse = unique_warehouse();
    let warehouse_text = warehouse.to_string_lossy().into_owned();
    let (session, context) = iceberg_session(&warehouse_text).await;
    insert_id_rows(&context, DATA_FILE_COUNT).await;
    let spec = scan_spec(&warehouse_text, None, Vec::new());
    let files = match spec.data_file_paths(&context).await {
        Ok(files) => files,
        Err(error) => panic!("data_file_paths: {error}"),
    };
    assert!(
        files.len() == DATA_FILE_COUNT,
        "need {DATA_FILE_COUNT} files, got {}: {files:?}",
        files.len()
    );

    let queries = [
        format!("SELECT count(*) FROM {CATALOG_NAME}.{NAMESPACE}.{TABLE} WHERE id + 0 >= 0"),
        format!("SELECT sum(id) FROM {CATALOG_NAME}.{NAMESPACE}.{TABLE}"),
        format!("SELECT id FROM {CATALOG_NAME}.{NAMESPACE}.{TABLE} WHERE id >= 4"),
    ];
    let mut saw_two_executors = false;
    let mut last_counts = HashMap::new();
    for sql in queries {
        let iceberg_plan = physical_plan(&context, &sql).await;
        assert!(
            count_named(iceberg_plan.as_ref(), "IcebergTableScan") >= 1,
            "local Iceberg plan missing IcebergTableScan for {sql}:\n{}",
            plan_text(iceberg_plan.as_ref())
        );
        let expected = local_answer(&context, Arc::clone(&iceberg_plan)).await;
        let distributed = match spec
            .rewrite_iceberg_table_scans_as_file_groups(iceberg_plan, &context)
            .await
        {
            Ok(plan) => plan,
            Err(error) => panic!("rewrite file groups for {sql}: {error}"),
        };
        assert!(
            count_named(distributed.as_ref(), "IcebergTableScan") == 0,
            "distributed plan still carries IcebergTableScan (needs datafusion-proto to travel): {}",
            plan_text(distributed.as_ref())
        );
        let (got, counts, status) = cluster_answer(&session, distributed).await;
        let expected_rows = sorted_row_keys(&expected);
        let got_rows = sorted_row_keys(&got);
        assert!(
            got_rows == expected_rows,
            "{sql}: cluster rows {got_rows:?} != local rows {expected_rows:?}"
        );
        last_counts = counts.clone();
        if counts.len() >= EXECUTOR_COUNT {
            saw_two_executors = true;
        }
        assert!(
            matches!(status, JobStatus::Completed { .. }),
            "{sql} status after drain: {status:?}"
        );
    }
    if saw_two_executors {
        for (executor_id, count) in &last_counts {
            assert!(
                *count >= 1,
                "executor {executor_id} ran {count} tasks, want >= 1; {last_counts:?}"
            );
        }
    } else {
        panic!(
            "scheduler did not place tasks on {EXECUTOR_COUNT} executors; last counts {last_counts:?}"
        );
    }
    drop(session);
    let _ = std::fs::remove_dir_all(&warehouse);
}
