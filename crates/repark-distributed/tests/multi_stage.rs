#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_methods)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use repark_core::ReparkSession;
use repark_distributed::{
    DistributedExecutor, LocalDataFusionExecutor, ReparkClusterExecutor, ReparkSessionProvider,
};

fn bind_address() -> SocketAddr {
    match "127.0.0.1:0".parse() {
        Ok(address) => address,
        Err(error) => panic!("bind address: {error}"),
    }
}

fn session_with(
    target_partitions: usize,
    extra: &[(&str, &str)],
) -> (ReparkSession, SessionContext) {
    let mut builder = ReparkSession::builder().target_partitions(target_partitions);
    for (key, value) in extra {
        builder = builder.config(*key, *value);
    }
    let session = match builder.build() {
        Ok(session) => session,
        Err(error) => panic!("ReparkSession::build: {error}"),
    };
    let context = session.context().clone();
    (session, context)
}

fn partitioned_kv_table(keys: &[i64], values: &[i64], partitions: usize) -> MemTable {
    assert!(
        keys.len() == values.len(),
        "keys {} != values {}",
        keys.len(),
        values.len()
    );
    assert!(
        partitions > 0 && keys.len() >= partitions,
        "need at least one row per partition"
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let chunk = keys.len() / partitions;
    let mut parts = Vec::with_capacity(partitions);
    for part in 0..partitions {
        let start = part * chunk;
        let end = if part + 1 == partitions {
            keys.len()
        } else {
            start + chunk
        };
        let batch = match RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(keys[start..end].to_vec())),
                Arc::new(Int64Array::from(values[start..end].to_vec())),
            ],
        ) {
            Ok(batch) => batch,
            Err(error) => panic!("kv batch part {part}: {error}"),
        };
        parts.push(vec![batch]);
    }
    match MemTable::try_new(schema, parts) {
        Ok(table) => table,
        Err(error) => panic!("MemTable: {error}"),
    }
}

fn register_kv(
    context: &SessionContext,
    name: &str,
    keys: &[i64],
    values: &[i64],
    partitions: usize,
) {
    if let Err(error) = context.register_table(
        name,
        Arc::new(partitioned_kv_table(keys, values, partitions)),
    ) {
        panic!("register_table {name}: {error}");
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
                    Err(error) => {
                        panic!("scalar row {row_index} col {column_index}: {error}")
                    }
                }
            }
            rows.push(cells.join("\t"));
        }
    }
    rows.sort();
    rows
}

fn walk_nodes(root: &dyn ExecutionPlan) -> Vec<&dyn ExecutionPlan> {
    let mut nodes = Vec::new();
    let mut stack: Vec<&dyn ExecutionPlan> = vec![root];
    while let Some(node) = stack.pop() {
        nodes.push(node);
        for child in node.children() {
            stack.push(child.as_ref());
        }
    }
    nodes
}

fn plan_text(plan: &dyn ExecutionPlan) -> String {
    datafusion::physical_plan::displayable(plan)
        .indent(false)
        .to_string()
}

fn count_named(plan: &dyn ExecutionPlan, name: &str) -> usize {
    walk_nodes(plan)
        .into_iter()
        .filter(|node| node.name() == name)
        .count()
}

fn max_partitions(plan: &dyn ExecutionPlan) -> usize {
    walk_nodes(plan)
        .into_iter()
        .map(|node| node.properties().output_partitioning().partition_count())
        .max()
        .unwrap_or(0)
}

fn join_named<'plan>(plan: &'plan dyn ExecutionPlan, name: &str) -> &'plan dyn ExecutionPlan {
    walk_nodes(plan)
        .into_iter()
        .find(|node| node.name() == name)
        .unwrap_or_else(|| panic!("plan missing {name}: {}", plan_text(plan)))
}

fn subtree_has(plan: &dyn ExecutionPlan, name: &str) -> bool {
    count_named(plan, name) > 0
}

async fn local_answer(context: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    let local = LocalDataFusionExecutor::new(context.clone());
    let handle = match local.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("local execute: {error}"),
    };
    drain_stream(handle.stream()).await
}

async fn cluster_answer(session: &ReparkSession, plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    let provider = ReparkSessionProvider::from_session(session);
    let cluster = match ReparkClusterExecutor::new(2, bind_address(), provider).await {
        Ok(cluster) => cluster,
        Err(error) => panic!("ReparkClusterExecutor::new: {error}"),
    };
    let handle = match cluster.execute(plan).await {
        Ok(handle) => handle,
        Err(error) => panic!("cluster execute: {error}"),
    };
    tokio::time::timeout(Duration::from_secs(30), drain_stream(handle.stream()))
        .await
        .expect("cluster drain timed out after 30s")
}

const DISABLE_COLLECT_LEFT: &[(&str, &str)] = &[
    (
        "datafusion.optimizer.hash_join_single_partition_threshold",
        "0",
    ),
    (
        "datafusion.optimizer.hash_join_single_partition_threshold_rows",
        "0",
    ),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_aggregate_over_four_partitions_matches_local_executor() {
    let (session, context) = session_with(4, &[]);
    let keys: Vec<i64> = (0..16).map(|row| row % 4).collect();
    let values: Vec<i64> = (0..16).map(|row| row + 1).collect();
    register_kv(&context, "sales", &keys, &values, 4);
    let sql = "SELECT k, sum(v) AS total FROM sales GROUP BY k";
    let plan = physical_plan(&context, sql).await;
    let text = plan_text(plan.as_ref());
    assert!(
        count_named(plan.as_ref(), "AggregateExec") >= 2,
        "hash aggregate must be two-stage Partial+Final, plan:\n{text}"
    );
    assert!(
        text.contains("mode=Partial")
            && (text.contains("mode=FinalPartitioned") || text.contains("mode=Final")),
        "hash aggregate must show Partial and Final, plan:\n{text}"
    );
    assert!(
        max_partitions(plan.as_ref()) >= 4,
        "hash aggregate must run over 4 partitions, max={}, plan:\n{text}",
        max_partitions(plan.as_ref())
    );

    let expected = local_answer(&context, Arc::clone(&plan)).await;
    let got = cluster_answer(&session, plan).await;
    let expected_rows = sorted_row_keys(&expected);
    let got_rows = sorted_row_keys(&got);
    assert!(
        got_rows == expected_rows,
        "cluster rows {got_rows:?} != local rows {expected_rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hash_join_of_two_tables_matches_local_executor() {
    let (session, context) = session_with(4, DISABLE_COLLECT_LEFT);
    let left_keys: Vec<i64> = (0..8).collect();
    let left_values: Vec<i64> = (0..8).map(|row| row * 10).collect();
    let right_keys: Vec<i64> = (0..8).map(|row| row * 2).collect();
    let right_values: Vec<i64> = (0..8).map(|row| row * 100).collect();
    register_kv(&context, "left_t", &left_keys, &left_values, 4);
    register_kv(&context, "right_t", &right_keys, &right_values, 4);
    let sql = "SELECT l.k, l.v AS lv, r.v AS rv \
               FROM left_t AS l INNER JOIN right_t AS r ON l.k = r.k";
    let plan = physical_plan(&context, sql).await;
    let text = plan_text(plan.as_ref());
    assert!(
        count_named(plan.as_ref(), "HashJoinExec") == 1,
        "expected one HashJoinExec, plan:\n{text}"
    );
    assert!(
        text.contains("HashJoinExec"),
        "hash join plan missing HashJoinExec:\n{text}"
    );
    let join = join_named(plan.as_ref(), "HashJoinExec");
    let children = join.children();
    assert!(
        children.len() == 2,
        "hash join of two tables must have two children, plan:\n{text}"
    );
    assert!(
        subtree_has(children[0].as_ref(), "RepartitionExec")
            && subtree_has(children[1].as_ref(), "RepartitionExec"),
        "partitioned hash join must shuffle both sides, plan:\n{text}"
    );

    let expected = local_answer(&context, Arc::clone(&plan)).await;
    let got = cluster_answer(&session, plan).await;
    let expected_rows = sorted_row_keys(&expected);
    let got_rows = sorted_row_keys(&got);
    assert!(
        got_rows == expected_rows,
        "cluster rows {got_rows:?} != local rows {expected_rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sort_merge_join_with_prefer_hash_join_false_matches_local_executor() {
    let mut extra = DISABLE_COLLECT_LEFT.to_vec();
    extra.push(("datafusion.optimizer.prefer_hash_join", "false"));
    let (session, context) = session_with(4, &extra);
    let left_keys: Vec<i64> = (0..8).collect();
    let left_values: Vec<i64> = (0..8).map(|row| row * 10).collect();
    let right_keys: Vec<i64> = (0..8).map(|row| row * 2).collect();
    let right_values: Vec<i64> = (0..8).map(|row| row * 100).collect();
    register_kv(&context, "left_t", &left_keys, &left_values, 4);
    register_kv(&context, "right_t", &right_keys, &right_values, 4);
    let sql = "SELECT l.k, l.v AS lv, r.v AS rv \
               FROM left_t AS l INNER JOIN right_t AS r ON l.k = r.k";
    let plan = physical_plan(&context, sql).await;
    let text = plan_text(plan.as_ref());
    assert!(
        count_named(plan.as_ref(), "SortMergeJoinExec") == 1,
        "prefer_hash_join=false must plan SortMergeJoinExec, plan:\n{text}"
    );
    assert!(
        count_named(plan.as_ref(), "HashJoinExec") == 0,
        "prefer_hash_join=false must not plan HashJoinExec, plan:\n{text}"
    );
    let join = join_named(plan.as_ref(), "SortMergeJoinExec");
    let children = join.children();
    assert!(
        children.len() == 2,
        "sort-merge join must have two children, plan:\n{text}"
    );
    assert!(
        subtree_has(children[0].as_ref(), "RepartitionExec")
            && subtree_has(children[1].as_ref(), "RepartitionExec"),
        "sort-merge join must repartition both sides, plan:\n{text}"
    );

    let expected = local_answer(&context, Arc::clone(&plan)).await;
    let got = cluster_answer(&session, plan).await;
    let expected_rows = sorted_row_keys(&expected);
    let got_rows = sorted_row_keys(&got);
    assert!(
        got_rows == expected_rows,
        "cluster rows {got_rows:?} != local rows {expected_rows:?}"
    );
}
