use super::super::*;
use arrow::array::{Int32Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, SortExpr};

fn key_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

fn shuffled_batches(count: i32, rows_per_batch: i32) -> Vec<RecordBatch> {
    let schema = key_schema();
    let total = count * rows_per_batch;
    (0..count)
        .map(|batch| {
            let values: Vec<i32> = (0..rows_per_batch)
                .map(|row| (total - 1) - (batch * rows_per_batch + row))
                .collect();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])
                .expect("shuffled fixture batch")
        })
        .collect()
}

fn ascending_batches(count: i32, rows_per_batch: i32) -> Vec<RecordBatch> {
    let schema = key_schema();
    (0..count)
        .map(|batch| {
            let values: Vec<i32> = (0..rows_per_batch)
                .map(|row| batch * rows_per_batch + row)
                .collect();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])
                .expect("ascending fixture batch")
        })
        .collect()
}

async fn materialized_shape(
    session: &ReparkSession,
    name: &str,
) -> (Vec<RecordBatch>, Vec<Vec<SortExpr>>) {
    let reference = session.temp_view_ref(name).expect("temp view resolves");
    let provider = session
        .context()
        .table_provider(reference)
        .await
        .expect("registered provider");
    let provider_any: &dyn std::any::Any = provider.as_ref();
    let table = provider_any
        .downcast_ref::<MemTable>()
        .expect("cache view is a MemTable");
    let batches = table
        .batches
        .first()
        .expect("MemTable keeps one partition")
        .read()
        .await
        .clone();
    let order = table.sort_order.lock().clone();
    (batches, order)
}

fn sole_key(order: &[Vec<SortExpr>]) -> &SortExpr {
    assert_eq!(order.len(), 1, "one declared ordering");
    assert_eq!(order[0].len(), 1, "one declared key");
    &order[0][0]
}

fn batch_values(batch: &RecordBatch) -> Vec<i32> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 key column")
        .values()
        .to_vec()
}

#[tokio::test]
async fn sorted_cache_registers_one_batch_with_declared_order() {
    let session = ReparkSession::new().expect("session");
    let batches = shuffled_batches(3, 9_000);
    session
        .register_record_batches_as_temp_view("src", batches[0].schema(), batches)
        .expect("source view");
    let frame = session
        .sql("SELECT id FROM src ORDER BY id")
        .await
        .expect("sorted plan");
    session
        .materialize_dataframe_as_cache_view("cached", frame, (None, None))
        .await
        .expect("cache materialize");
    let (stored, order) = materialized_shape(&session, "cached").await;
    assert_eq!(stored.len(), 1, "sorted cache concats to one batch");
    let values = batch_values(&stored[0]);
    assert_eq!(values.len(), 27_000);
    assert!(
        values.windows(2).all(|pair| pair[0] <= pair[1]),
        "stored batch is sorted"
    );
    let key = sole_key(&order);
    assert_eq!(key.expr, Expr::Column(Column::from_name("id")));
    assert!(key.asc);
    assert!(!key.nulls_first);
}

#[tokio::test]
async fn descending_nulls_last_cache_declares_exact_options() {
    let session = ReparkSession::new().expect("session");
    let batches = shuffled_batches(3, 9_000);
    session
        .register_record_batches_as_temp_view("src", batches[0].schema(), batches)
        .expect("source view");
    let frame = session
        .sql("SELECT id FROM src ORDER BY id DESC NULLS LAST")
        .await
        .expect("sorted plan");
    session
        .materialize_dataframe_as_cache_view("cached", frame, (None, None))
        .await
        .expect("cache materialize");
    let (stored, order) = materialized_shape(&session, "cached").await;
    assert_eq!(stored.len(), 1, "sorted cache concats to one batch");
    let values = batch_values(&stored[0]);
    assert!(
        values.windows(2).all(|pair| pair[0] >= pair[1]),
        "stored batch is descending"
    );
    let key = sole_key(&order);
    assert_eq!(key.expr, Expr::Column(Column::from_name("id")));
    assert!(!key.asc);
    assert!(!key.nulls_first);
}

#[tokio::test]
async fn unsorted_cache_is_unchanged() {
    let session = ReparkSession::new().expect("session");
    let batches = shuffled_batches(3, 100);
    session
        .register_record_batches_as_temp_view("src", batches[0].schema(), batches)
        .expect("source view");
    let frame = session.sql("SELECT id FROM src").await.expect("plain plan");
    session
        .materialize_dataframe_as_cache_view("cached", frame, (None, None))
        .await
        .expect("cache materialize");
    let (stored, order) = materialized_shape(&session, "cached").await;
    assert_eq!(stored.len(), 3, "unsorted cache keeps its batch count");
    assert!(order.is_empty(), "unsorted cache declares no order");
}

#[tokio::test]
async fn declared_multibatch_source_concats_to_one_batch_with_order() {
    let session = ReparkSession::new().expect("session");
    let batches = ascending_batches(4, 500);
    session
        .register_record_batches_as_temp_view("src", batches[0].schema(), batches)
        .expect("source view");
    session
        .declare_temp_view_sorted("src", &["id".to_string()], false)
        .await
        .expect("source declares sorted");
    let frame = session.sql("SELECT id FROM src").await.expect("plain plan");
    session
        .materialize_dataframe_as_cache_view("cached", frame, (None, None))
        .await
        .expect("cache materialize");
    let (stored, order) = materialized_shape(&session, "cached").await;
    assert_eq!(stored.len(), 1, "declared cache concats to one batch");
    let values = batch_values(&stored[0]);
    assert_eq!(values.len(), 2_000);
    assert!(
        values.windows(2).all(|pair| pair[0] <= pair[1]),
        "stored batch is sorted"
    );
    let key = sole_key(&order);
    assert_eq!(key.expr, Expr::Column(Column::from_name("id")));
    assert!(key.asc);
    assert!(!key.nulls_first);
}

#[tokio::test]
async fn sorted_cache_over_budget_keeps_todays_path() {
    let session = ReparkSession::new().expect("session");
    let batches = ascending_batches(4, 500);
    let stream_total: u64 = batches
        .iter()
        .map(|batch| u64::try_from(batch.get_array_memory_size()).expect("byte size fits"))
        .sum();
    session
        .register_record_batches_as_temp_view("src", batches[0].schema(), batches)
        .expect("source view");
    session
        .declare_temp_view_sorted("src", &["id".to_string()], false)
        .await
        .expect("source declares sorted");
    let frame = session.sql("SELECT id FROM src").await.expect("plain plan");
    session
        .materialize_dataframe_as_cache_view("full", frame, (None, None))
        .await
        .expect("cache materialize");
    let (stored, _) = materialized_shape(&session, "full").await;
    assert_eq!(stored.len(), 1, "unbudgeted sorted cache concats");
    let frame = session.sql("SELECT id FROM src").await.expect("plain plan");
    session
        .materialize_dataframe_as_cache_view("guarded", frame, (Some(stream_total), None))
        .await
        .expect("exact budget still materializes");
    let (stored, order) = materialized_shape(&session, "guarded").await;
    assert_eq!(stored.len(), 4, "guarded cache keeps its split batches");
    assert!(order.is_empty(), "guarded cache declares no order");
}

#[tokio::test]
async fn sort_filter_cache_multi_partition_not_declared() {
    let session = ReparkSession::builder()
        .target_partitions(16)
        .build()
        .expect("session");
    let batches = shuffled_batches(4, 50_000);
    session
        .register_record_batches_as_temp_view("src", batches[0].schema(), batches)
        .expect("source view");
    let frame = session
        .sql("SELECT id FROM (SELECT id FROM src ORDER BY id) WHERE id > 0")
        .await
        .expect("sort filter plan");
    session
        .materialize_dataframe_as_cache_view("cached", frame, (None, None))
        .await
        .expect("cache materialize");
    let (stored, order) = materialized_shape(&session, "cached").await;
    assert!(
        stored.len() > 1,
        "multi-partition cache keeps split batches"
    );
    assert!(order.is_empty(), "multi-partition cache declares no order");
    let ranked = session
        .sql("SELECT id, ROW_NUMBER() OVER (ORDER BY id ASC) AS rank FROM cached")
        .await
        .expect("window plan");
    let collected = ranked.collect().await.expect("window collect");
    let mut pairs = Vec::new();
    for batch in &collected {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column");
        let ranks = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("rank column");
        pairs.extend(
            ids.values()
                .iter()
                .copied()
                .zip(ranks.values().iter().copied()),
        );
    }
    assert_eq!(pairs.len(), 199_999);
    pairs.sort_by_key(|pair| pair.0);
    assert!(
        pairs.windows(2).all(|pair| pair[0].1 < pair[1].1),
        "ranks follow key order"
    );
}

#[tokio::test]
async fn sorted_but_empty_cache_registers_todays_path() {
    let session = ReparkSession::new().expect("session");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let empty = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
    )
    .expect("empty fixture batch");
    session
        .register_record_batches_as_temp_view("src", schema, vec![empty])
        .expect("source view");
    let frame = session
        .sql("SELECT id FROM src ORDER BY id")
        .await
        .expect("sorted plan");
    session
        .materialize_dataframe_as_cache_view("cached", frame, (None, None))
        .await
        .expect("cache materialize");
    let (stored, _) = materialized_shape(&session, "cached").await;
    assert_eq!(
        stored.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "empty cache stores no rows"
    );
}
