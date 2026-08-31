//! Isolated Databricks-dialect SQL pins for higher-order kernel values.

use datafusion::arrow::array::{Array, AsArray, Int32Array, Int64Array};
use datafusion::prelude::{SessionConfig, SessionContext};

fn hof_context() -> SessionContext {
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.dialect = datafusion::config::Dialect::Databricks;
    let ctx = SessionContext::new_with_config(config);
    crate::register_all(&ctx);
    ctx
}

async fn collect(ctx: &SessionContext, sql: &str) -> datafusion::arrow::array::RecordBatch {
    let batches = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("exec {sql}: {error}"));
    assert_eq!(batches.len(), 1, "{sql}");
    batches.into_iter().next().expect("one batch")
}

fn list_i64(column: &dyn Array) -> Vec<Option<Vec<Option<i64>>>> {
    let lists = column.as_list::<i32>();
    (0..lists.len())
        .map(|row| {
            if lists.is_null(row) {
                return None;
            }
            let values = lists.value(row);
            Some(int_values(values.as_ref()))
        })
        .collect()
}

fn int_values(values: &dyn Array) -> Vec<Option<i64>> {
    if let Some(ints) = values.as_any().downcast_ref::<Int64Array>() {
        return (0..ints.len())
            .map(|index| ints.is_valid(index).then(|| ints.value(index)))
            .collect();
    }
    let ints = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 or int64 list values");
    (0..ints.len())
        .map(|index| ints.is_valid(index).then(|| i64::from(ints.value(index))))
        .collect()
}

/// pins: fnp-4c-higher-order-kernels/C-001
#[tokio::test]
async fn transform_index_is_zero_based() {
    let ctx = hof_context();
    let batch = collect(
        &ctx,
        "SELECT transform(make_array(10, 20, 30), (x, i) -> x + i)",
    )
    .await;
    assert_eq!(
        list_i64(batch.column(0)),
        vec![Some(vec![Some(10), Some(21), Some(32)])]
    );
}

/// pins: fnp-4c-higher-order-kernels/C-003
#[tokio::test]
async fn aggregate_mixed_width_init_and_element_reaches_a_type_fixpoint() {
    let ctx = hof_context();
    let batch = collect(
        &ctx,
        "SELECT aggregate(make_array(1, 2, 3), CAST(0 AS INT), \
         (acc, x) -> acc + coalesce(x, 0))",
    )
    .await;
    let values = int_values(batch.column(0).as_ref());
    assert_eq!(values, vec![Some(6)]);
    assert_eq!(
        batch.column(0).data_type(),
        &datafusion::arrow::datatypes::DataType::Int64
    );
}

/// pins: fnp-4c-higher-order-kernels/C-003
#[tokio::test]
async fn aggregate_applies_the_finish_lambda() {
    let ctx = hof_context();
    let batch = collect(
        &ctx,
        "SELECT aggregate(make_array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10)",
    )
    .await;
    let values = int_values(batch.column(0).as_ref());
    assert_eq!(values, vec![Some(60)]);
}

/// pins: fnp-4c-higher-order-kernels/C-006
#[tokio::test]
async fn zip_with_null_pads_the_shorter_array() {
    let ctx = hof_context();
    let batch = collect(
        &ctx,
        "SELECT zip_with(make_array(1, 3, 5), make_array(0, 2), (x, y) -> x + coalesce(y, 0))",
    )
    .await;
    assert_eq!(
        list_i64(batch.column(0)),
        vec![Some(vec![Some(1), Some(5), Some(5)])]
    );
}

/// pins: fnp-4c-higher-order-kernels/C-009
#[tokio::test]
async fn map_filter_drops_entries_whose_predicate_is_not_true() {
    let ctx = hof_context();
    let batch = collect(
        &ctx,
        "SELECT map_filter(map('foo', 1, 'bar', 2), (k, v) -> k IS NOT NULL AND v > 1)",
    )
    .await;
    let maps = batch.column(0).as_map();
    assert_eq!(maps.keys().len(), 1);
    assert_eq!(maps.keys().as_string::<i32>().value(0), "bar");
    assert_eq!(int_values(maps.values().as_ref()), vec![Some(2)]);
}

/// pins: fnp-4c-higher-order-kernels/C-007
#[tokio::test]
async fn transform_keys_duplicate_key_raises() {
    let ctx = hof_context();
    let error = ctx
        .sql("SELECT transform_keys(map('foo', 1, 'bar', 2), (k, v) -> 'same')")
        .await
        .unwrap_or_else(|error| panic!("plan: {error}"))
        .collect()
        .await
        .expect_err("duplicate produced keys must raise");
    let text = error.to_string();
    assert!(
        text.contains("DUPLICATED_MAP_KEY"),
        "unexpected error: {text}"
    );
}
