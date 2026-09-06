use datafusion::arrow::array::{AsArray, BooleanArray};

use super::super::*;
use super::common::*;

fn hof_ctx() -> (SessionContext, CatalogRegistry) {
    let ctx = SessionContext::new();
    repark_functions::register_all(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![Some(2), None, Some(1)]))],
    )
    .unwrap();
    ctx.register_batch("t", batch).unwrap();
    (ctx, CatalogRegistry::new())
}

async fn collect_one(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> RecordBatch {
    let batches = crate::execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1, "{sql}");
    batches.into_iter().next().unwrap()
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
        .expect("int32 or int64 values");
    (0..ints.len())
        .map(|index| ints.is_valid(index).then(|| i64::from(ints.value(index))))
        .collect()
}

fn list_column(batch: &RecordBatch) -> Vec<Option<Vec<Option<i64>>>> {
    let lists = batch.column(0).as_list::<i32>();
    (0..lists.len())
        .map(|row| {
            if lists.is_null(row) {
                return None;
            }
            Some(int_values(lists.value(row).as_ref()))
        })
        .collect()
}

fn bool_column(batch: &RecordBatch) -> Vec<Option<bool>> {
    let flags = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean column");
    (0..flags.len())
        .map(|row| flags.is_valid(row).then(|| flags.value(row)))
        .collect()
}

#[tokio::test]
async fn sql_door_transform_answers_both_arities() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform(make_array(1, 2, 3), x -> x + 1) AS r",
    )
    .await;
    assert_eq!(
        list_column(&batch),
        vec![Some(vec![Some(2), Some(3), Some(4)])]
    );
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform(make_array(10, 20, 30), (x, i) -> x + i) AS r",
    )
    .await;
    assert_eq!(
        list_column(&batch),
        vec![Some(vec![Some(10), Some(21), Some(32)])]
    );
}

#[tokio::test]
async fn sql_door_filter_drops_null_predicates() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT filter(make_array(1, 2, 3), x -> x > 1) AS r",
    )
    .await;
    assert_eq!(list_column(&batch), vec![Some(vec![Some(2), Some(3)])]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT filter(make_array(1, 2, 3), (x, i) -> i % 2 = 0) AS r",
    )
    .await;
    assert_eq!(list_column(&batch), vec![Some(vec![Some(1), Some(3)])]);
}

#[tokio::test]
async fn sql_door_exists_and_forall_answer_three_valued() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT exists(make_array(1, 2, 3), x -> x > 2) AS r",
    )
    .await;
    assert_eq!(bool_column(&batch), vec![Some(true)]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT forall(make_array(1, 2, 3), x -> x > 0) AS r",
    )
    .await;
    assert_eq!(bool_column(&batch), vec![Some(true)]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT forall(CAST(make_array() AS ARRAY<INT>), x -> x > 0) AS r",
    )
    .await;
    assert_eq!(bool_column(&batch), vec![Some(true)]);
}

#[tokio::test]
async fn sql_door_aggregate_and_reduce_fold_with_optional_finish() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT aggregate(make_array(1, 2, 3), 0, (acc, x) -> acc + x) AS r",
    )
    .await;
    assert_eq!(int_values(batch.column(0).as_ref()), vec![Some(6)]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT aggregate(make_array(1, 2, 3), 0, (acc, x) -> acc + x, acc -> acc * 10) AS r",
    )
    .await;
    assert_eq!(int_values(batch.column(0).as_ref()), vec![Some(60)]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT reduce(make_array(1, 2, 3), 0, (acc, x) -> acc + x) AS r",
    )
    .await;
    assert_eq!(int_values(batch.column(0).as_ref()), vec![Some(6)]);
}

#[tokio::test]
async fn sql_door_zip_with_null_pads_the_shorter_array() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT zip_with(make_array(1, 3, 5), make_array(0, 2), (x, y) -> x + coalesce(y, 0)) AS r",
    )
    .await;
    assert_eq!(
        list_column(&batch),
        vec![Some(vec![Some(1), Some(5), Some(5)])]
    );
}

#[tokio::test]
async fn sql_door_map_family_rewrites_filters_and_zips() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform_keys(map('foo', 1, 'bar', 2), (k, v) -> upper(k)) AS r",
    )
    .await;
    let maps = batch.column(0).as_map();
    assert_eq!(maps.keys().len(), 2);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform_values(map('foo', 1, 'bar', 2), (k, v) -> v + 1) AS r",
    )
    .await;
    let maps = batch.column(0).as_map();
    assert_eq!(int_values(maps.values().as_ref()), vec![Some(2), Some(3)]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT map_filter(map('foo', 1, 'bar', 2), (k, v) -> v > 1) AS r",
    )
    .await;
    let maps = batch.column(0).as_map();
    assert_eq!(maps.keys().len(), 1);
    assert_eq!(maps.keys().as_string::<i32>().value(0), "bar");
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT map_zip_with(map('foo', 1, 'bar', 2), map('foo', 10, 'baz', 3), (k, v1, v2) -> coalesce(v1, 0) + coalesce(v2, 0)) AS r",
    )
    .await;
    let maps = batch.column(0).as_map();
    assert_eq!(maps.keys().len(), 3);
    assert_eq!(
        int_values(maps.values().as_ref()),
        vec![Some(11), Some(2), Some(3)]
    );
}

#[tokio::test]
async fn sql_door_transform_keys_duplicate_key_raises() {
    let (ctx, catalogs) = hof_ctx();
    let error = crate::execute(
        &ctx,
        &catalogs,
        "SELECT transform_keys(map('foo', 1, 'bar', 2), (k, v) -> 'same') AS r",
    )
    .await
    .unwrap()
    .collect()
    .await
    .expect_err("duplicate produced keys must raise");
    assert!(
        error.to_string().contains("DUPLICATED_MAP_KEY"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn sql_door_lambda_results_keep_narrowed_int32() {
    let (ctx, catalogs) = hof_ctx();
    for sql in [
        "SELECT transform(make_array(1, 2, 3), x -> x + 1) AS r",
        "SELECT filter(make_array(1, 2, 3), x -> x > 1) AS r",
        "SELECT zip_with(make_array(1, 2), make_array(3, 4), (x, y) -> x + y) AS r",
    ] {
        let batch = collect_one(&ctx, &catalogs, sql).await;
        let field = batch.schema().field(0).clone();
        let DataType::List(element) = field.data_type() else {
            panic!("{sql} did not return a list: {}", field.data_type());
        };
        assert_eq!(element.data_type(), &DataType::Int32, "{sql}");
    }
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT aggregate(make_array(1, 2, 3), 0, (acc, x) -> acc + x) AS r",
    )
    .await;
    assert_eq!(
        batch.schema().field(0).data_type(),
        &DataType::Int32,
        "aggregate over int32 must stay int32"
    );
}

#[tokio::test]
async fn queries_without_a_lambda_still_parse_with_the_session_dialect() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(&ctx, &catalogs, "SELECT count(\"v\") AS r FROM t").await;
    assert_eq!(int_values(batch.column(0).as_ref()), vec![Some(2)]);
}
