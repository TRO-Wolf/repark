use datafusion::arrow::array::{AsArray, BooleanArray};
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::{Analyzer, AnalyzerRule};

use super::super::*;
use super::common::*;

fn hof_ctx() -> (SessionContext, CatalogRegistry) {
    hof_ctx_with_ansi(true)
}

fn hof_ctx_with_ansi(ansi_enabled: bool) -> (SessionContext, CatalogRegistry) {
    let config = repark_functions::ansi::with_spark_ansi_config(
        datafusion::prelude::SessionConfig::new(),
        ansi_enabled,
    );
    let rules =
        repark_functions::analyzer_rules_with_higher_order_preparation(Analyzer::new().rules)
            .unwrap();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
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

#[tokio::test]
async fn higher_order_preparation_is_structural_identity_without_a_hof() {
    let ctx = SessionContext::new();
    let plan = ctx
        .state()
        .create_logical_plan("SELECT 1 + 2 AS r")
        .await
        .unwrap();
    let prepared = repark_functions::lambda_rebind::HigherOrderPreparation
        .analyze(plan.clone(), ctx.state().config_options())
        .unwrap();
    assert_eq!(prepared, plan);
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
async fn sql_door_indexed_transform_matches_spark_width_and_nullability() {
    for ansi_enabled in [true, false] {
        let (ctx, catalogs) = hof_ctx_with_ansi(ansi_enabled);
        let batch = collect_one(
            &ctx,
            &catalogs,
            "SELECT transform(make_array(1, 2, 3), (x, i) -> x + i) AS r",
        )
        .await;
        let schema = batch.schema();
        let field = schema.field(0);
        let DataType::List(element) = field.data_type() else {
            panic!("expected list, got {}", field.data_type());
        };
        assert_eq!(element.data_type(), &DataType::Int32);
        assert!(!field.is_nullable());
        assert!(!element.is_nullable());
        let batch = collect_one(
            &ctx,
            &catalogs,
            "SELECT transform(make_array(1, 2, 3), (x, i) -> x + CAST(i AS BIGINT)) AS r",
        )
        .await;
        let schema = batch.schema();
        let field = schema.field(0);
        let DataType::List(element) = field.data_type() else {
            panic!("expected list, got {}", field.data_type());
        };
        assert_eq!(element.data_type(), &DataType::Int64);
    }
}

#[tokio::test]
async fn sql_door_exists_nullability_follows_array_and_predicate() {
    for ansi_enabled in [true, false] {
        let (ctx, catalogs) = hof_ctx_with_ansi(ansi_enabled);
        let batch = collect_one(
            &ctx,
            &catalogs,
            "SELECT exists(make_array(1, 2, 3), x -> x > 2) AS r",
        )
        .await;
        assert_eq!(bool_column(&batch), vec![Some(true)]);
        assert!(!batch.schema().field(0).is_nullable());
        let batch = collect_one(
            &ctx,
            &catalogs,
            "SELECT exists(make_array(1, 2, 3), x -> CAST(NULL AS BOOLEAN)) AS r",
        )
        .await;
        assert_eq!(bool_column(&batch), vec![None]);
        assert!(batch.schema().field(0).is_nullable());
        let batch = collect_one(
            &ctx,
            &catalogs,
            "SELECT exists(CAST(NULL AS ARRAY<INT>), x -> x > 2) AS r",
        )
        .await;
        assert_eq!(bool_column(&batch), vec![None]);
        assert!(batch.schema().field(0).is_nullable());
    }
}

#[tokio::test]
async fn sql_door_short_lambdas_carry_sparks_arity_class() {
    let (ctx, catalogs) = hof_ctx();
    for (sql, expected) in [
        (
            "SELECT aggregate(make_array(1, 2, 3), 0, acc -> acc) AS r",
            "expects 1 arguments, but got 2",
        ),
        (
            "SELECT zip_with(make_array(1, 2), make_array(3, 4), x -> x) AS r",
            "expects 1 arguments, but got 2",
        ),
        (
            "SELECT transform_keys(map('a', 1), k -> k) AS r",
            "expects 1 arguments, but got 2",
        ),
        (
            "SELECT transform_values(map('a', 1), v -> v) AS r",
            "expects 1 arguments, but got 2",
        ),
        (
            "SELECT map_filter(map('a', 1), k -> true) AS r",
            "expects 1 arguments, but got 2",
        ),
        (
            "SELECT map_zip_with(map('a', 1), map('b', 2), (k, v) -> v) AS r",
            "expects 2 arguments, but got 3",
        ),
        (
            "SELECT map_zip_with(map('a', 1), map('b', 2), k -> k) AS r",
            "expects 1 arguments, but got 3",
        ),
    ] {
        let error = crate::execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a short lambda must refuse");
        let text = error.to_string();
        assert!(
            text.contains("INVALID_LAMBDA_FUNCTION_CALL.NUM_ARGS_MISMATCH")
                && text.contains(expected),
            "unexpected error for {sql}: {text}"
        );
    }
}

#[tokio::test]
async fn sql_door_aggregate_merge_must_match_the_init_type() {
    let (ctx, catalogs) = hof_ctx();
    for (sql, expected) in [
        (
            "SELECT aggregate(make_array(1, 2, 3), 0, (acc, x) -> 's') AS r",
            "The third parameter requires the \"INT\" type",
        ),
        (
            "SELECT aggregate(make_array(1, 2, 3), 0, (acc, x) -> CAST(acc AS BIGINT)) AS r",
            "however the merge lambda has the type \"BIGINT\"",
        ),
        (
            "SELECT aggregate(make_array(1, 2, 3), CAST(0 AS BIGINT), (acc, x) -> CAST(acc AS INT)) AS r",
            "The third parameter requires the \"BIGINT\" type",
        ),
    ] {
        let error = crate::execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a mistyped merge must refuse");
        let text = error.to_string();
        assert!(
            text.contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") && text.contains(expected),
            "unexpected error for {sql}: {text}"
        );
    }
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT aggregate(make_array(1, 2, 3), NULL, (acc, x) -> acc) AS r",
    )
    .await;
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).data_type(), &DataType::Null);
}

#[tokio::test]
async fn sql_door_index_elements_are_non_nullable() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform(make_array(1, 2, 3), (x, i) -> i) AS r",
    )
    .await;
    let field = batch.schema().field(0).clone();
    let DataType::List(element) = field.data_type() else {
        panic!("transform did not return a list: {}", field.data_type());
    };
    assert_eq!(element.data_type(), &DataType::Int32);
    assert!(!element.is_nullable(), "the index is never null");
}

#[tokio::test]
async fn queries_without_a_lambda_still_parse_with_the_session_dialect() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(&ctx, &catalogs, "SELECT count(\"v\") AS r FROM t").await;
    assert_eq!(int_values(batch.column(0).as_ref()), vec![Some(2)]);
}

#[tokio::test]
async fn sql_door_overlong_lambdas_divergence_df_plan_time_text() {
    let (ctx, catalogs) = hof_ctx();
    for (sql, expected) in [
        (
            "SELECT transform(make_array(1, 2), (x, i, z) -> x) AS r",
            "lambda defined 3 params but UDF support only 2",
        ),
        (
            "SELECT filter(make_array(1, 2), (a, b, c) -> true) AS r",
            "lambda defined 3 params but UDF support only 2",
        ),
        (
            "SELECT exists(make_array(1, 2), (x, i) -> x > 1) AS r",
            "lambda defined 2 params but UDF support only 1",
        ),
        (
            "SELECT forall(make_array(1, 2), (x, i) -> x > 1) AS r",
            "lambda defined 2 params but UDF support only 1",
        ),
        (
            "SELECT exists(make_array(1, 2), (a, b, c) -> true) AS r",
            "lambda defined 3 params but UDF support only 1",
        ),
        (
            "SELECT aggregate(make_array(1), 0, (a, b, c) -> a) AS r",
            "lambda defined 3 params but UDF support only 2",
        ),
        (
            "SELECT aggregate(make_array(1), 0, (acc, x) -> acc + x, (a, b) -> a) AS r",
            "lambda defined 2 params but UDF support only 1",
        ),
        (
            "SELECT zip_with(make_array(1), make_array(2), (a, b, c) -> a) AS r",
            "lambda defined 3 params but UDF support only 2",
        ),
        (
            "SELECT transform_keys(map('a', 1), (a, b, c) -> a) AS r",
            "lambda defined 3 params but UDF support only 2",
        ),
        (
            "SELECT transform_values(map('a', 1), (a, b, c) -> a) AS r",
            "lambda defined 3 params but UDF support only 2",
        ),
        (
            "SELECT map_filter(map('a', 1), (a, b, c) -> true) AS r",
            "lambda defined 3 params but UDF support only 2",
        ),
        (
            "SELECT map_zip_with(map('a', 1), map('b', 2), (a, b, c, d) -> a) AS r",
            "lambda defined 4 params but UDF support only 3",
        ),
    ] {
        let error = crate::execute(&ctx, &catalogs, sql)
            .await
            .expect_err("an overlong lambda must refuse");
        let text = error.to_string();
        assert!(
            text.contains(expected) && !text.contains("INVALID_LAMBDA_FUNCTION_CALL"),
            "unexpected error for {sql}: {text}"
        );
    }
}

#[tokio::test]
async fn sql_door_aggregate_fold_matches_the_oracle_rows() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT aggregate(make_array(1, 2, 3), CAST(0 AS BIGINT), (acc, x) -> acc + x) AS r",
    )
    .await;
    assert_eq!(int_values(batch.column(0).as_ref()), vec![Some(6)]);
    assert_eq!(batch.column(0).data_type(), &DataType::Int64);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT aggregate(make_array(1, 2, 3), CAST(0 AS INT), (acc, x) -> acc + coalesce(x, 0)) AS r",
    )
    .await;
    assert_eq!(int_values(batch.column(0).as_ref()), vec![Some(6)]);
    assert_eq!(batch.column(0).data_type(), &DataType::Int32);
}

#[tokio::test]
async fn sql_door_aggregate_provisional_initial_literals_match_spark_width() {
    let (ctx, catalogs) = hof_ctx();
    for (sql, expected) in [
        (
            "SELECT aggregate(CAST(NULL AS ARRAY<INT>), 0, (acc, x) -> acc + x) AS r",
            None,
        ),
        (
            "SELECT aggregate(CAST(array() AS ARRAY<INT>), 42, (acc, x) -> acc + x) AS r",
            Some(42),
        ),
    ] {
        let batch = collect_one(&ctx, &catalogs, sql).await;
        assert_eq!(int_values(batch.column(0).as_ref()), vec![expected]);
        assert_eq!(batch.column(0).data_type(), &DataType::Int32);
    }
}

#[tokio::test]
async fn sql_door_result_nullability_matches_the_oracle() {
    let (ctx, catalogs) = hof_ctx();
    let packed = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform(make_array(1, 2), (x, i) -> x) AS r",
    )
    .await;
    let plain = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform(make_array(1, 2), x -> x) AS r",
    )
    .await;
    let packed_schema = packed.schema();
    let DataType::List(packed_element) = packed_schema.field(0).data_type() else {
        panic!("transform did not return a list");
    };
    let plain_schema = plain.schema();
    let DataType::List(plain_element) = plain_schema.field(0).data_type() else {
        panic!("transform did not return a list");
    };
    assert_eq!(packed_element.data_type(), &DataType::Int32);
    assert_eq!(
        packed_element.is_nullable(),
        plain_element.is_nullable(),
        "packing keeps the element nullability it is given"
    );
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT zip_with(make_array(1, 2), make_array(10, 20), (x, y) -> x + y) AS r",
    )
    .await;
    let field = batch.schema().field(0).clone();
    assert!(!field.is_nullable(), "zip over non-null arrays is non-null");
    let DataType::List(element) = field.data_type() else {
        panic!("zip_with did not return a list: {}", field.data_type());
    };
    assert!(
        element.is_nullable(),
        "zip pads, so its elements are nullable"
    );
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT aggregate(make_array(v, CAST(1 AS INT)), CAST(0 AS INT), (acc, x) -> acc + coalesce(x, CAST(0 AS INT))) AS r FROM t",
    )
    .await;
    assert_eq!(
        int_values(batch.column(0).as_ref()),
        vec![Some(3), Some(1), Some(2)]
    );
    assert_eq!(batch.column(0).data_type(), &DataType::Int32);
    assert!(
        batch.schema().field(0).is_nullable(),
        "aggregate stays nullable"
    );
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT filter(make_array(1, 2, 3), x -> x > 1) AS r",
    )
    .await;
    let field = batch.schema().field(0).clone();
    assert!(
        !field.is_nullable(),
        "filter over non-null arrays is non-null"
    );
    let DataType::List(element) = field.data_type() else {
        panic!("filter did not return a list: {}", field.data_type());
    };
    assert!(
        !element.is_nullable(),
        "filter's Spark result elements are non-null"
    );
}

#[tokio::test]
async fn sql_door_map_result_nullability_matches_the_oracle() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform_keys(map('a', 1), (k, v) -> upper(k)) AS r",
    )
    .await;
    let field = batch.schema().field(0).clone();
    assert!(!field.is_nullable(), "map rewrites keep a non-null map");
    let input = collect_one(&ctx, &catalogs, "SELECT map('a', 1) AS r").await;
    let input_schema = input.schema();
    let DataType::Map(entries, _) = field.data_type() else {
        panic!("transform_keys did not return a map: {}", field.data_type());
    };
    let DataType::Struct(pair) = entries.data_type() else {
        panic!("a map holds key/value entries: {}", entries.data_type());
    };
    let DataType::Map(input_entries, _) = input_schema.field(0).data_type() else {
        panic!("map did not return a map");
    };
    let DataType::Struct(input_pair) = input_entries.data_type() else {
        panic!("a map holds key/value entries");
    };
    assert_eq!(
        pair[1].is_nullable(),
        input_pair[1].is_nullable(),
        "passed-through values keep theirs"
    );
}

#[tokio::test]
async fn sql_door_edge_rows_match_the_oracle() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT zip_with(make_array(1, 2, 3), make_array(10), (x, y) -> x + y) AS r",
    )
    .await;
    assert_eq!(list_column(&batch), vec![Some(vec![Some(11), None, None])]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform(CAST(NULL AS ARRAY<INT>), x -> x + 1) AS r",
    )
    .await;
    assert_eq!(list_column(&batch), vec![None]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT exists(CAST(make_array() AS ARRAY<INT>), x -> x > 1) AS r",
    )
    .await;
    assert_eq!(bool_column(&batch), vec![Some(false)]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT exists(make_array(1, NULL, 3), x -> x > 5) AS r",
    )
    .await;
    assert_eq!(bool_column(&batch), vec![None]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT forall(make_array(1, NULL, 3), x -> x > 0) AS r",
    )
    .await;
    assert_eq!(bool_column(&batch), vec![None]);
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT exists(CAST(NULL AS ARRAY<INT>), x -> x > 1) AS e, forall(CAST(NULL AS ARRAY<INT>), x -> x > 1) AS f",
    )
    .await;
    assert_eq!(bool_column(&batch), vec![None]);
    let flags = batch
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean column");
    assert!(flags.is_null(0));
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT map_filter(map('a', 1, 'b', NULL), (k, v) -> v IS NOT NULL) AS r",
    )
    .await;
    let maps = batch.column(0).as_map();
    assert_eq!(maps.keys().len(), 1);
    assert_eq!(maps.keys().as_string::<i32>().value(0), "a");
}

#[tokio::test]
async fn sql_door_null_lambda_keys_carry_sparks_null_map_key() {
    let (ctx, catalogs) = hof_ctx();
    let error = crate::execute(
        &ctx,
        &catalogs,
        "SELECT transform_keys(map('a', 1), (k, v) -> NULL) AS r",
    )
    .await
    .expect("a null key plans")
    .collect()
    .await
    .expect_err("a null key must refuse");
    assert!(
        error.to_string().contains("[NULL_MAP_KEY]"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn sql_door_null_lambda_values_make_a_void_map() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform_values(map('a', 1), (k, v) -> NULL) AS r",
    )
    .await;
    let field = batch.schema().field(0).clone();
    let DataType::Map(entries, _) = field.data_type() else {
        panic!(
            "transform_values did not return a map: {}",
            field.data_type()
        );
    };
    let DataType::Struct(pair) = entries.data_type() else {
        panic!("a map holds key/value entries: {}", entries.data_type());
    };
    assert_eq!(pair[1].data_type(), &DataType::Null);
    let maps = batch.column(0).as_map();
    assert_eq!(maps.keys().len(), 1);
    assert_eq!(maps.values().data_type(), &DataType::Null);
}

#[tokio::test]
async fn sql_door_higher_order_over_values_keeps_narrowed_int32() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT transform(make_array(a, b), x -> x + 1) AS r FROM (VALUES (1, 2), (3, 4)) AS t(a, b)",
    )
    .await;
    assert_eq!(batch.num_rows(), 2);
    let field = batch.schema().field(0).clone();
    let DataType::List(element) = field.data_type() else {
        panic!("transform did not return a list: {}", field.data_type());
    };
    assert_eq!(element.data_type(), &DataType::Int32);
    assert_eq!(
        list_column(&batch),
        vec![Some(vec![Some(2), Some(3)]), Some(vec![Some(4), Some(5)])]
    );
}

#[tokio::test]
async fn sql_door_lambda_body_overflow_divergence_wraps() {
    let (ctx, catalogs) = hof_ctx();
    let batch = collect_one(
        &ctx,
        &catalogs,
        "SELECT aggregate(make_array(2000000000, 2000000000), 0, (acc, x) -> acc + x) AS r",
    )
    .await;
    assert_eq!(
        int_values(batch.column(0).as_ref()),
        vec![Some(-294_967_296)]
    );
}
