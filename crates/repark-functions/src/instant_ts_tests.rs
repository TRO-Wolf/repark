use std::sync::Arc;

use super::*;
use datafusion::arrow::array::{
    ArrayRef, AsArray, Int32Array, RecordBatch, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema, TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::logical_expr::{Cast, Expr};
use datafusion::prelude::SessionContext;

fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

#[tokio::test]
async fn now_and_current_timestamp_are_microsecond_utc_non_null() {
    let ctx = ctx();
    for sql in ["SELECT now() AS ts", "SELECT current_timestamp() AS ts"] {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        let schema = batches[0].schema();
        let field = schema.field(0);
        assert_eq!(field.data_type(), &ltz_timestamp_type(), "{sql}: type");
        assert!(!field.is_nullable(), "{sql}: Spark marks now() not-null");
    }
}

#[tokio::test]
async fn to_timestamp_of_a_zone_suffixed_string_is_microsecond_utc() {
    let ctx = ctx();
    let sql = "SELECT to_timestamp('2024-03-10T01:30:00-05:00') AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type(), &ltz_timestamp_type());
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, 1_710_052_200_000_000);
}

#[tokio::test]
async fn zoneless_to_timestamp_under_utc_session_keeps_wall_as_utc() {
    let ctx = ctx();
    let sql = "SELECT to_timestamp('2024-06-15 12:00:00') AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ltz_timestamp_type()
    );
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, 1_718_452_800_000_000);
}

fn ctx_at(zone: &str) -> SessionContext {
    use datafusion::prelude::SessionConfig;
    let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
    let ctx = SessionContext::new_with_config(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

#[tokio::test]
async fn zoneless_inputs_localize_in_the_session_zone() {
    let ctx = ctx_at("America/New_York");
    let expected = 1_718_467_200_000_000_i64;
    for sql in [
        "SELECT to_timestamp('2024-06-15 12:00:00') AS ts",
        "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
    ] {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ltz_timestamp_type(),
            "{sql}"
        );
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ticks, expected, "{sql}");
    }
}

#[tokio::test]
async fn zone_suffixed_to_timestamp_is_not_localized_again() {
    let ctx = ctx_at("America/New_York");
    let sql = "SELECT to_timestamp('2024-06-15T12:00:00Z') AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, 1_718_452_800_000_000);
}

#[tokio::test]
async fn integer_cast_to_timestamp_is_microsecond_utc_seconds() {
    let ctx = ctx();
    let sql = "SELECT CAST(CAST(-1800 AS BIGINT) AS TIMESTAMP) AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).data_type(), &ltz_timestamp_type());
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, -1_800_000_000);
}

fn ctx_ntz_at(zone: &str) -> SessionContext {
    use datafusion::prelude::SessionConfig;
    let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
    let config = crate::timestamp_type::with_spark_timestamp_type(
        config,
        crate::timestamp_type::SparkTimestampType::Ntz,
    );
    let ctx = SessionContext::new_with_config(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

#[tokio::test]
async fn ntz_opt_in_bare_timestamp_is_naive_microseconds() {
    let ctx = ctx_ntz_at("America/New_York");
    let expected = 1_718_452_800_000_000_i64;
    for sql in [
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
        "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
    ] {
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ntz_timestamp_type(),
            "{sql}"
        );
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ticks, expected, "{sql}");
    }
}

#[tokio::test]
async fn ntz_opt_in_does_not_retarget_to_timestamp_or_now() {
    let ctx = ctx_ntz_at("UTC");
    let batches = ctx
        .sql("SELECT to_timestamp('2024-06-15 12:00:00') AS ts")
        .await
        .expect("to_timestamp")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ltz_timestamp_type(),
        "to_timestamp stays LTZ; the knob is the SQL type name TIMESTAMP"
    );
    let batches = ctx
        .sql("SELECT current_timestamp() AS ts")
        .await
        .expect("current_timestamp")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ltz_timestamp_type(),
        "current_timestamp is an instant, not the SQL type name"
    );
}

#[tokio::test]
async fn ntz_opt_in_integer_cast_is_naive_microseconds_seconds() {
    let ctx = ctx_ntz_at("UTC");
    let sql = "SELECT CAST(CAST(-1800 AS BIGINT) AS TIMESTAMP) AS ts";
    let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &ntz_timestamp_type()
    );
    let ticks = batches[0]
        .column(0)
        .as_primitive::<TimestampMicrosecondType>()
        .value(0);
    assert_eq!(ticks, -1_800_000_000);
}

fn naive_us() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn ltz_us() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn ns_naive() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn struct_type(children: &[(&str, DataType)]) -> DataType {
    DataType::Struct(
        children
            .iter()
            .map(|(name, data_type)| Arc::new(Field::new(*name, data_type.clone(), true)))
            .collect(),
    )
}

fn list_type(inner: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", inner, true)))
}

fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", key, false),
                Field::new("value", value, true),
            ])),
            false,
        )),
        false,
    )
}

fn apply(source: DataType, target: DataType) -> (Expr, Expr) {
    let schema =
        DFSchema::try_from(Schema::new(vec![Field::new("st", source, true)])).expect("schema");
    let expr = Expr::Cast(Cast::new_from_field(
        Box::new(Expr::Column(Column::new_unqualified("st"))),
        Arc::new(Field::new("out", target, true)),
    ));
    let rewritten = super::rewrite_cast(
        expr.clone(),
        &schema,
        "America/New_York",
        crate::timestamp_type::SparkTimestampType::Ltz,
        true,
    );
    (expr, rewritten.data)
}

fn selected_paths(expr: &Expr) -> String {
    let Expr::Cast(cast) = expr else {
        panic!("outer cast, got {expr}");
    };
    let Expr::ScalarFunction(function) = cast.expr.as_ref() else {
        panic!("kernel call, got {}", cast.expr);
    };
    assert_eq!(
        function.name(),
        crate::nested_wall::LOCALIZE_NESTED_NTZ_NAME
    );
    let Expr::Literal(ScalarValue::Utf8(Some(paths)), _) = &function.args[1] else {
        panic!("path literal, got {:?}", function.args.get(1));
    };
    paths.clone()
}

#[test]
fn a_struct_cast_with_a_naive_leaf_is_rewritten() {
    let (original, rewritten) = apply(
        struct_type(&[("v", naive_us()), ("n", DataType::Int32)]),
        struct_type(&[("v", ltz_us()), ("n", DataType::Int32)]),
    );
    assert_ne!(rewritten, original);
    assert_eq!(selected_paths(&rewritten), "v");
    let (deeper, nested) = apply(
        struct_type(&[("a", struct_type(&[("b", naive_us())]))]),
        struct_type(&[("a", struct_type(&[("b", ltz_us())]))]),
    );
    assert_ne!(nested, deeper);
    assert_eq!(selected_paths(&nested), "a.b");
    let (_, both) = apply(
        struct_type(&[("v", naive_us()), ("w", naive_us())]),
        struct_type(&[("v", ltz_us()), ("w", ltz_us())]),
    );
    assert_eq!(selected_paths(&both), "v,w");
}

#[test]
fn a_struct_cast_without_a_naive_to_instant_leaf_is_unchanged() {
    let source = struct_type(&[("v", ltz_us()), ("n", DataType::Int32)]);
    let (original, rewritten) = apply(source.clone(), source);
    assert_eq!(rewritten, original);
}

#[test]
fn nanosecond_list_and_map_leaves_are_unchanged() {
    let cases = [
        struct_type(&[("v", ns_naive())]),
        struct_type(&[("xs", list_type(naive_us()))]),
        struct_type(&[("m", map_type(DataType::Utf8, naive_us()))]),
    ];
    let targets = [
        struct_type(&[("v", ltz_us())]),
        struct_type(&[("xs", list_type(ltz_us()))]),
        struct_type(&[("m", map_type(DataType::Utf8, ltz_us()))]),
    ];
    for (source, target) in cases.into_iter().zip(targets) {
        let (original, rewritten) = apply(source, target);
        assert_eq!(rewritten, original);
    }
    let (_, mixed) = apply(
        struct_type(&[
            ("ns", ns_naive()),
            ("v", naive_us()),
            ("xs", list_type(naive_us())),
            ("m", map_type(DataType::Utf8, naive_us())),
        ]),
        struct_type(&[
            ("ns", ltz_us()),
            ("v", ltz_us()),
            ("xs", list_type(ltz_us())),
            ("m", map_type(DataType::Utf8, ltz_us())),
        ]),
    );
    assert_eq!(selected_paths(&mixed), "v");
}

#[test]
fn a_naive_microsecond_source_against_a_non_ltz_target_is_not_selected() {
    for target in [ns_naive(), DataType::Utf8, DataType::Date32] {
        let (original, rewritten) = apply(
            struct_type(&[("v", naive_us())]),
            struct_type(&[("v", target.clone())]),
        );
        assert_eq!(rewritten, original, "{target}");
    }
    let (_, rewritten) = apply(
        struct_type(&[("v", naive_us()), ("w", naive_us())]),
        struct_type(&[("v", DataType::Utf8), ("w", ltz_us())]),
    );
    assert_eq!(selected_paths(&rewritten), "w");
}

#[test]
fn a_member_name_containing_a_dot_or_a_comma_is_not_selected() {
    for name in ["a.b", "a,b"] {
        let (original, rewritten) = apply(
            struct_type(&[(name, naive_us())]),
            struct_type(&[(name, ltz_us())]),
        );
        assert_eq!(rewritten, original, "{name}");
    }
    let (_, rewritten) = apply(
        struct_type(&[("a.b", naive_us()), ("v", naive_us())]),
        struct_type(&[("a.b", ltz_us()), ("v", ltz_us())]),
    );
    assert_eq!(selected_paths(&rewritten), "v");
}

#[test]
fn a_top_level_naive_column_still_becomes_to_timestamp() {
    let schema =
        DFSchema::try_from(Schema::new(vec![Field::new("v", naive_us(), true)])).expect("schema");
    let expr = Expr::Cast(Cast::new_from_field(
        Box::new(Expr::Column(Column::new_unqualified("v"))),
        Arc::new(Field::new("out", ltz_us(), true)),
    ));
    let rewritten = super::rewrite_cast(
        expr,
        &schema,
        "America/New_York",
        crate::timestamp_type::SparkTimestampType::Ltz,
        true,
    );
    let text = rewritten.data.to_string();
    assert!(text.contains("to_timestamp"), "{text}");
    assert!(
        !text.contains(crate::nested_wall::LOCALIZE_NESTED_NTZ_NAME),
        "{text}"
    );
}

fn wall_stamps(zoned: bool) -> TimestampMicrosecondArray {
    let array = TimestampMicrosecondArray::from(vec![Some(1_767_323_045_123_456_i64)]);
    if zoned {
        array.with_timezone("UTC")
    } else {
        array
    }
}

fn relation(zoned: bool) -> RecordBatch {
    let nested_stamps = wall_stamps(zoned);
    let top_stamps = wall_stamps(zoned);
    let nested = StructArray::try_new(
        vec![
            Arc::new(Field::new("v", nested_stamps.data_type().clone(), true)),
            Arc::new(Field::new("n", DataType::Int32, true)),
        ]
        .into(),
        vec![
            Arc::new(nested_stamps) as ArrayRef,
            Arc::new(Int32Array::from(vec![7_i32])),
        ],
        None,
    )
    .expect("struct");
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("st", nested.data_type().clone(), true),
            Field::new("v", top_stamps.data_type().clone(), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1_i32])),
            Arc::new(nested),
            Arc::new(top_stamps),
        ],
    )
    .expect("batch")
}

async fn plan_text(ctx: &SessionContext, sql: &str) -> String {
    let frame = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let state = ctx.state();
    let analyzed = crate::analyze_eagerly(&state, frame.logical_plan().clone())
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    analyzed.display_indent().to_string()
}

#[tokio::test]
async fn case_coalesce_and_union_are_rewritten_and_the_top_level_stays_to_timestamp() {
    let ctx = ctx_at("America/New_York");
    ctx.register_batch("a", relation(false)).expect("a");
    ctx.register_batch("b", relation(true)).expect("b");
    let kernel = crate::nested_wall::LOCALIZE_NESTED_NTZ_NAME;
    for sql in [
        "SELECT CASE WHEN a.id = 2 THEN b.st ELSE a.st END AS mixed FROM a JOIN b ON a.id = b.id",
        "SELECT COALESCE(a.st, b.st) AS mixed FROM a JOIN b ON a.id = b.id",
        "SELECT st AS mixed FROM a UNION ALL SELECT st AS mixed FROM b",
    ] {
        let text = plan_text(&ctx, sql).await;
        assert!(text.contains(kernel), "{sql}\n{text}");
    }
    let top = plan_text(
        &ctx,
        "SELECT CASE WHEN a.id = 2 THEN b.v ELSE a.v END AS mixed FROM a JOIN b ON a.id = b.id",
    )
    .await;
    assert!(top.contains("to_timestamp"), "{top}");
    assert!(!top.contains(kernel), "{top}");
}

#[tokio::test]
async fn an_update_assignment_cast_keeps_mains_text_and_an_inner_coercion_is_localised() {
    let ctx = ctx_at("America/New_York");
    let (zoned, naive) = (relation(true), relation(false));
    let both = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            zoned.schema().field(0).clone(),
            zoned.schema().field(1).clone(),
            naive.schema().field(1).clone().with_name("ns"),
        ])),
        vec![
            Arc::clone(zoned.column(0)),
            Arc::clone(zoned.column(1)),
            Arc::clone(naive.column(1)),
        ],
    )
    .expect("batch");
    ctx.register_batch("b", both).expect("b");
    let kernel = crate::nested_wall::LOCALIZE_NESTED_NTZ_NAME;
    let whole = plan_text(&ctx, "UPDATE b SET st = ns").await;
    assert!(whole.contains("Dml: op=[Update]"), "{whole}");
    assert!(!whole.contains(kernel), "{whole}");
    let inner = plan_text(
        &ctx,
        "UPDATE b SET st = CASE WHEN id = 1 THEN ns ELSE st END",
    )
    .await;
    assert!(inner.contains(kernel), "{inner}");
}

#[tokio::test]
async fn an_analyzed_insert_into_a_struct_wall_keeps_mains_struct_cast() {
    let ctx = ctx_at("America/New_York");
    ctx.register_batch("a", relation(false)).expect("a");
    ctx.register_batch("b", relation(true)).expect("b");
    let kernel = crate::nested_wall::LOCALIZE_NESTED_NTZ_NAME;
    let mixed = "SELECT id, st, v FROM a UNION ALL SELECT id, st, v FROM b";
    let stored = plan_text(&ctx, &format!("INSERT INTO a {mixed}")).await;
    assert!(stored.contains(kernel), "{stored}");
    let analyzed = plan_text(&ctx, &format!("EXPLAIN ANALYZE INSERT INTO a {mixed}")).await;
    assert!(analyzed.contains("Dml: op=[Insert Into]"), "{analyzed}");
    assert!(!analyzed.contains(kernel), "{analyzed}");
    assert!(analyzed.contains("to_timestamp"), "{analyzed}");
    let zoned = plan_text(&ctx, &format!("EXPLAIN ANALYZE INSERT INTO b {mixed}")).await;
    assert!(zoned.contains(kernel), "{zoned}");
}
