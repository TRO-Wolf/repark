use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{
    DataType, Date32Type, Field, Fields, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use datafusion::common::ScalarValue;
use datafusion::functions::core::expr_fn::named_struct;
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::{Cast, Expr, TryCast, lit};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::ansi::with_spark_ansi_config;
use crate::session_time_zone::with_session_time_zone;

const ZONES: [&str; 3] = ["UTC", "America/New_York", "Asia/Kolkata"];
const INSTANT: i64 = 1_767_323_045_123_456;
const DAY: i32 = 20_455;
const EPOCH_SECONDS: i64 = 1_767_323_045;
const WALL_TEXT: &str = "2026-01-02 03:04:05.123456";

const NTZ_WALLS: [&str; 3] = ["1767323045123456", "1767305045123456", "1767342845123456"];
const SESSION_TEXT: [&str; 3] = [
    "2026-01-02 03:04:05.123456",
    "2026-01-01 22:04:05.123456",
    "2026-01-02 08:34:05.123456",
];
const SESSION_DAYS: [&str; 3] = ["20455", "20454", "20455"];
const WALL_INSTANTS: [&str; 3] = ["1767323045123456", "1767341045123456", "1767303245123456"];
const DATE_INSTANTS: [&str; 3] = ["1767312000000000", "1767330000000000", "1767292200000000"];
const SECONDS: [&str; 3] = ["1767323045", "1767323045", "1767323045"];
const WALL_TEXTS: [&str; 3] = [WALL_TEXT, WALL_TEXT, WALL_TEXT];

#[derive(Clone, Copy, Debug)]
enum Shape {
    Struct,
    Array,
    Map,
}

const SHAPES: [Shape; 3] = [Shape::Struct, Shape::Array, Shape::Map];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Ntz,
    Ltz,
    Text,
    Day,
    Seconds,
}

fn context(zone: &str) -> SessionContext {
    let config = with_spark_ansi_config(with_session_time_zone(SessionConfig::new(), zone), true);
    let ctx = crate::spark_type_planner::planned_context(config);
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

fn instant() -> Expr {
    lit(ScalarValue::TimestampMicrosecond(
        Some(INSTANT),
        Some(Arc::from("UTC")),
    ))
}

fn wall(micros: i64) -> Expr {
    lit(ScalarValue::TimestampMicrosecond(Some(micros), None))
}

fn bigint() -> Expr {
    Expr::Cast(Cast::new(Box::new(lit(EPOCH_SECONDS)), DataType::Int64))
}

fn day() -> Expr {
    lit(ScalarValue::Date32(Some(DAY)))
}

fn leaf_type(kind: Kind) -> DataType {
    match kind {
        Kind::Ntz => DataType::Timestamp(TimeUnit::Microsecond, None),
        Kind::Ltz => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        Kind::Text => DataType::Utf8,
        Kind::Day => DataType::Date32,
        Kind::Seconds => DataType::Int64,
    }
}

fn wrap(shape: Shape, value: Expr) -> Expr {
    match shape {
        Shape::Struct => named_struct(vec![lit("a"), value]),
        Shape::Array => make_array(vec![value]),
        Shape::Map => {
            crate::expr_fn::map_from_arrays(make_array(vec![lit("k")]), make_array(vec![value]))
        }
    }
}

fn wrap_type(shape: Shape, leaf: DataType) -> DataType {
    match shape {
        Shape::Struct => DataType::Struct(Fields::from(vec![Field::new("a", leaf, true)])),
        Shape::Array => DataType::List(Arc::new(Field::new_list_field(leaf, true))),
        Shape::Map => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", leaf, true),
                ])),
                false,
            )),
            false,
        ),
    }
}

fn container_cast(shape: Shape, value: Expr, kind: Kind, try_cast: bool) -> Expr {
    let wrapped = wrap(shape, value);
    let target = wrap_type(shape, leaf_type(kind));
    match (shape, try_cast) {
        (Shape::Map, _) => crate::cast_map::cast_map_expr(wrapped, &target, try_cast),
        (_, true) => Expr::TryCast(TryCast::new(Box::new(wrapped), target)),
        (_, false) => Expr::Cast(Cast::new(Box::new(wrapped), target)),
    }
}

async fn selected(zone: &str, expr: Expr) -> datafusion::error::Result<RecordBatch> {
    let ctx = context(zone);
    let frame = ctx
        .sql("SELECT 1 AS one")
        .await?
        .select(vec![expr.alias("v")])?;
    let analyzed = crate::analyze_eagerly(&ctx.state(), frame.logical_plan().clone())?;
    let batches = ctx.execute_logical_plan(analyzed).await?.collect().await?;
    Ok(batches.into_iter().next().expect("one batch"))
}

fn first_leaf(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Struct(_) => Arc::clone(array.as_struct().column(0)),
        DataType::List(_) => Arc::clone(array.as_list::<i32>().values()),
        DataType::Map(_, _) => Arc::clone(array.as_map().values()),
        _ => Arc::clone(array),
    }
}

fn rendered(leaf: &ArrayRef) -> (Option<Kind>, String) {
    let kind = match leaf.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, None) => Some(Kind::Ntz),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Some(Kind::Ltz),
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Some(Kind::Text),
        DataType::Date32 => Some(Kind::Day),
        DataType::Int64 => Some(Kind::Seconds),
        _ => None,
    };
    if leaf.is_empty() || leaf.is_null(0) {
        return (kind, "NULL".to_string());
    }
    let text = match kind {
        Some(Kind::Ntz | Kind::Ltz) => leaf
            .as_primitive::<TimestampMicrosecondType>()
            .value(0)
            .to_string(),
        Some(Kind::Seconds) => leaf.as_primitive::<Int64Type>().value(0).to_string(),
        Some(Kind::Day) => leaf.as_primitive::<Date32Type>().value(0).to_string(),
        Some(Kind::Text) => arrow::util::display::array_value_to_string(leaf, 0)
            .unwrap_or_else(|error| format!("unrenderable: {error}")),
        None => format!("{:?}", leaf.data_type()),
    };
    (kind, text)
}

async fn answer(zone: &str, expr: Expr) -> (Option<Kind>, String) {
    match selected(zone, expr).await {
        Ok(batch) => rendered(&first_leaf(batch.column(0))),
        Err(error) => (None, format!("error: {error}")),
    }
}

async fn leaf_answer(zone: &str, expr: Expr) -> (Option<Kind>, String) {
    match selected(zone, expr).await {
        Ok(batch) => rendered(batch.column(0)),
        Err(error) => (None, format!("error: {error}")),
    }
}

async fn assert_pair(source: fn() -> Expr, kind: Kind, expected: [&str; 3], label: &str) {
    let mut wrong = Vec::new();
    for shape in SHAPES {
        for try_cast in [false, true] {
            for (zone, want) in ZONES.iter().zip(expected) {
                let got = answer(zone, container_cast(shape, source(), kind, try_cast)).await;
                if got != (Some(kind), want.to_string()) {
                    wrong.push(format!(
                        "{shape:?} try={try_cast} {zone}: {got:?} want {want}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{label}:\n{}", wrong.join("\n"));
}

fn text_source() -> Expr {
    lit(WALL_TEXT)
}

fn wall_source() -> Expr {
    wall(INSTANT)
}

#[tokio::test]
async fn a_container_cast_of_an_instant_to_timestamp_ntz_takes_the_session_wall() {
    assert_pair(instant, Kind::Ntz, NTZ_WALLS, "row 1").await;
}

#[tokio::test]
async fn a_container_cast_of_an_instant_to_string_renders_the_session_text() {
    assert_pair(instant, Kind::Text, SESSION_TEXT, "row 2").await;
}

#[tokio::test]
async fn a_container_cast_of_an_instant_to_date_takes_the_session_date() {
    assert_pair(instant, Kind::Day, SESSION_DAYS, "row 3").await;
}

#[tokio::test]
async fn a_container_cast_of_an_instant_to_bigint_answers_epoch_seconds() {
    assert_pair(instant, Kind::Seconds, SECONDS, "row 4").await;
}

#[tokio::test]
async fn a_container_cast_of_a_wall_to_timestamp_reads_it_in_the_session_zone() {
    assert_pair(wall_source, Kind::Ltz, WALL_INSTANTS, "row 5").await;
}

#[tokio::test]
async fn a_container_cast_of_a_zoneless_string_to_timestamp_reads_it_in_the_session_zone() {
    assert_pair(text_source, Kind::Ltz, WALL_INSTANTS, "row 6").await;
}

#[tokio::test]
async fn a_container_cast_of_a_date_to_timestamp_starts_at_the_session_midnight() {
    assert_pair(day, Kind::Ltz, DATE_INSTANTS, "row 7").await;
}

#[tokio::test]
async fn a_container_cast_of_a_wall_to_string_renders_spark_text() {
    assert_pair(wall_source, Kind::Text, WALL_TEXTS, "row 8").await;
}

async fn assert_refused(source: Expr, kind: Kind, label: &str) {
    let mut wrong = Vec::new();
    for shape in SHAPES {
        for try_cast in [false, true] {
            let got = answer(
                "America/New_York",
                container_cast(shape, source.clone(), kind, try_cast),
            )
            .await;
            if !got.1.contains("DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION") {
                wrong.push(format!("{shape:?} try={try_cast}: {got:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{label}:\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn a_container_cast_of_a_wall_to_bigint_refuses_as_the_leaf_does() {
    assert_refused(wall(INSTANT), Kind::Seconds, "row 9").await;
}

#[tokio::test]
async fn a_container_cast_of_a_bigint_to_timestamp_ntz_refuses_as_the_leaf_does() {
    assert_refused(bigint(), Kind::Ntz, "row 10").await;
}

fn bare_try(value: Expr, kind: Kind) -> Expr {
    Expr::TryCast(TryCast::new(Box::new(value), leaf_type(kind)))
}

#[tokio::test]
async fn a_leaf_try_cast_answers_what_the_leaf_cast_answers() {
    let cases: [(Expr, Kind, [&str; 3], &str); 7] = [
        (instant(), Kind::Text, SESSION_TEXT, "instant to string"),
        (instant(), Kind::Day, SESSION_DAYS, "instant to date"),
        (instant(), Kind::Seconds, SECONDS, "instant to bigint"),
        (wall(INSTANT), Kind::Ltz, WALL_INSTANTS, "wall to timestamp"),
        (wall(INSTANT), Kind::Text, WALL_TEXTS, "wall to string"),
        (day(), Kind::Ltz, DATE_INSTANTS, "date to timestamp"),
        (
            bigint(),
            Kind::Ltz,
            ["1767323045000000", "1767323045000000", "1767323045000000"],
            "bigint to timestamp",
        ),
    ];
    let mut wrong = Vec::new();
    for (value, kind, expected, label) in cases {
        for (zone, want) in ZONES.iter().zip(expected) {
            let got = leaf_answer(zone, bare_try(value.clone(), kind)).await;
            if got != (Some(kind), want.to_string()) {
                wrong.push(format!("{label} {zone}: {got:?} want {want}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[tokio::test]
async fn a_try_cast_answers_null_where_its_leaf_cannot_convert() {
    let mut wrong = Vec::new();
    for shape in SHAPES {
        let unplaceable = container_cast(shape, wall(i64::MAX), Kind::Ltz, true);
        let got = answer("America/New_York", unplaceable).await;
        if got != (Some(Kind::Ltz), "NULL".to_string()) {
            wrong.push(format!("{shape:?} unplaceable wall: {got:?}"));
        }
        let malformed = container_cast(shape, lit("not a time"), Kind::Ltz, true);
        let got = answer("America/New_York", malformed).await;
        if got != (Some(Kind::Ltz), "NULL".to_string()) {
            wrong.push(format!("{shape:?} malformed text: {got:?}"));
        }
    }
    let overflow = leaf_answer("UTC", bare_try(lit(i64::MAX), Kind::Ltz)).await;
    if overflow != (Some(Kind::Ltz), "NULL".to_string()) {
        wrong.push(format!("bigint overflow: {overflow:?}"));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[tokio::test]
async fn to_and_from_utc_timestamp_cast_a_wall_date_or_null_to_an_instant_first() {
    let kolkata = || lit("Asia/Kolkata");
    let new_york = || lit("America/New_York");
    let cases: [(Expr, [&str; 3], &str); 5] = [
        (
            crate::expr_fn::to_utc_timestamp(wall(INSTANT), kolkata()),
            ["1767303245123456", "1767321245123456", "1767283445123456"],
            "to_utc_timestamp of a wall",
        ),
        (
            crate::expr_fn::from_utc_timestamp(wall(INSTANT), new_york()),
            ["1767305045123456", "1767323045123456", "1767285245123456"],
            "from_utc_timestamp of a wall",
        ),
        (
            crate::expr_fn::to_utc_timestamp(day(), kolkata()),
            ["1767292200000000", "1767310200000000", "1767272400000000"],
            "to_utc_timestamp of a date",
        ),
        (
            crate::expr_fn::from_utc_timestamp(day(), new_york()),
            ["1767294000000000", "1767312000000000", "1767274200000000"],
            "from_utc_timestamp of a date",
        ),
        (
            crate::expr_fn::to_utc_timestamp(lit(ScalarValue::Null), kolkata()),
            ["NULL", "NULL", "NULL"],
            "to_utc_timestamp of NULL",
        ),
    ];
    let mut wrong = Vec::new();
    for (expr, expected, label) in cases {
        for (zone, want) in ZONES.iter().zip(expected) {
            let got = leaf_answer(zone, expr.clone()).await;
            if got != (Some(Kind::Ltz), want.to_string()) {
                wrong.push(format!("{label} {zone}: {got:?} want {want}"));
            }
        }
    }
    let null_from = leaf_answer(
        "UTC",
        crate::expr_fn::from_utc_timestamp(lit(ScalarValue::Null), new_york()),
    )
    .await;
    if null_from != (Some(Kind::Ltz), "NULL".to_string()) {
        wrong.push(format!("from_utc_timestamp of NULL: {null_from:?}"));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[tokio::test]
async fn to_json_renders_a_timestamp_ntz_leaf_as_its_wall_without_a_zone() {
    let to_json = |value: Expr| crate::json::to_json_udf().call(vec![value]);
    let cases: [(Expr, &str); 3] = [
        (
            named_struct(vec![lit("a"), wall(INSTANT)]),
            r#"{"a":"2026-01-02T03:04:05.123"}"#,
        ),
        (
            make_array(vec![make_array(vec![wall(INSTANT)])]),
            r#"[["2026-01-02T03:04:05.123"]]"#,
        ),
        (
            crate::expr_fn::map_from_arrays(
                make_array(vec![lit("k")]),
                make_array(vec![wall(INSTANT)]),
            ),
            r#"{"k":"2026-01-02T03:04:05.123"}"#,
        ),
    ];
    let mut wrong = Vec::new();
    for (value, want) in cases {
        for zone in ZONES {
            let got = leaf_answer(zone, to_json(value.clone())).await;
            if got != (Some(Kind::Text), want.to_string()) {
                wrong.push(format!("{zone}: {got:?} want {want}"));
            }
        }
    }
    let instant_json = leaf_answer(
        "America/New_York",
        to_json(named_struct(vec![lit("a"), instant()])),
    )
    .await;
    if instant_json.1 != r#"{"a":"2026-01-01T22:04:05.123-05:00"}"# {
        wrong.push(format!("instant control: {instant_json:?}"));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[tokio::test]
async fn a_container_cast_of_an_instant_column_takes_the_session_wall() {
    let ctx = context("America/New_York");
    let column = arrow::array::TimestampMicrosecondArray::from(vec![Some(INSTANT), None])
        .with_timezone("UTC");
    let batch =
        RecordBatch::try_from_iter(vec![("x", Arc::new(column) as ArrayRef)]).expect("one column");
    let target = wrap_type(Shape::Struct, leaf_type(Kind::Ntz));
    let cast = Expr::Cast(Cast::new(
        Box::new(wrap(Shape::Struct, datafusion::prelude::col("x"))),
        target,
    ));
    let batches = ctx
        .read_batch(batch)
        .expect("frame")
        .select(vec![cast.alias("v")])
        .expect("select")
        .collect()
        .await
        .expect("collect");
    let leaf = first_leaf(batches[0].column(0));
    let micros = leaf.as_primitive::<TimestampMicrosecondType>();
    assert_eq!(leaf.data_type(), &leaf_type(Kind::Ntz));
    assert_eq!(micros.value(0), 1_767_305_045_123_456);
    assert!(micros.is_null(1));
}

#[tokio::test]
async fn an_instant_kept_as_an_instant_and_a_naive_member_stay_native() {
    let mixed = named_struct(vec![lit("a"), instant(), lit("b"), lit(7_i64)]);
    let target = DataType::Struct(Fields::from(vec![
        Field::new("a", leaf_type(Kind::Ltz), true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let cast = Expr::Cast(Cast::new(Box::new(mixed), target));
    let batch = selected("America/New_York", cast).await.expect("answers");
    let structs = batch.column(0).as_struct();
    let instant_leaf = structs.column(0).as_primitive::<TimestampMicrosecondType>();
    assert_eq!(instant_leaf.value(0), INSTANT);
    assert_eq!(
        arrow::util::display::array_value_to_string(structs.column(1), 0).expect("text"),
        "7"
    );
}
