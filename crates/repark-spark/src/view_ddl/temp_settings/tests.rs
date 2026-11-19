use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::DFSchema;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{
    Expr, LogicalPlanBuilder, ScalarUDF, cast, col, in_list, lit, try_cast,
};
use datafusion::physical_expr::expressions::{CastExpr, TryCastExpr};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use repark_core::ReparkSession;

use super::*;
use crate::{SparkDialect, SparkExtension};

const LA: &str = "America/Los_Angeles";

fn spark_state(ansi: bool, zone: &str) -> SessionState {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("spark.sql.ansi.enabled", ansi.to_string())
        .config("spark.sql.session.timeZone", zone)
        .build()
        .unwrap_or_else(|error| panic!("session: {error}"))
        .context()
        .state()
}

fn function(state: &SessionState, name: &str) -> Arc<ScalarUDF> {
    state
        .scalar_functions()
        .get(name)
        .cloned()
        .unwrap_or_else(|| panic!("{name} is registered"))
}

fn numbers() -> (RecordBatch, DFSchema) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("big", DataType::Int64, false),
        Field::new("x", DataType::Float64, false),
        Field::new("d", DataType::Decimal128(10, 2), false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![3_000_000_000, 5, -0])),
        Arc::new(Float64Array::from(vec![1e10, -0.0, f64::NAN])),
        Arc::new(
            Decimal128Array::from(vec![12_345_678, -250, 0])
                .with_precision_and_scale(10, 2)
                .unwrap_or_else(|error| panic!("decimal: {error}")),
        ),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
        .unwrap_or_else(|error| panic!("batch: {error}"));
    let df_schema =
        DFSchema::try_from(schema.as_ref().clone()).unwrap_or_else(|error| panic!("{error}"));
    (batch, df_schema)
}

fn evaluated(state: &SessionState, expr: &Expr, schema: &DFSchema, batch: &RecordBatch) -> String {
    match state.create_physical_expr(expr.clone(), schema) {
        Err(error) => format!("plan error: {error}"),
        Ok(physical) => match physical.evaluate(batch) {
            Err(error) => format!("error: {error}"),
            Ok(value) => match value.into_array(batch.num_rows()) {
                Err(error) => format!("error: {error}"),
                Ok(array) => format!("{array:?}"),
            },
        },
    }
}

#[test]
fn the_allow_list_is_closed_and_defaults_to_sensitive() {
    let (_, schema) = numbers();
    let schema = Some(&schema);
    let free = [
        col("big"),
        lit(1_i64),
        col("big").eq(lit(1_i64)),
        col("big").not_eq(lit(0_i64)).and(col("x").is_null()),
        col("big").lt(lit(3_i64)).or(col("big").gt_eq(lit(9_i64))),
        col("big").is_not_null().is_true(),
        Expr::Not(Box::new(col("big").eq(lit(0_i64)))),
        in_list(col("big"), vec![lit(2_i64), lit(3_i64)], false),
        col("big").between(lit(1_i64), lit(5_i64)),
        cast(col("big"), DataType::Int32).gt(lit(1_i32)),
        try_cast(col("x"), DataType::Int16).eq(lit(2_i16)),
        cast(col("d"), DataType::Float64).alias("dd"),
    ];
    for expr in &free {
        assert!(settings_free(expr, schema), "free: {expr}");
    }
    let sensitive = [
        col("big") + lit(0_i64),
        -col("big"),
        cast(col("big"), DataType::Utf8),
        cast(lit("7"), DataType::Int32),
        cast(col("big"), DataType::Int32)
            .gt(lit(1_i32))
            .and(col("x") / lit(2.0)),
        col("big").like(lit("1%")),
        Expr::Case(datafusion::logical_expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(col("big").eq(lit(1_i64))), Box::new(lit(1_i64)))],
            else_expr: None,
        }),
        datafusion::functions::expr_fn::abs(col("big")),
        Expr::Placeholder(datafusion::logical_expr::expr::Placeholder::new_with_field(
            "$1".to_string(),
            None,
        )),
    ];
    for expr in &sensitive {
        assert!(!settings_free(expr, schema), "sensitive: {expr}");
    }
    assert!(!settings_free(&cast(col("big"), DataType::Int32), None));
}

#[test]
fn a_numeric_cast_plans_the_same_physical_cast_under_every_setting() {
    let (batch, schema) = numbers();
    let on_utc = spark_state(true, "UTC");
    let off_la = spark_state(false, LA);
    let casts = [
        cast(col("big"), DataType::Int32),
        cast(col("big"), DataType::Int16),
        cast(col("x"), DataType::Int32),
        cast(col("d"), DataType::Int8),
        cast(col("big"), DataType::Decimal128(5, 0)),
        cast(col("x"), DataType::Float32),
        try_cast(col("big"), DataType::Int32),
    ];
    for expr in &casts {
        assert!(settings_free(expr, Some(&schema)), "{expr}");
        let physical = on_utc
            .create_physical_expr(expr.clone(), &schema)
            .unwrap_or_else(|error| panic!("{expr}: {error}"));
        let planned = physical.as_ref();
        assert!(
            planned.downcast_ref::<CastExpr>().is_some()
                || planned.downcast_ref::<TryCastExpr>().is_some(),
            "{expr} plans as {physical}"
        );
        assert_eq!(
            evaluated(&on_utc, expr, &schema, &batch),
            evaluated(&off_la, expr, &schema, &batch),
            "{expr}"
        );
    }
}

#[test]
fn the_shield_and_zero_udfs_read_no_setting() {
    let (batch, schema) = numbers();
    let on_utc = spark_state(true, "UTC");
    let off_la = spark_state(false, LA);
    let calls = [
        repark_core::cast_shield_udf().call(vec![cast(col("big"), DataType::Int32)]),
        repark_core::cast_shield_udf().call(vec![cast(col("x"), DataType::Int64)]),
        repark_core::extension::normalize_zero_call(col("x")),
        repark_core::extension::normalize_zero_call(cast(col("big"), DataType::Float64)),
    ];
    for expr in &calls {
        assert_eq!(
            evaluated(&on_utc, expr, &schema, &batch),
            evaluated(&off_la, expr, &schema, &batch),
            "{expr}"
        );
    }
    for source in [
        include_str!("../../../../repark-core/src/session/df_guards/cast_shield/udf.rs"),
        include_str!("../../../../repark-core/src/session/df_guards/signed_zero/udf.rs"),
    ] {
        assert!(!source.contains("config_options"));
        assert!(!source.contains("ConfigOptions"));
    }
}

#[test]
fn matching_settings_keep_the_scan_on_todays_path() {
    let creation = spark_state(true, "UTC");
    let captured = TempViewSettings::capture(creation.config_options());
    let same = spark_state(true, "UTC");
    let reader_off = spark_state(false, "UTC");
    let reader_la = spark_state(true, LA);
    assert!(captured.overlay(&same, same.config_options()).is_none());
    assert!(
        captured
            .overlay(&reader_off, reader_off.config_options())
            .is_some()
    );
    assert!(
        captured
            .overlay(&same, reader_la.config_options())
            .is_some()
    );
    assert!(
        captured
            .overlay(&reader_la, same.config_options())
            .is_some()
    );
    let overlay = captured
        .overlay(&reader_la, reader_la.config_options())
        .unwrap_or_else(|| panic!("differing settings overlay"));
    assert!(captured.same_as(overlay.config_options()));
    let props = overlay
        .execution_props()
        .config_options
        .as_ref()
        .unwrap_or_else(|| panic!("the overlay installs its options"));
    assert!(captured.same_as(props));
    assert_eq!(
        overlay.execution_props().query_execution_start_time,
        reader_la.execution_props().query_execution_start_time
    );
    assert_eq!(reader_zone(&overlay), LA);
    assert!(captured.overlay(&overlay, same.config_options()).is_some());
}

#[test]
fn a_nested_overlay_keeps_the_outermost_reader_zone() {
    let reader = spark_state(true, LA);
    let outer = TempViewSettings::capture(spark_state(true, "Pacific/Kiritimati").config_options());
    let inner = TempViewSettings::capture(spark_state(true, "UTC").config_options());
    let outer_overlay = outer
        .overlay(&reader, reader.config_options())
        .unwrap_or_else(|| panic!("outer overlay"));
    let inner_overlay = inner
        .overlay(&outer_overlay, reader.config_options())
        .unwrap_or_else(|| panic!("inner overlay"));
    assert_eq!(reader_zone(&outer_overlay), LA);
    assert_eq!(reader_zone(&inner_overlay), LA);
    assert!(inner.same_as(inner_overlay.config_options()));
}

async fn rows_under_overlay(filters: &[Expr], limit: Option<usize>) -> (String, String) {
    let creation = spark_state(true, "UTC");
    let reader = spark_state(false, LA);
    let overlay = TempViewSettings::capture(creation.config_options())
        .overlay(&reader, reader.config_options())
        .unwrap_or_else(|| panic!("overlay"));
    let zone = function(&reader, "current_timezone");
    let body =
        LogicalPlanBuilder::values(vec![vec![lit(1_i32)], vec![lit(2_i32)], vec![lit(3_i32)]])
            .and_then(|builder| {
                builder.project(vec![
                    col("column1").alias("a"),
                    zone.call(vec![]).alias("z"),
                ])
            })
            .and_then(LogicalPlanBuilder::build)
            .unwrap_or_else(|error| panic!("body: {error}"));
    let plan = plan_under_overlay(&overlay, &reader, body, None, filters, limit)
        .await
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let shape = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string();
    let batches = collect(Arc::clone(&plan), reader.task_ctx())
        .await
        .unwrap_or_else(|error| panic!("collect: {error}"));
    let rows = pretty_format_batches(&batches)
        .unwrap_or_else(|error| panic!("format: {error}"))
        .to_string();
    assert_global_limit(&plan, limit);
    (rows, shape)
}

fn assert_global_limit(plan: &Arc<dyn ExecutionPlan>, limit: Option<usize>) {
    if let Some(limit) = limit
        && let Some(global) = plan.as_ref().downcast_ref::<GlobalLimitExec>()
    {
        assert_eq!(global.fetch(), Some(limit));
    }
}

#[tokio::test]
async fn sensitive_conjuncts_run_under_the_reader_with_the_limit_above() {
    let zone = function(&spark_state(false, LA), "current_timezone");
    let filters = [col("a").gt(lit(1_i32)), zone.call(vec![]).eq(lit(LA))];
    let (rows, shape) = rows_under_overlay(&filters, Some(1)).await;
    assert!(rows.contains("| 2 | America/Los_Angeles |"), "{rows}");
    assert_eq!(rows.matches("America/Los_Angeles").count(), 1, "{rows}");
    let top = shape.lines().next().unwrap_or_default();
    assert!(top.starts_with("GlobalLimitExec"), "{shape}");
    assert!(
        shape.contains("FilterExec: current_timezone() = America/Los_Angeles"),
        "{shape}"
    );
    let (rows, _) = rows_under_overlay(&filters[1..], Some(2)).await;
    assert!(rows.contains("| 1 | America/Los_Angeles |"), "{rows}");
    assert!(rows.contains("| 2 | America/Los_Angeles |"), "{rows}");
    assert!(!rows.contains("| 3 |"), "{rows}");
}

#[tokio::test]
async fn free_conjuncts_enter_the_body_and_the_body_answers_the_reader_zone() {
    let (rows, shape) = rows_under_overlay(&[col("a").not_eq(lit(2_i32))], Some(1)).await;
    assert!(rows.contains("| 1 | America/Los_Angeles |"), "{rows}");
    assert!(!rows.contains("| 3 |"), "{rows}");
    assert!(!shape.contains("current_timezone()"), "{shape}");
}

#[tokio::test]
async fn a_sensitive_conjunct_leaves_unread_body_columns_pruned() {
    let creation = spark_state(true, "UTC");
    let reader = spark_state(false, LA);
    let overlay = TempViewSettings::capture(creation.config_options())
        .overlay(&reader, reader.config_options())
        .unwrap_or_else(|| panic!("overlay"));
    let zone = function(&reader, "current_timezone");
    let raising = lit(10.0)
        / repark_functions::ansi::guard_nonzero_divisor(cast(
            col("column1") - lit(1_i32),
            DataType::Float64,
        ));
    let body = LogicalPlanBuilder::values(vec![vec![lit(1_i32)], vec![lit(2_i32)]])
        .and_then(|builder| builder.project(vec![col("column1").alias("a"), raising.alias("bad")]))
        .and_then(LogicalPlanBuilder::build)
        .unwrap_or_else(|error| panic!("body: {error}"));
    let filters = [zone.call(vec![]).eq(lit(LA)).and(col("a").gt(lit(1_i32)))];
    let plan = plan_under_overlay(&overlay, &reader, body, Some(&vec![0]), &filters, None)
        .await
        .unwrap_or_else(|error| panic!("plan: {error}"));
    let batches = collect(plan, reader.task_ctx())
        .await
        .unwrap_or_else(|error| panic!("collect: {error}"));
    let rows = pretty_format_batches(&batches)
        .unwrap_or_else(|error| panic!("format: {error}"))
        .to_string();
    assert!(rows.contains("| 2 |"), "{rows}");
    assert!(!rows.contains("| 1 |"), "{rows}");
}
