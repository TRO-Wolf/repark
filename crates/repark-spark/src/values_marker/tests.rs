use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::config::Dialect;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{Analyzer, AnalyzerRule};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::{SparkValuesTyping, mark_df_statement, values_cell_udf};

fn ctx() -> SessionContext {
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = Analyzer::new().rules;
    rules.insert(0, Arc::new(SparkValuesTyping));
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_udf(values_cell_udf().as_ref().clone());
    ctx
}

async fn plan(ctx: &SessionContext, sql: &str, marked: bool) -> LogicalPlan {
    let state = ctx.state();
    let mut statement = state.sql_to_statement(sql, &Dialect::Generic).unwrap();
    if marked {
        mark_df_statement(&mut statement);
    }
    state.statement_to_plan(statement).await.unwrap()
}

async fn answer(ctx: &SessionContext, sql: &str, marked: bool) -> (DataType, Vec<String>) {
    let logical = plan(ctx, sql, marked).await;
    let frame = ctx.execute_logical_plan(logical).await.unwrap();
    let physical = frame.clone().create_physical_plan().await.unwrap();
    let data_type = physical.schema().field(0).data_type().clone();
    let mut values = Vec::new();
    for batch in frame.collect().await.unwrap() {
        for row in 0..batch.num_rows() {
            values.push(array_value_to_string(batch.column(0), row).unwrap());
        }
    }
    values.sort();
    (data_type, values)
}

const MIXED: &str =
    "SELECT c FROM (VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))) AS t(c)";

#[tokio::test]
async fn a_marked_mixed_values_types_as_double() {
    let ctx = ctx();
    let (data_type, values) = answer(&ctx, MIXED, true).await;
    assert_eq!(data_type, DataType::Float64);
    assert_eq!(values, vec!["0.1", "1e-20"]);
}

#[tokio::test]
async fn an_unmarked_mixed_values_refuses_naming_the_unit() {
    let ctx = ctx();
    let logical = plan(&ctx, MIXED, false).await;
    let error = ctx
        .execute_logical_plan(logical)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("DECIMAL-DOUBLE-UNION-COERCION-1"), "{error}");
}

#[tokio::test]
async fn the_tripwire_stays_quiet_on_the_shapes_main_answers() {
    let ctx = ctx();
    let thousand = (0..1000)
        .map(|row| {
            if row == 500 {
                "(CAST('1e-20' AS DOUBLE))".to_string()
            } else {
                format!("(CAST({row} AS DECIMAL(10,2)))")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    for (sql, data_type, first) in [
        (
            "SELECT c FROM (VALUES (CAST(0.1 AS DECIMAL(30,15))), (CAST(CAST('1e-20' AS DOUBLE) AS DECIMAL(30,15)))) AS t(c)".to_string(),
            DataType::Decimal128(30, 15),
            "0.000000000000000",
        ),
        (
            "SELECT c FROM (VALUES (CAST(0.1 AS DOUBLE)), (CAST('1e-20' AS DOUBLE))) AS t(c)".to_string(),
            DataType::Float64,
            "0.1",
        ),
        (
            "SELECT c FROM (VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST(2 AS DECIMAL(10,2)))) AS t(c)".to_string(),
            DataType::Decimal128(10, 2),
            "0.10",
        ),
        (
            "SELECT c FROM (VALUES (CAST(0.1 AS DECIMAL(10,2)))) AS t(c)".to_string(),
            DataType::Decimal128(10, 2),
            "0.10",
        ),
        (
            "SELECT c FROM (VALUES (1), (CAST('1e-20' AS DOUBLE))) AS t(c)".to_string(),
            DataType::Float64,
            "1.0",
        ),
        (
            format!("SELECT c FROM (VALUES {thousand}) AS t(c)"),
            DataType::Float64,
            "0.0",
        ),
    ] {
        let (planned, values) = answer(&ctx, &sql, true).await;
        assert_eq!(planned, data_type, "{sql}");
        assert_eq!(values.first().map(String::as_str), Some(first), "{sql}");
    }
}

#[tokio::test]
async fn a_second_analyzer_pass_finds_nothing_to_refuse() {
    let ctx = ctx();
    for sql in [
        MIXED,
        "SELECT c FROM (VALUES (CAST(0.1 AS DECIMAL(30,15))), (CAST(CAST('1e-20' AS DOUBLE) AS DECIMAL(30,15)))) AS t(c)",
        "SELECT c FROM (VALUES (CAST(0.5 AS DECIMAL(10,2))), (CAST(CAST('1e-20' AS DOUBLE) AS DECIMAL(10,2))), (CAST(1 AS DECIMAL(3,1)))) AS t(c)",
    ] {
        let state = ctx.state();
        let logical = plan(&ctx, sql, true).await;
        let once = state
            .analyzer()
            .execute_and_check(logical, state.config_options(), |_, _| {})
            .unwrap();
        let twice = state
            .analyzer()
            .execute_and_check(once.clone(), state.config_options(), |_, _| {})
            .unwrap();
        assert_eq!(once.schema(), twice.schema(), "{sql}");
        assert!(
            !format!("{}", twice.display_indent()).contains(super::VALUES_CELL_NAME),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn a_values_under_a_set_operation_and_a_subquery_is_typed() {
    let ctx = ctx();
    let (data_type, values) = answer(
        &ctx,
        "SELECT 1 AS p WHERE CAST('1e-20' AS DOUBLE) IN (SELECT c FROM (VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))) AS t(c))",
        true,
    )
    .await;
    assert_eq!(data_type, DataType::Int64);
    assert_eq!(values, vec!["1"]);
    let (data_type, values) = answer(
        &ctx,
        "SELECT c FROM (VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))) AS t(c) UNION ALL SELECT CAST(2.5 AS DOUBLE)",
        true,
    )
    .await;
    assert_eq!(data_type, DataType::Float64);
    assert_eq!(values, vec!["0.1", "1e-20", "2.5"]);
}

#[tokio::test]
async fn an_insert_values_casts_through_double_and_keeps_the_target_type() {
    let ctx = ctx();
    ctx.sql("CREATE TABLE target (c DECIMAL(10,2))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let logical = plan(
        &ctx,
        "INSERT INTO target VALUES (CAST(1.5 AS DECIMAL(4,3))), (CAST('2.25' AS DOUBLE))",
        true,
    )
    .await;
    let state = ctx.state();
    let analyzed = state
        .analyzer()
        .execute_and_check(logical, state.config_options(), |_, _| {})
        .unwrap();
    let text = format!("{}", analyzed.display_indent());
    assert!(
        text.matches("AS Float64) AS Decimal128(10, 2))").count() == 2,
        "{text}"
    );
    assert!(!text.contains(super::VALUES_CELL_NAME), "{text}");
}

#[test]
fn the_marker_pass_wraps_only_multi_row_values_cells() {
    let state = ctx().state();
    for (sql, marked) in [
        ("SELECT * FROM (VALUES (1), (2)) AS t(c)", 2),
        ("SELECT * FROM (VALUES (1)) AS t(c)", 0),
        (
            "EXPLAIN SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(a, b)",
            4,
        ),
        ("INSERT INTO x VALUES ($1), ($2)", 0),
        ("INSERT INTO x VALUES (DEFAULT), (1)", 1),
        (
            "SELECT 1 UNION ALL SELECT * FROM (VALUES (1), (2)) AS t(c)",
            2,
        ),
    ] {
        let mut statement = state.sql_to_statement(sql, &Dialect::Generic).unwrap();
        mark_df_statement(&mut statement);
        mark_df_statement(&mut statement);
        let text = statement.to_string();
        assert_eq!(
            text.matches(super::VALUES_CELL_NAME).count(),
            marked,
            "{text}"
        );
    }
}
