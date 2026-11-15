use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::{Analyzer, AnalyzerRule};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::{SparkDecimalFloatWiden, insert_decimal_float_widen_before_coercion};

fn ctx() -> SessionContext {
    let rules = insert_decimal_float_widen_before_coercion(Analyzer::new().rules).unwrap();
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    SessionContext::new_with_state(state)
}

async fn setup() -> SessionContext {
    let ctx = ctx();
    ctx.sql(
        "CREATE VIEW src AS SELECT * FROM (VALUES (1, CAST(0 AS DECIMAL(10,2)), CAST(1e-20 AS DOUBLE), CAST(0.5 AS DECIMAL(38,18)), CAST(0.5 AS FLOAT))) t(id, dc, dd, dbig, ff)",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    ctx
}

async fn answer(ctx: &SessionContext, sql: &str) -> (DataType, Vec<String>) {
    let frame = ctx.sql(sql).await.unwrap();
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

#[tokio::test]
async fn set_operations_unify_a_decimal_and_a_double_to_double() {
    let ctx = setup().await;
    for (sql, expected) in [
        (
            "SELECT dc AS c FROM src UNION ALL SELECT dd FROM src",
            vec!["0.0", "1e-20"],
        ),
        (
            "SELECT dd AS c FROM src UNION ALL SELECT dc FROM src",
            vec!["0.0", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION SELECT dd FROM src",
            vec!["0.0", "1e-20"],
        ),
        (
            "SELECT dc AS c FROM src UNION ALL SELECT ff FROM src",
            vec!["0.0", "0.5"],
        ),
        (
            "SELECT dc AS c FROM src INTERSECT SELECT dd FROM src",
            vec![],
        ),
        (
            "SELECT dc AS c FROM src EXCEPT SELECT dd FROM src",
            vec!["0.0"],
        ),
    ] {
        let (data_type, values) = answer(&ctx, sql).await;
        assert_eq!(data_type, DataType::Float64, "{sql}");
        assert_eq!(values, expected, "{sql}");
    }
}

#[tokio::test]
async fn expressions_unify_a_decimal_and_a_double_to_double() {
    let ctx = setup().await;
    for (sql, data_type, expected) in [
        ("SELECT dc = dd FROM src", DataType::Boolean, "false"),
        ("SELECT dd = dc FROM src", DataType::Boolean, "false"),
        (
            "SELECT dd IN (dc, dbig) FROM src",
            DataType::Boolean,
            "false",
        ),
        (
            "SELECT dd BETWEEN dc AND dc FROM src",
            DataType::Boolean,
            "false",
        ),
        (
            "SELECT dc BETWEEN dc AND dd FROM src",
            DataType::Boolean,
            "true",
        ),
        (
            "SELECT count(*) FROM src WHERE dd IN (SELECT dc FROM src)",
            DataType::Int64,
            "0",
        ),
        (
            "SELECT count(*) FROM src WHERE dc IN (SELECT dd FROM src)",
            DataType::Int64,
            "0",
        ),
        (
            "SELECT CASE WHEN id = 2 THEN dc ELSE dd END FROM src",
            DataType::Float64,
            "1e-20",
        ),
        (
            "SELECT CASE dc WHEN dd THEN 1 ELSE 0 END FROM src",
            DataType::Int64,
            "0",
        ),
        (
            "SELECT CASE dc WHEN dd THEN 1 WHEN dbig THEN 2 ELSE 0 END FROM src",
            DataType::Int64,
            "0",
        ),
        (
            "SELECT coalesce(dd, dc) FROM src",
            DataType::Float64,
            "1e-20",
        ),
        (
            "SELECT greatest(dc, dd) FROM src",
            DataType::Float64,
            "1e-20",
        ),
        (
            "SELECT least(dbig, dd) FROM src",
            DataType::Float64,
            "1e-20",
        ),
        ("SELECT nullif(dd, dc) FROM src", DataType::Float64, "1e-20"),
        (
            "SELECT nullif(dc, dd) FROM src",
            DataType::Decimal128(10, 2),
            "0.00",
        ),
        (
            "SELECT count(*) FROM src a JOIN src b ON a.dc = b.dd",
            DataType::Int64,
            "0",
        ),
    ] {
        let (found, values) = answer(&ctx, sql).await;
        assert_eq!(found, data_type, "{sql}");
        assert_eq!(values, vec![expected.to_string()], "{sql}");
    }
}

#[tokio::test]
async fn a_user_semi_join_keeps_the_left_type() {
    let ctx = setup().await;
    let (data_type, values) = answer(
        &ctx,
        "SELECT a.dc FROM src a LEFT SEMI JOIN src b ON a.dc = b.dd",
    )
    .await;
    assert_eq!(data_type, DataType::Decimal128(10, 2));
    assert!(values.is_empty());
}

#[tokio::test]
async fn a_plan_without_a_decimal_and_float_pair_is_returned_unchanged() {
    let ctx = setup().await;
    for sql in [
        "SELECT dc + 1, dd * 2, dc = dbig, dd = 1.5 FROM src",
        "SELECT dc AS c FROM src UNION ALL SELECT dbig FROM src",
        "SELECT dd AS c FROM src UNION ALL SELECT ff FROM src",
    ] {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let shown = plan.display_indent().to_string();
        let after = SparkDecimalFloatWiden
            .analyze(plan, ctx.state().config_options())
            .unwrap();
        assert_eq!(after.display_indent().to_string(), shown, "{sql}");
    }
}

#[test]
fn the_rule_sits_before_type_coercion_and_refuses_without_it() {
    let rules = insert_decimal_float_widen_before_coercion(Analyzer::new().rules).unwrap();
    let names: Vec<&str> = rules.iter().map(|rule| rule.name()).collect();
    let widen = names
        .iter()
        .position(|name| *name == "spark_decimal_float_widen")
        .unwrap();
    assert_eq!(names.get(widen + 1), Some(&"type_coercion"));
    let without: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = Analyzer::new()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != "type_coercion")
        .collect();
    assert!(insert_decimal_float_widen_before_coercion(without).is_err());
}
