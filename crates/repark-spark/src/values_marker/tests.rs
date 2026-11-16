use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::config::Dialect;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{Analyzer, AnalyzerRule};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::{SparkValuesTyping, mark_df_statement, mark_fragment_statement, values_cell_udf};

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
            !format!("{}", twice.display_indent()).contains(super::values_cell_name()),
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
    assert!(!text.contains(super::values_cell_name()), "{text}");
}

#[test]
fn the_marker_pass_wraps_only_multi_row_values_cells() {
    let state = ctx().state();
    for (sql, marked) in [
        ("SELECT * FROM (VALUES (1), (2)) AS t(c)", 1),
        ("SELECT * FROM (VALUES (1)) AS t(c)", 0),
        (
            "EXPLAIN SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(a, b)",
            1,
        ),
        ("INSERT INTO x VALUES ($1), ($2)", 0),
        ("INSERT INTO x VALUES (DEFAULT), (1)", 1),
        (
            "SELECT 1 UNION ALL SELECT * FROM (VALUES (1), (2)) AS t(c)",
            1,
        ),
        (
            "SELECT * FROM (VALUES (NULL, 0.5), (1, 0.25)) AS t(a, b)",
            1,
        ),
        (
            "SELECT * FROM (VALUES (1, 0.5), (2, CAST(0.25 AS DOUBLE))) AS t(a, b)",
            1,
        ),
        (
            "SELECT * FROM (VALUES (1, x), (2, CAST(0.25 AS DOUBLE))) AS t(a, b)",
            2,
        ),
        ("SELECT * FROM (VALUES (1, 0.5), (2, f(y))) AS t(a, b)", 2),
        (
            "SELECT * FROM (VALUES ('a', 0.5), ('b', 'c')) AS t(a, b)",
            1,
        ),
    ] {
        let mut statement = state.sql_to_statement(sql, &Dialect::Generic).unwrap();
        mark_df_statement(&mut statement);
        mark_df_statement(&mut statement);
        let text = statement.to_string();
        assert_eq!(
            text.matches(super::values_cell_name()).count(),
            marked,
            "{text}"
        );
    }
}

#[test]
fn a_decided_mixed_column_is_cast_to_double_in_the_ast_and_a_fragment_marks_it() {
    let state = ctx().state();
    let sql = "SELECT * FROM (VALUES (1, 0.5, NULL), (2, -0.25, CAST(1 AS DECIMAL(3,1))), (3, CAST(0.75 AS DOUBLE), CAST('1e-20' AS DOUBLE)), (4, CAST(1 AS FLOAT), 7)) AS t(a, b, c)";
    let mut statement = state.sql_to_statement(sql, &Dialect::Generic).unwrap();
    mark_df_statement(&mut statement);
    let text = statement.to_string();
    for cast in [
        "CAST(0.5 AS DOUBLE)",
        "CAST(-0.25 AS DOUBLE)",
        "CAST(CAST(1 AS FLOAT) AS DOUBLE)",
        "CAST(CAST(1 AS DECIMAL(3,1)) AS DOUBLE)",
        "CAST(7 AS DOUBLE)",
    ] {
        assert!(text.contains(cast), "{cast}: {text}");
    }
    assert!(!text.contains("CAST(1 AS DOUBLE)"), "{text}");
    assert!(!text.contains("CAST(NULL AS DOUBLE)"), "{text}");
    assert_eq!(text.matches(super::values_cell_name()).count(), 1, "{text}");
    let mut fragment = state.sql_to_statement(sql, &Dialect::Generic).unwrap();
    assert!(mark_fragment_statement(&mut fragment));
    let text = fragment.to_string();
    assert!(!text.contains("AS DOUBLE) AS DOUBLE"), "{text}");
    assert_eq!(text.matches(super::values_cell_name()).count(), 8, "{text}");
    let mut plain = state
        .sql_to_statement(
            "SELECT * FROM (VALUES (1, 0.5), (2, 0.25)) AS t(a, b)",
            &Dialect::Generic,
        )
        .unwrap();
    assert!(!mark_fragment_statement(&mut plain));
}

#[tokio::test]
async fn a_user_written_marker_name_does_not_resolve() {
    let ctx = ctx();
    assert!(super::values_cell_name().starts_with("__repark_values_cell_"));
    for sql in [
        "SELECT __repark_values_cell(1)",
        "SELECT __REPARK_VALUES_CELL(1)",
        "SELECT c FROM (VALUES (__repark_values_cell(0.1)), (CAST('1e-20' AS DOUBLE))) AS t(c)",
    ] {
        let state = ctx.state();
        let mut statement = state.sql_to_statement(sql, &Dialect::Generic).unwrap();
        mark_df_statement(&mut statement);
        let error = state
            .statement_to_plan(statement)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Invalid function '__repark_values_cell'"),
            "{sql}: {error}"
        );
    }
}

#[tokio::test]
async fn an_outer_reference_to_a_retyped_values_column_follows_its_type() {
    let ctx = ctx();
    let values = "(VALUES (CAST(0.1 AS DECIMAL(10,2))), (abs(CAST('1e-20' AS DOUBLE)))) AS t(c)";
    for (sql, expected) in [
        (
            format!(
                "SELECT l.y FROM {values}, LATERAL (SELECT u.y FROM (VALUES (0.05), (0.5)) AS u(y) WHERE u.y > t.c) l"
            ),
            vec!["0.05", "0.5", "0.5"],
        ),
        (
            format!(
                "SELECT t.c FROM {values} WHERE EXISTS (SELECT 1 FROM (VALUES (CAST('1e-20' AS DOUBLE))) AS u(y) WHERE u.y = t.c)"
            ),
            vec!["1e-20"],
        ),
        (
            format!(
                "SELECT t.c FROM {values} WHERE t.c IN (SELECT u.y FROM (VALUES (CAST('1e-20' AS DOUBLE))) AS u(y) WHERE u.y >= t.c)"
            ),
            vec!["1e-20"],
        ),
    ] {
        let (data_type, got) = answer(&ctx, &sql, true).await;
        assert_eq!(data_type, DataType::Float64, "{sql}");
        assert_eq!(got, expected, "{sql}");
    }
}

#[tokio::test]
#[ignore = "phase timing, run by hand"]
async fn seat_v_phase_timing() {
    let session = |consume: bool| {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.parse_float_as_decimal = true;
        let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = Analyzer::new().rules;
        if consume {
            rules.insert(0, Arc::new(SparkValuesTyping));
        }
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_analyzer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(values_cell_udf().as_ref().clone());
        ctx.state()
    };
    let (base, seat) = (session(false), session(true));
    let name = super::values_cell_name();
    let rows = |odd: &str, wrap: bool| {
        (0..10_000)
            .map(|row| {
                let cell = if row == 9_999 { odd } else { "0.5" };
                if wrap {
                    format!("({name}({row}), {name}({cell}))")
                } else {
                    format!("({row}, {cell})")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let timed = |label: &str, started: std::time::Instant| {
        println!("{label}: {:.1} ms", started.elapsed().as_secs_f64() * 1e3);
    };
    for odd in ["0.5", "CAST(0.25 AS DOUBLE)"] {
        for (label, wrap, mark) in [
            ("plain", false, false),
            ("all-marked", true, false),
            ("seat", false, true),
        ] {
            let state = if label == "plain" { &base } else { &seat };
            let sql = format!(
                "SELECT count(*) FROM (VALUES {}) AS x(a, b)",
                rows(odd, wrap)
            );
            for _ in 0..2 {
                let started = std::time::Instant::now();
                let mut statement = state.sql_to_statement(&sql, &Dialect::Generic).unwrap();
                timed(&format!("{odd} {label} parse"), started);
                let started = std::time::Instant::now();
                if mark {
                    mark_df_statement(&mut statement);
                }
                timed(&format!("{odd} {label} mark"), started);
                let started = std::time::Instant::now();
                let logical = state.statement_to_plan(statement).await.unwrap();
                timed(&format!("{odd} {label} plan"), started);
                let started = std::time::Instant::now();
                let analyzed = state
                    .analyzer()
                    .execute_and_check(logical, state.config_options(), |_, _| {})
                    .unwrap();
                timed(&format!("{odd} {label} analyze"), started);
                let started = std::time::Instant::now();
                let optimized = state.optimize(&analyzed).unwrap();
                timed(&format!("{odd} {label} optimize"), started);
                assert!(!format!("{}", optimized.display_indent()).contains(name));
            }
        }
    }
}
