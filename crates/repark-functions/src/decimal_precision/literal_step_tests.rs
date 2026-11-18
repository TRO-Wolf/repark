use std::sync::Arc;

use datafusion::arrow::array::{Array, Decimal128Array, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::optimizer::Analyzer;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::Tokenizer;

use super::{dataframe_divide_operand, spell_integer_suffix, write_pick_minimum_precision};
use crate::analyze_eagerly;

fn spark_ctx(pick: bool) -> SessionContext {
    spark_ctx_in(pick, true)
}

fn spark_ctx_in(pick: bool, ansi: bool) -> SessionContext {
    let mut rules =
        crate::lambda_rebind::analyzer_rules_with_higher_order_preparation(Analyzer::new().rules)
            .expect("type_coercion seat");
    rules.extend(crate::analyzer_rules());
    let mut config = crate::ansi::with_spark_ansi_config(SessionConfig::new(), ansi);
    write_pick_minimum_precision(config.options_mut(), pick);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    crate::register_all(&ctx);
    let schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Decimal128(10, 2), true),
        Field::new("i", DataType::Int32, true),
        Field::new("j", DataType::Int32, true),
    ]));
    let decimals = Decimal128Array::from(vec![Some(251), Some(-251), None])
        .with_precision_and_scale(10, 2)
        .expect("decimal fixture");
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(decimals),
            Arc::new(Int32Array::from(vec![7, 8, 9])),
            Arc::new(Int32Array::from(vec![2, 2, 2])),
        ],
    )
    .expect("fixture batch");
    ctx.register_batch("t", batch).expect("register t");
    ctx
}

async fn first_column(ctx: &SessionContext, sql: &str) -> (DataType, Vec<String>) {
    let frame = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    column_text(&batches)
}

fn column_text(batches: &[RecordBatch]) -> (DataType, Vec<String>) {
    let data_type = batches[0].schema().field(0).data_type().clone();
    let mut values = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        for row in 0..column.len() {
            values.push(
                datafusion::arrow::util::display::array_value_to_string(column, row)
                    .expect("display"),
            );
        }
    }
    (data_type, values)
}

#[tokio::test]
async fn a_literal_divisor_takes_its_digits() {
    let ctx = spark_ctx(true);
    let (data_type, values) = first_column(&ctx, "SELECT d / 3 FROM t").await;
    assert_eq!(data_type, DataType::Decimal128(14, 6));
    assert_eq!(values, ["0.836667", "-0.836667", ""]);
}

#[tokio::test]
async fn a_literal_dividend_takes_its_digits() {
    let ctx = spark_ctx(true);
    let (data_type, values) = first_column(&ctx, "SELECT 3 / d FROM t").await;
    assert_eq!(data_type, DataType::Decimal128(14, 11));
    assert_eq!(values[0], "1.19521912351");
}

#[tokio::test]
async fn a_literal_modulus_takes_its_digits() {
    let ctx = spark_ctx(true);
    let (data_type, values) = first_column(&ctx, "SELECT d % 3 FROM t").await;
    assert_eq!(data_type, DataType::Decimal128(3, 2));
    assert_eq!(values, ["2.51", "-2.51", ""]);
}

#[tokio::test]
async fn mod_and_pmod_calls_take_the_remainder_type() {
    let ctx = spark_ctx(true);
    for sql in [
        "SELECT mod(d, 3) FROM t",
        "SELECT pmod(d, 3) FROM t",
        "SELECT mod(3, d) FROM t",
    ] {
        let (data_type, _) = first_column(&ctx, sql).await;
        assert_eq!(data_type, DataType::Decimal128(3, 2), "{sql}");
    }
    let (_, values) = first_column(&ctx, "SELECT pmod(d, 3) FROM t").await;
    assert_eq!(values, ["2.51", "0.49", ""]);
}

#[tokio::test]
async fn a_decimal_mod_call_answers_as_the_remainder_operator() {
    let ctx = spark_ctx(true);
    for (call, operator) in [
        ("SELECT mod(d, 3) FROM t", "SELECT d % 3 FROM t"),
        ("SELECT mod(d, -3) FROM t", "SELECT d % -3 FROM t"),
        ("SELECT mod(3, d) FROM t", "SELECT 3 % d FROM t"),
        ("SELECT mod(d, i) FROM t", "SELECT d % i FROM t"),
    ] {
        assert_eq!(
            first_column(&ctx, call).await,
            first_column(&ctx, operator).await,
            "{call}"
        );
    }
    let error = ctx
        .sql("SELECT mod(d, 0) FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect_err("a zero divisor raises with ANSI on");
    assert!(error.to_string().contains("DIVIDE_BY_ZERO"), "{error}");
    let (data_type, values) = first_column(&ctx, "SELECT mod(i, j) FROM t").await;
    assert_eq!(data_type, DataType::Int32);
    assert_eq!(values, ["1", "0", "1"]);
}

#[tokio::test]
async fn a_decimal_pmod_call_answers_as_spark_pmod_formula() {
    let ctx = spark_ctx(true);
    for (sql, data_type, expected) in [
        ("SELECT pmod(d, -3) FROM t", (3, 2), ["2.51", "-2.51", ""]),
        ("SELECT pmod(-3, d) FROM t", (3, 2), ["2.02", "-0.49", ""]),
        ("SELECT pmod(d, i) FROM t", (10, 2), ["2.51", "5.49", ""]),
    ] {
        let (answered, values) = first_column(&ctx, sql).await;
        assert_eq!(
            answered,
            DataType::Decimal128(data_type.0, data_type.1),
            "{sql}"
        );
        assert_eq!(values, expected, "{sql}");
    }
    for sql in [
        "SELECT pmod(d, 0) FROM t",
        "SELECT pmod(-3, d * 10000) FROM t",
    ] {
        let outcome = ctx.sql(sql).await.expect("plan").collect().await;
        assert!(outcome.is_err(), "{sql} raises with ANSI on");
    }
    let ctx = spark_ctx_in(true, false);
    let (data_type, values) = first_column(&ctx, "SELECT pmod(-3, d * 10000) FROM t").await;
    assert_eq!(data_type, DataType::Decimal128(3, 2));
    assert_eq!(values, ["", "-3.00", ""]);
    let (_, values) = first_column(&ctx, "SELECT pmod(d, 0) FROM t").await;
    assert_eq!(values, ["", "", ""]);
}

#[tokio::test]
async fn a_folded_divisor_keeps_the_integer_width() {
    let ctx = spark_ctx(true);
    let (folded, _) =
        first_column(&ctx, "SELECT d / (CAST(1 AS INT) + CAST(2 AS INT)) FROM t").await;
    assert_eq!(folded, DataType::Decimal128(21, 13));
    let (data_type, _) = first_column(&ctx, "SELECT d / CAST(3 AS DECIMAL(20,0)) FROM t").await;
    assert_eq!(data_type, DataType::Decimal128(31, 23));
}

#[tokio::test]
async fn pick_minimum_precision_false_types_every_literal_by_its_type() {
    let ctx = spark_ctx(false);
    for (literal, typed) in [
        ("SELECT d * 3 FROM t", "SELECT d * CAST(3 AS BIGINT) FROM t"),
        ("SELECT d / 3 FROM t", "SELECT d / CAST(3 AS BIGINT) FROM t"),
        ("SELECT d % 3 FROM t", "SELECT d % CAST(3 AS BIGINT) FROM t"),
    ] {
        let (left, _) = first_column(&ctx, literal).await;
        let (right, _) = first_column(&ctx, typed).await;
        assert_eq!(left, right, "{literal}");
    }
    let (data_type, _) = first_column(&ctx, "SELECT mod(d, 3) FROM t").await;
    assert_eq!(data_type, DataType::Decimal128(10, 2));
    let (data_type, _) = first_column(&spark_ctx(true), "SELECT d * 3 FROM t").await;
    assert_eq!(data_type, DataType::Decimal128(12, 2));
}

#[tokio::test]
async fn a_second_analyze_changes_nothing() {
    let ctx = spark_ctx(true);
    for sql in [
        "SELECT d / 3, d % 3, mod(d, 3), pmod(d, 3) FROM t",
        "SELECT pmod(-3, d), pmod(d, i), pmod(-3, d * 10000) FROM t",
        "SELECT avg(d / 3) FROM t",
    ] {
        let plan = ctx.state().create_logical_plan(sql).await.expect("plan");
        let once = analyze_eagerly(&ctx.state(), plan).expect("first analyze");
        let twice = analyze_eagerly(&ctx.state(), once.clone()).expect("second analyze");
        assert_eq!(once, twice, "{sql}");
    }
}

async fn frame_column(ctx: &SessionContext, expr: Expr) -> (DataType, Vec<String>) {
    let frame = ctx
        .table("t")
        .await
        .expect("table")
        .select(vec![expr])
        .expect("select");
    column_text(&frame.collect().await.expect("collect"))
}

#[tokio::test]
async fn the_dataframe_divide_types_a_decimal_operand_as_sql_does() {
    let ctx = spark_ctx(true);
    let quotient = dataframe_divide_operand(col("d")) / dataframe_divide_operand(lit(3_i32));
    let (data_type, values) = frame_column(&ctx, quotient).await;
    assert_eq!(data_type, DataType::Decimal128(14, 6));
    assert_eq!(values[0], "0.836667");
    let integers = dataframe_divide_operand(col("i")) / dataframe_divide_operand(col("j"));
    let (data_type, values) = frame_column(&ctx, integers).await;
    assert_eq!(data_type, DataType::Float64);
    assert_eq!(values[0], "3.5");
}

fn spelled(sql: &str, at: &str, sql_type: &str) -> String {
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize_with_location()
        .expect("tokenize");
    let number = tokens
        .iter()
        .position(|token| token.token.to_string() == at)
        .expect("number token");
    spell_integer_suffix(&tokens, number, number, false, sql_type, "X".to_string())
}

#[test]
fn a_suffixed_operand_is_marked_only_beside_arithmetic() {
    let marked = format!("{}(X)", crate::java_double::SUFFIX_LITERAL_NAME);
    assert_eq!(spelled("SELECT d * 3", "3", "BIGINT"), marked);
    assert_eq!(spelled("SELECT 3 / d", "3", "SMALLINT"), marked);
    assert_eq!(spelled("SELECT mod(d, 3)", "3", "BIGINT"), marked);
    assert_eq!(spelled("SELECT pmod(3, d)", "3", "BIGINT"), marked);
    assert_eq!(spelled("SELECT 3", "3", "BIGINT"), "X");
    assert_eq!(spelled("SELECT * FROM t LIMIT 3", "3", "BIGINT"), "X");
    assert_eq!(spelled("SELECT array(1, 3)", "3", "BIGINT"), "X");
    assert_eq!(spelled("SELECT d * 3", "3", "TINYINT"), "X");
}
