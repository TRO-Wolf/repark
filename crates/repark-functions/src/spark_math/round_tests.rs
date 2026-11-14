use datafusion::arrow::array::{Array, AsArray, Decimal128Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type, Int32Type};
use datafusion::prelude::SessionContext;

fn ctx() -> SessionContext {
    let ctx = SessionContext::new();
    crate::register_all(&ctx);
    for rule in crate::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

async fn run(ctx: &SessionContext, sql: &str) -> datafusion::common::Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

async fn one(sql: &str) -> RecordBatch {
    let ctx = ctx();
    let batches = run(&ctx, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    batches
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("{sql}: no batch"))
}

async fn double_bits(sql: &str) -> (DataType, u64) {
    let batch = one(sql).await;
    let column = batch.column(0);
    (
        column.data_type().clone(),
        column.as_primitive::<Float64Type>().value(0).to_bits(),
    )
}

async fn float_bits(sql: &str) -> (DataType, u32) {
    let batch = one(sql).await;
    let column = batch.column(0);
    (
        column.data_type().clone(),
        column.as_primitive::<Float32Type>().value(0).to_bits(),
    )
}

#[tokio::test]
async fn round_double_rounds_the_jdk_text_half_up() {
    for (sql, want) in [
        ("SELECT round(CAST('1.005' AS DOUBLE), 2)", 1.01f64),
        ("SELECT round(CAST('0.285' AS DOUBLE), 2)", 0.29),
        ("SELECT round(CAST('1.255' AS DOUBLE), 2)", 1.26),
        (
            "SELECT round(CAST('-22.129547339983247' AS DOUBLE), 18)",
            -22.129_547_339_983_247,
        ),
        ("SELECT round(CAST('1e307' AS DOUBLE), 2)", 1e307),
        (
            "SELECT round(CAST('1.949133209895265e16' AS DOUBLE), -2)",
            1.949_133_209_895_26e16,
        ),
        ("SELECT round(CAST('1.005' AS DOUBLE), 309)", 1.005),
        ("SELECT round(CAST('1.5' AS DOUBLE), -400)", 0.0),
        ("SELECT round(CAST('2.5' AS DOUBLE))", 3.0),
        ("SELECT round(CAST('-0.0' AS DOUBLE), 2)", 0.0),
        ("SELECT round(CAST('-0.4' AS DOUBLE), 0)", 0.0),
    ] {
        assert_eq!(
            double_bits(sql).await,
            (DataType::Float64, want.to_bits()),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn bround_double_rounds_the_jdk_text_half_even() {
    for (sql, want) in [
        ("SELECT bround(CAST('2.675' AS DOUBLE), 2)", 2.68f64),
        ("SELECT bround(CAST('1.005' AS DOUBLE), 2)", 1.0),
        ("SELECT bround(CAST('0.125' AS DOUBLE), 2)", 0.12),
        ("SELECT bround(CAST('-0.0' AS DOUBLE), 2)", 0.0),
        (
            "SELECT bround(CAST('-22.129547339983247' AS DOUBLE), 18)",
            -22.129_547_339_983_247,
        ),
    ] {
        assert_eq!(
            double_bits(sql).await,
            (DataType::Float64, want.to_bits()),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn float_stays_float_and_rounds_the_widened_text() {
    for (sql, want) in [
        ("SELECT round(CAST('1.255' AS FLOAT), 2)", 1.25f32),
        ("SELECT bround(CAST('1.255' AS FLOAT), 2)", 1.25),
        ("SELECT round(CAST('-0.0' AS FLOAT), 2)", 0.0),
        ("SELECT bround(CAST('2.5' AS FLOAT), 0)", 2.0),
    ] {
        assert_eq!(
            float_bits(sql).await,
            (DataType::Float32, want.to_bits()),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn null_scale_answers_null_of_the_input_type() {
    for (sql, want) in [
        (
            "SELECT round(CAST('1.5' AS DOUBLE), CAST(NULL AS INT))",
            DataType::Float64,
        ),
        (
            "SELECT round(CAST('1.5' AS FLOAT), CAST(NULL AS INT))",
            DataType::Float32,
        ),
        (
            "SELECT bround(CAST('1.5' AS FLOAT), CAST(NULL AS INT))",
            DataType::Float32,
        ),
    ] {
        let batch = one(sql).await;
        assert_eq!(batch.column(0).data_type(), &want, "{sql}");
        assert!(batch.column(0).is_null(0), "{sql}");
    }
}

#[tokio::test]
async fn a_column_scale_is_refused_as_spark_does() {
    let ctx = ctx();
    for sql in [
        "SELECT round(v, s) FROM (VALUES (CAST('1.005' AS DOUBLE), 2)) AS t(v, s)",
        "SELECT round(v, s) FROM (VALUES (CAST('1.005' AS FLOAT), 2)) AS t(v, s)",
        "SELECT bround(v, s) FROM (VALUES (CAST('1.005' AS DOUBLE), 2)) AS t(v, s)",
    ] {
        let error = run(&ctx, sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sql} answered"))
            .to_string();
        assert!(error.contains("NON_FOLDABLE_INPUT"), "{sql}: {error}");
    }
}

#[tokio::test]
async fn integer_and_decimal_arms_keep_their_kernel() {
    let batch = one("SELECT round(CAST(25 AS INT), -1)").await;
    assert_eq!(batch.column(0).as_primitive::<Int32Type>().value(0), 30);
    let batch = one("SELECT round(CAST('1.005' AS DECIMAL(10,3)), 2)").await;
    assert_eq!(batch.column(0).data_type(), &DataType::Decimal128(10, 3));
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap_or_else(|| panic!("decimal output"));
    assert_eq!(values.value(0), 1010);
}
