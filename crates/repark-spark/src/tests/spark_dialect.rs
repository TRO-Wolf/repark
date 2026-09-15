use super::super::*;
use super::common::*;

use datafusion::arrow::array::{
    BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array, Int64Array,
};
use repark_core::{SessionBuildConf, SessionExtension, SessionTimeZone};

fn production_ctx(keep_verbatim: bool) -> SessionContext {
    let mut conf = HashMap::new();
    if keep_verbatim {
        conf.insert(
            "spark.sql.parser.escapedStringLiterals".to_string(),
            "true".to_string(),
        );
    }
    let zone = SessionTimeZone::default();
    let config = SparkExtension
        .configure(
            SessionBuildConf {
                conf: &conf,
                session_time_zone: &zone,
            },
            datafusion::prelude::SessionConfig::new(),
        )
        .unwrap();
    let state = datafusion::execution::SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_analyzer_rules(
            SparkExtension
                .configure_analyzer_rules(datafusion::optimizer::Analyzer::new().rules)
                .unwrap(),
        )
        .build();
    let ctx = SessionContext::new_with_state(state);
    repark_functions::register_all(&ctx);
    ctx.register_udf(crate::spark_as_udf().as_ref().clone());
    ctx.register_udf(crate::suffix_literal_udf().as_ref().clone());
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx.add_analyzer_rule(Arc::new(crate::FoldSparkNumericCasts));
    ctx.add_analyzer_rule(Arc::new(crate::SparkProjectionDisplay));
    ctx
}

async fn one_cell(ctx: &SessionContext, sql: &str) -> (RecordBatch, DataType, bool) {
    let batches = execute(ctx, &CatalogRegistry::new(), sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1, "`{sql}` must yield one batch");
    assert_eq!(batches[0].num_rows(), 1, "`{sql}` must yield one row");
    let field = batches[0].schema().field(0).clone();
    let data_type = field.data_type().clone();
    let nullable = field.is_nullable();
    (batches[0].clone(), data_type, nullable)
}

async fn utf8_value(ctx: &SessionContext, sql: &str) -> (String, bool) {
    let (batch, data_type, nullable) = one_cell(ctx, sql).await;
    assert_eq!(data_type, DataType::Utf8, "`{sql}` must be Utf8");
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    (column.value(0).to_string(), nullable)
}

#[test]
fn configure_parses_with_databricks_dialect() {
    let ctx = production_ctx(false);
    assert_eq!(
        ctx.state().config().options().sql_parser.dialect,
        datafusion::config::Dialect::Databricks
    );
}

#[test]
fn configure_defaults_verbatim_off_and_honors_true() {
    let off = production_ctx(false);
    assert!(!crate::spark_literals::escaped_verbatim_from_options(
        off.state().config().options()
    ));
    let on = production_ctx(true);
    assert!(crate::spark_literals::escaped_verbatim_from_options(
        on.state().config().options()
    ));
}

#[test]
fn configure_refuses_verbatim_notabool() {
    let mut conf = HashMap::new();
    conf.insert(
        "spark.sql.parser.escapedStringLiterals".to_string(),
        "notabool".to_string(),
    );
    let zone = SessionTimeZone::default();
    let error = SparkExtension
        .configure(
            SessionBuildConf {
                conf: &conf,
                session_time_zone: &zone,
            },
            datafusion::prelude::SessionConfig::new(),
        )
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("spark.sql.parser.escapedStringLiterals"),
        "error must name the key: {error}"
    );
}

#[tokio::test]
async fn double_quoted_literal_is_a_string() {
    let ctx = production_ctx(false);
    let (value, nullable) = utf8_value(&ctx, r#"SELECT "abc" AS s"#).await;
    assert_eq!(value, "abc");
    assert!(!nullable);
}

#[tokio::test]
async fn double_quoted_escapes_match_spark() {
    let ctx = production_ctx(false);
    let (quote, _) = utf8_value(&ctx, r#"SELECT "a\"b" AS s"#).await;
    assert_eq!(quote, "a\"b");
    let batches = execute(
        &ctx,
        &CatalogRegistry::new(),
        r#"SELECT length("a\nb") AS n"#,
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32")
            .value(0),
        3
    );
    let (squote, _) = utf8_value(&ctx, r#"SELECT "it\'s" AS s"#).await;
    assert_eq!(squote, "it's");
}

#[tokio::test]
async fn double_quoted_digits_compare_as_string() {
    let ctx = production_ctx(false);
    let (batch, data_type, _) = one_cell(&ctx, r#"SELECT "1" = 1 AS b"#).await;
    assert_eq!(data_type, DataType::Boolean);
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Boolean")
            .value(0)
    );
}

#[tokio::test]
async fn verbatim_flag_keeps_backslashes() {
    let off = production_ctx(false);
    let (processed, _) = utf8_value(&off, r"SELECT '\d' AS s").await;
    assert_eq!(processed, "d");
    let on = production_ctx(true);
    let (kept, _) = utf8_value(&on, r"SELECT '\d' AS s").await;
    assert_eq!(kept, "\\d");
}

#[tokio::test]
async fn out_of_range_u_keeps_java_artifact() {
    let ctx = production_ctx(false);
    let (value, _) = utf8_value(&ctx, r"SELECT '\U00110000' AS v").await;
    assert_eq!(value, "??");
    let (value, _) = utf8_value(&ctx, r"SELECT '\UFFFFFFFF' AS v").await;
    assert_eq!(value, "\u{d7bf}?");
}

#[tokio::test]
async fn d_suffix_is_double() {
    let ctx = production_ctx(false);
    let (batch, data_type, _) = one_cell(&ctx, "SELECT rint(2.5D) AS v").await;
    assert_eq!(data_type, DataType::Float64);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64")
            .value(0)
            .to_bits(),
        2.0f64.to_bits()
    );
    let (batch, data_type, _) = one_cell(&ctx, "SELECT ceil(-1.5D) AS v").await;
    assert_eq!(data_type, DataType::Int64);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0),
        -1
    );
    let (batch, _, _) = one_cell(&ctx, "SELECT 1e200D AS v").await;
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64")
            .value(0)
            .to_bits(),
        1e200f64.to_bits()
    );
}

#[tokio::test]
async fn other_suffixes_keep_spark_types() {
    let ctx = production_ctx(false);
    let (_, data_type, nullable) = one_cell(&ctx, "SELECT 1L AS v").await;
    assert_eq!(data_type, DataType::Int64);
    assert!(!nullable);
    let (batch, data_type, nullable) = one_cell(&ctx, "SELECT -9223372036854775808L AS v").await;
    assert_eq!(data_type, DataType::Int64);
    assert!(!nullable);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0),
        i64::MIN
    );
    let (batch, data_type, _) = one_cell(&ctx, "SELECT 1S AS v").await;
    assert_eq!(data_type, DataType::Int16);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("Int16")
            .value(0),
        1
    );
    let (batch, data_type, _) = one_cell(&ctx, "SELECT 1Y AS v").await;
    assert_eq!(data_type, DataType::Int8);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Int8")
            .value(0),
        1
    );
    let (batch, data_type, _) = one_cell(&ctx, "SELECT 1.5F AS v").await;
    assert_eq!(data_type, DataType::Float32);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32")
            .value(0)
            .to_bits(),
        1.5f32.to_bits()
    );
    let (batch, data_type, nullable) = one_cell(&ctx, "SELECT 1.5BD AS v").await;
    assert_eq!(data_type, DataType::Decimal128(2, 1));
    assert!(!nullable);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128")
            .value(0),
        15
    );
}

#[tokio::test]
async fn bd_literals_take_precision_from_digits() {
    let ctx = production_ctx(false);
    let (_, data_type, nullable) = one_cell(&ctx, "SELECT 10BD AS v").await;
    assert_eq!(data_type, DataType::Decimal128(2, 0));
    assert!(!nullable);
    let (_, data_type, _) = one_cell(&ctx, "SELECT 0.001BD AS v").await;
    assert_eq!(data_type, DataType::Decimal128(3, 3));
    let (batch, data_type, _) = one_cell(&ctx, "SELECT 1.5e2BD AS v").await;
    assert_eq!(data_type, DataType::Decimal128(3, 0));
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128")
            .value(0),
        150
    );
}

#[tokio::test]
async fn typed_numeric_literals_are_non_null() {
    let ctx = production_ctx(false);
    for sql in [
        "SELECT 2.5D AS v",
        "SELECT 1e200D AS v",
        "SELECT .5D AS v",
        "SELECT 5.D AS v",
        "SELECT 1E-2D AS v",
        "SELECT 1e3 AS v",
        "SELECT 1.e2 AS v",
    ] {
        let (_, data_type, nullable) = one_cell(&ctx, sql).await;
        assert_eq!(data_type, DataType::Float64, "{sql}");
        assert!(!nullable, "{sql}");
    }
    let (_, data_type, nullable) = one_cell(&ctx, "SELECT 1.5F AS v").await;
    assert_eq!(data_type, DataType::Float32);
    assert!(!nullable);
}

#[tokio::test]
async fn out_of_range_integer_suffix_is_invalid_numeric_literal() {
    let ctx = production_ctx(false);
    let error = execute(&ctx, &CatalogRegistry::new(), "SELECT 128Y AS v")
        .await
        .expect_err("128Y is out of range");
    assert!(
        error.to_string().contains("INVALID_NUMERIC_LITERAL_RANGE"),
        "{error}"
    );
    let error = execute(&ctx, &CatalogRegistry::new(), "SELECT 40000S AS v")
        .await
        .expect_err("40000S is out of range");
    assert!(
        error.to_string().contains("INVALID_NUMERIC_LITERAL_RANGE"),
        "{error}"
    );
}

#[tokio::test]
async fn signed_integer_suffix_minima_answer() {
    let ctx = production_ctx(false);
    let (batch, data_type, nullable) = one_cell(&ctx, "SELECT -128Y AS v").await;
    assert_eq!(data_type, DataType::Int8);
    assert!(!nullable);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("Int8")
            .value(0),
        i8::MIN
    );
    let (batch, data_type, nullable) = one_cell(&ctx, "SELECT -32768S AS v").await;
    assert_eq!(data_type, DataType::Int16);
    assert!(!nullable);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("Int16")
            .value(0),
        i16::MIN
    );
    for (sql, value) in [
        ("SELECT -1Y AS v", -1i64),
        ("SELECT 127Y AS v", 127i64),
        ("SELECT -1S AS v", -1i64),
        ("SELECT 32767S AS v", 32767i64),
    ] {
        let (batch, data_type, nullable) = one_cell(&ctx, sql).await;
        assert!(!nullable, "{sql}");
        let actual: i64 = match data_type {
            DataType::Int8 => batch
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Int8")
                .value(0)
                .into(),
            DataType::Int16 => batch
                .column(0)
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("Int16")
                .value(0)
                .into(),
            other => panic!("{sql} kept unexpected type {other}"),
        };
        assert_eq!(actual, value, "{sql}");
    }
    let (batch, data_type, nullable) = one_cell(&ctx, "SELECT -1L AS v").await;
    assert_eq!(data_type, DataType::Int64);
    assert!(!nullable);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64")
            .value(0),
        -1
    );
    for sql in ["SELECT -129Y AS v", "SELECT -32769S AS v"] {
        let error = execute(&ctx, &CatalogRegistry::new(), sql)
            .await
            .expect_err("out-of-range signed suffix refuses");
        assert!(
            error.to_string().contains("INVALID_NUMERIC_LITERAL_RANGE"),
            "{sql}: {error}"
        );
    }
}

#[tokio::test]
async fn unaliased_suffix_names_come_from_value_text() {
    let ctx = production_ctx(false);
    for (sql, name) in [
        ("SELECT 1.5BD", "1.5"),
        ("SELECT 1e200D", "1.0E200"),
        ("SELECT 2.5D", "2.5"),
        ("SELECT 1D", "1.0"),
        ("SELECT 1L", "1"),
        ("SELECT 1.5F", "1.5"),
        ("SELECT 1Y", "1"),
        ("SELECT 1S", "1"),
        ("SELECT -128Y", "-128"),
        ("SELECT -0.0BD", "-0.0"),
        ("SELECT * FROM (SELECT 1.5BD) t", "1.5"),
        ("SELECT * FROM (SELECT 1e200D) t", "1.0E200"),
        ("SELECT * FROM (SELECT 1D) t", "1"),
    ] {
        let (batch, _, _) = one_cell(&ctx, sql).await;
        assert_eq!(batch.schema().field(0).name(), name, "{sql}");
    }
    let batches = execute(
        &ctx,
        &CatalogRegistry::new(),
        "SELECT * FROM (SELECT 1D UNION ALL SELECT 2D) t",
    )
    .await
    .unwrap_or_else(|error| panic!("union of suffix literals failed: {error}"))
    .collect()
    .await
    .unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "1");
}

#[tokio::test]
async fn bigint_overflow_is_invalid_numeric_literal() {
    let ctx = production_ctx(false);
    for sql in [
        "SELECT 9223372036854775808L AS v",
        "SELECT -9223372036854775809L AS v",
        "SELECT -(9223372036854775808L) AS v",
    ] {
        let error = execute(&ctx, &CatalogRegistry::new(), sql)
            .await
            .expect_err("out-of-range BIGINT refuses");
        assert!(
            error.to_string().contains("INVALID_NUMERIC_LITERAL_RANGE"),
            "{sql}: {error}"
        );
    }
}

#[tokio::test]
async fn exponent_l_and_zero_x_hex_are_unresolved_identifiers() {
    let ctx = production_ctx(false);
    let error = execute(&ctx, &CatalogRegistry::new(), "SELECT 1e3L AS v")
        .await
        .expect_err("1e3L is an identifier");
    assert!(
        error.to_string().contains("1e3L") || error.to_string().contains("No field named"),
        "{error}"
    );
    let error = execute(&ctx, &CatalogRegistry::new(), "SELECT 0x1D AS v")
        .await
        .expect_err("0x1D is an identifier");
    assert!(
        error.to_string().contains("0x1D") || error.to_string().contains("No field named"),
        "{error}"
    );
}

#[tokio::test]
async fn chained_struct_field_access_on_call_result() {
    let ctx = production_ctx(false);
    let (batch, data_type, nullable) = one_cell(
        &ctx,
        "SELECT named_struct('s', named_struct('a', 1)).s.a AS v",
    )
    .await;
    assert_eq!(data_type, DataType::Int32);
    assert!(!nullable);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32")
            .value(0),
        1
    );
}

#[tokio::test]
async fn backtick_ident_survives_the_door() {
    let ctx = production_ctx(false);
    let batches = execute(&ctx, &CatalogRegistry::new(), "SELECT 1 AS `my col`")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "my col");
}

#[tokio::test]
async fn wildcard_exclude_reads_as_except() {
    let ctx = production_ctx(false);
    let batches = execute(
        &ctx,
        &CatalogRegistry::new(),
        "SELECT * EXCLUDE (b) FROM (SELECT 1 AS a, 2 AS b) t",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "a");
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32")
            .value(0),
        1
    );
}

#[tokio::test]
async fn drop_temporary_function_if_exists_is_a_noop() {
    let ctx = production_ctx(false);
    let batches = execute(
        &ctx,
        &CatalogRegistry::new(),
        "DROP TEMPORARY FUNCTION IF EXISTS no_such_function_xyz",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

#[tokio::test]
async fn exponent_literal_is_double() {
    let ctx = production_ctx(false);
    let batches = execute(
        &ctx,
        &CatalogRegistry::new(),
        "SELECT 1.0E6 AS a, 1E2 AS b, 1.5 AS c, 1e-3 AS d, 1.0E21 AS e, 4.9E-324 AS f",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let schema = batches[0].schema();
    for name in ["a", "b", "d", "e", "f"] {
        assert_eq!(
            schema.field_with_name(name).unwrap().data_type(),
            &DataType::Float64,
            "{name} must be DOUBLE"
        );
    }
    assert_eq!(
        schema.field_with_name("c").unwrap().data_type(),
        &DataType::Decimal128(2, 1)
    );
    let bits = |name: &str| {
        batches[0]
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64")
            .value(0)
            .to_bits()
    };
    assert_eq!(bits("a"), 1_000_000.0f64.to_bits());
    assert_eq!(bits("b"), 100.0f64.to_bits());
    assert_eq!(bits("d"), 0.001f64.to_bits());
    assert_eq!(bits("e"), 1e21f64.to_bits());
    assert_eq!(bits("f"), 5e-324f64.to_bits());
    let (batch, data_type, _) = one_cell(&ctx, "SELECT CAST(1.0E6 AS DOUBLE) AS v").await;
    assert_eq!(data_type, DataType::Float64);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64")
            .value(0)
            .to_bits(),
        1_000_000.0f64.to_bits()
    );
}

#[tokio::test]
async fn struct_field_access_on_call_result() {
    let ctx = production_ctx(false);
    let (batch, data_type, nullable) = one_cell(&ctx, "SELECT named_struct('a', 1).a AS v").await;
    assert_eq!(data_type, DataType::Int32);
    assert!(!nullable);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32")
            .value(0),
        1
    );
}

#[test]
fn fromless_delete_rewrites_to_delete_from() {
    let rewritten =
        crate::spark_literals::canonicalize("DELETE ice.sales.tgt WHERE id = 1").expect("rewrites");
    assert_eq!(rewritten.as_ref(), "DELETE FROM ice.sales.tgt WHERE id = 1");
    let rewritten =
        crate::spark_literals::canonicalize("WHEN MATCHED THEN DELETE OUTPUT DELETED.*")
            .expect("rewrites");
    assert_eq!(
        rewritten.as_ref(),
        "WHEN MATCHED THEN DELETE OUTPUT DELETED.*"
    );
}

#[test]
fn numeric_suffix_guard_skips_bare_decimals() {
    for sql in [
        "SELECT 1.5 AS v",
        "SELECT 0.5 AS c_0, 1.5 AS c_1",
        "SELECT * FROM t WHERE x > 2.5",
    ] {
        assert!(
            !crate::spark_literals::sql_may_have_numeric_suffix(sql),
            "{sql}"
        );
    }
    for sql in [
        "SELECT 1.5D AS v",
        "SELECT 1.5BD AS v",
        "SELECT 1L AS v",
        "SELECT 1Y AS v",
        "SELECT 1S AS v",
        "SELECT 1.5F AS v",
        "SELECT 1e3 AS v",
        "SELECT 1.e2 AS v",
        "SELECT .5D AS v",
        "SELECT 5.D AS v",
    ] {
        assert!(
            crate::spark_literals::sql_may_have_numeric_suffix(sql),
            "{sql}"
        );
    }
}
