use super::super::*;
use super::common::*;

use datafusion::arrow::array::{
    BooleanArray, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
};
use repark_core::{SessionBuildConf, SessionExtension, SessionTimeZone};

/// A production-faithful Spark-door context: `SparkExtension::configure` plus registry and rules.
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
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    ctx
}

/// The single output batch of a one-row `SELECT`, with its Arrow field.
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

/// The `Utf8` value of a one-row select.
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

/// `configure` parses every statement with `Dialect::Databricks`.
#[test]
fn configure_parses_with_databricks_dialect() {
    let ctx = production_ctx(false);
    assert_eq!(
        ctx.state().config().options().sql_parser.dialect,
        datafusion::config::Dialect::Databricks
    );
}

/// `configure` carries `escapedStringLiterals=false` unless the builder sets it.
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

/// A present-but-unparsable flag value fail-louds naming the key.
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

/// BL9-0: a double-quoted literal is a non-null STRING.
#[tokio::test]
async fn double_quoted_literal_is_a_string() {
    let ctx = production_ctx(false);
    let (value, nullable) = utf8_value(&ctx, r#"SELECT "abc" AS s"#).await;
    assert_eq!(value, "abc");
    assert!(!nullable);
}

/// BL9-1/2/3: Spark escapes inside double quotes.
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

/// BL9-6: a double-quoted digits literal compares as a string.
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

/// BL10: the default door processes escapes; the verbatim door keeps them.
#[tokio::test]
async fn verbatim_flag_keeps_backslashes() {
    let off = production_ctx(false);
    let (processed, _) = utf8_value(&off, r"SELECT '\d' AS s").await;
    assert_eq!(processed, "d");
    let on = production_ctx(true);
    let (kept, _) = utf8_value(&on, r"SELECT '\d' AS s").await;
    assert_eq!(kept, "\\d");
}

/// BL12: an out-of-range `\U` keeps Java's two-char artifact.
#[tokio::test]
async fn out_of_range_u_keeps_java_artifact() {
    let ctx = production_ctx(false);
    let (value, _) = utf8_value(&ctx, r"SELECT '\U00110000' AS v").await;
    assert_eq!(value, "??");
    let (value, _) = utf8_value(&ctx, r"SELECT '\UFFFFFFFF' AS v").await;
    assert_eq!(value, "\u{d7bf}?");
}

/// BL6-sql-3 + DIV: `D`-suffixed numbers are DOUBLE with Spark's values.
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
            .value(0),
        2.0
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
            .value(0),
        1e200
    );
}

/// `L/S/Y/F/BD` suffixes keep their Spark Arrow types.
#[tokio::test]
async fn other_suffixes_keep_spark_types() {
    let ctx = production_ctx(false);
    let (_, data_type, nullable) = one_cell(&ctx, "SELECT 1L AS v").await;
    assert_eq!(data_type, DataType::Int64);
    assert!(!nullable);
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
            .value(0),
        1.5
    );
    let (batch, data_type, _) = one_cell(&ctx, "SELECT 1.5BD AS v").await;
    assert!(matches!(data_type, DataType::Decimal128(_, _)));
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128")
            .value(0)
            > 0
    );
}

/// A backticked alias survives the door as the field name.
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

/// Internal `SELECT * EXCLUDE` keeps working: the door reads it as `EXCEPT`.
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

/// `DROP TEMPORARY FUNCTION IF EXISTS` parses on the Spark door (valid Spark SQL;
/// the Databricks lexer has no `TEMPORARY`) and is a no-op for a missing function.
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

/// JD-exp-literal-types: exponent literals are DOUBLE, a plain decimal stays DECIMAL.
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
    let floats = |name: &str| {
        batches[0]
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64")
            .value(0)
    };
    assert_eq!(floats("a"), 1_000_000.0);
    assert_eq!(floats("b"), 100.0);
    assert_eq!(floats("d"), 0.001);
    assert_eq!(floats("e"), 1e21);
    assert_eq!(floats("f"), 5e-324);
    let (batch, data_type, _) = one_cell(&ctx, "SELECT CAST(1.0E6 AS DOUBLE) AS v").await;
    assert_eq!(data_type, DataType::Float64);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64")
            .value(0),
        1_000_000.0
    );
}

/// FNP4B-sql-struct-lit: field access on a `named_struct` call answers.
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
