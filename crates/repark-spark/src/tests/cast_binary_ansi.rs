use super::super::*;
use super::common::*;

use datafusion::arrow::array::BinaryArray;

fn ansi_ctx(enabled: bool) -> (SessionContext, CatalogRegistry) {
    let config =
        crate::extension::apply_spark_float_as_decimal(datafusion::prelude::SessionConfig::new());
    let config = repark_functions::ansi::with_spark_ansi_config(config, enabled);
    let ctx = SessionContext::new_with_config(config);
    repark_functions::register_all(&ctx);
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    (ctx, CatalogRegistry::new())
}

async fn binary_answer(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    expr: &str,
) -> (Option<Vec<u8>>, DataType, bool) {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS b"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    let field = batches[0].schema().field(0).clone();
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap_or_else(|| panic!("`{expr}` must plan to Arrow Binary"));
    let value = (!column.is_null(0)).then(|| column.value(0).to_vec());
    (value, field.data_type().clone(), field.is_nullable())
}

async fn refusal(ctx: &SessionContext, catalogs: &CatalogRegistry, expr: &str) -> String {
    execute(ctx, catalogs, &format!("SELECT {expr} AS b"))
        .await
        .expect_err(&format!("`{expr}` must refuse"))
        .to_string()
}

#[tokio::test]
async fn int_to_binary_encodes_big_endian_widths_when_ansi_off() {
    let (ctx, catalogs) = ansi_ctx(false);
    for (expr, expected) in [
        ("CAST(CAST(1 AS TINYINT) AS BINARY)", vec![0x01]),
        ("CAST(CAST(1 AS SMALLINT) AS BINARY)", vec![0x00, 0x01]),
        ("CAST(1 AS BINARY)", vec![0x00, 0x00, 0x00, 0x01]),
        ("CAST(-1 AS BINARY)", vec![0xff, 0xff, 0xff, 0xff]),
        ("CAST(305419896 AS BINARY)", vec![0x12, 0x34, 0x56, 0x78]),
        (
            "CAST(CAST(1 AS BIGINT) AS BINARY)",
            vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
        ),
    ] {
        assert_eq!(
            binary_answer(&ctx, &catalogs, expr).await,
            (Some(expected), DataType::Binary, false),
            "`{expr}` must encode big-endian, non-null"
        );
    }
    assert_eq!(
        binary_answer(&ctx, &catalogs, "CAST(CAST(NULL AS INT) AS BINARY)").await,
        (None, DataType::Binary, true),
        "a NULL integral stays NULL and nullable"
    );
    assert_eq!(
        binary_answer(&ctx, &catalogs, "CAST('ab' AS BINARY)").await,
        (Some(b"ab".to_vec()), DataType::Binary, false),
        "the string cast is unaffected"
    );
}

#[tokio::test]
async fn never_castable_sources_refuse_without_suggestion_when_ansi_off() {
    let (ctx, catalogs) = ansi_ctx(false);
    for (expr, source) in [
        ("CAST(CAST(1.5 AS FLOAT) AS BINARY)", "FLOAT"),
        ("CAST(CAST(1.5 AS DOUBLE) AS BINARY)", "DOUBLE"),
        (
            "CAST(CAST(1.5 AS DECIMAL(10,2)) AS BINARY)",
            "DECIMAL(10,2)",
        ),
        ("CAST(true AS BINARY)", "BOOLEAN"),
        ("CAST(DATE '2024-01-01' AS BINARY)", "DATE"),
        (
            "CAST(TIMESTAMP '2024-01-01 00:00:00' AS BINARY)",
            "TIMESTAMP",
        ),
        ("CAST(INTERVAL '1' DAY AS BINARY)", "INTERVAL DAY"),
    ] {
        let message = refusal(&ctx, &catalogs, expr).await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]")
                && message.contains(&format!("cannot cast \"{source}\" to \"BINARY\""))
                && !message.contains("CAST_WITH_CONF_SUGGESTION"),
            "`{expr}` must refuse WITHOUT_SUGGESTION naming {source}, got: {message}"
        );
    }
}

#[tokio::test]
async fn int_casts_refuse_with_conf_suggestion_and_remedy_when_ansi_on() {
    let (ctx, catalogs) = ansi_ctx(true);
    for (expr, source) in [
        ("CAST(CAST(1 AS TINYINT) AS BINARY)", "TINYINT"),
        ("CAST(CAST(1 AS SMALLINT) AS BINARY)", "SMALLINT"),
        ("CAST(1 AS BINARY)", "INT"),
        ("CAST(-1 AS BINARY)", "INT"),
        ("CAST(CAST(1 AS BIGINT) AS BINARY)", "BIGINT"),
        ("CAST(CAST(NULL AS INT) AS BINARY)", "INT"),
    ] {
        let message = refusal(&ctx, &catalogs, expr).await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION]")
                && message.contains(&format!(
                    "cannot cast \"{source}\" to \"BINARY\" with ANSI mode on"
                ))
                && message.contains("spark.sql.ansi.enabled")
                && message.contains("SQLSTATE: 42K09"),
            "`{expr}` must refuse WITH_CONF_SUGGESTION naming {source}, got: {message}"
        );
    }
}

#[tokio::test]
async fn try_cast_int_refuses_without_suggestion_when_ansi_off() {
    let (ctx, catalogs) = ansi_ctx(false);
    let message = refusal(&ctx, &catalogs, "TRY_CAST(1 AS BINARY)").await;
    assert!(
        message.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]")
            && !message.contains("CAST_WITH_CONF_SUGGESTION"),
        "TRY_CAST must never carry the conf suggestion, got: {message}"
    );
}

#[tokio::test]
async fn string_cast_works_when_ansi_on() {
    let (ctx, catalogs) = ansi_ctx(true);
    assert_eq!(
        binary_answer(&ctx, &catalogs, "CAST('ab' AS BINARY)").await,
        (Some(b"ab".to_vec()), DataType::Binary, false),
        "the string cast works in both modes"
    );
}
