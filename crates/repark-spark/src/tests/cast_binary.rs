//! SQP-1 pins — `CAST(x AS BINARY)` on the Spark SQL door.
//!
//! Oracle: PySpark 4.1.2 (`<pyspark-4.1.2-oracle>`), ANSI ON. A legal cast plans to Arrow `Binary`
//! (value AND type); an illegal source (INT / BIGINT / DECIMAL / BOOLEAN / DATE) refuses with
//! Spark's `DATATYPE_MISMATCH` naming the source; `VARBINARY` keeps refusing; a `BINARY` DDL column
//! is untouched. Reverting `BINARY`→`BYTEA` reds the legal casts; reverting the refuse reds the refusals.
use super::super::*;
use super::common::*;

use datafusion::arrow::array::BinaryArray;

/// The `Binary` value of `SELECT <expr> AS b` (row 0), asserting Arrow type `Binary`; `None` is NULL.
async fn binary_row0(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    expr: &str,
) -> Option<Vec<u8>> {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS b"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Binary,
        "`{expr}` must plan to Arrow Binary (Spark BINARY)"
    );
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Binary column");
    if column.is_null(0) {
        None
    } else {
        Some(column.value(0).to_vec())
    }
}

/// The `Utf8` value of `SELECT <expr> AS s` (for the `hex(...)` / round-trip probes).
async fn utf8_row0(ctx: &SessionContext, catalogs: &CatalogRegistry, expr: &str) -> String {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS s"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    let column = datafusion::arrow::compute::cast(batches[0].column(0), &DataType::Utf8).unwrap();
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 after cast")
        .value(0)
        .to_string()
}

/// A legal `CAST … AS BINARY` plans to Arrow `Binary` with the source's UTF-8 bytes (B1, B8, B9,
/// B10, B15) — value AND type; `TRY_CAST` is the same. Escape processing composes with the cast
/// (B15: `hex(CAST('\t' AS BINARY))` = `09`).
/// pins: sqp-1-spark-string-literals/C-009
#[tokio::test]
async fn cast_string_to_binary_plans_and_round_trips() {
    let (ctx, catalogs) = binary_expr_ctx();
    // B1: string → binary is the UTF-8 bytes.
    assert_eq!(
        binary_row0(&ctx, &catalogs, "CAST('abc' AS BINARY)").await,
        Some(b"abc".to_vec())
    );
    // B9: TRY_CAST is the same.
    assert_eq!(
        binary_row0(&ctx, &catalogs, "TRY_CAST('abc' AS BINARY)").await,
        Some(b"abc".to_vec())
    );
    // B8: NULL binary, and a re-cast of a binary value is a no-op.
    assert_eq!(
        binary_row0(&ctx, &catalogs, "CAST(NULL AS BINARY)").await,
        None
    );
    assert_eq!(
        binary_row0(&ctx, &catalogs, "CAST(CAST('x' AS BINARY) AS BINARY)").await,
        Some(b"x".to_vec())
    );
    // B10: round trip through STRING, and the multibyte hex.
    assert_eq!(
        utf8_row0(&ctx, &catalogs, "CAST(CAST('héllo' AS BINARY) AS STRING)").await,
        "héllo"
    );
    assert_eq!(
        utf8_row0(&ctx, &catalogs, "hex(CAST('héllo' AS BINARY))").await,
        "68C3A96C6C6F"
    );
    // B15: the escape reaches the cast — `'\t'` is one TAB byte, hex `09`.
    assert_eq!(
        utf8_row0(&ctx, &catalogs, "hex(CAST('\\t' AS BINARY))").await,
        "09"
    );
    // `::BINARY` and SAFE-style spellings ride the same rewrite.
    assert_eq!(
        binary_row0(&ctx, &catalogs, "'abc'::BINARY").await,
        Some(b"abc".to_vec())
    );
}

/// The column path (B13): `CAST(c AS BINARY)` over a STRING column with a NULL row yields the
/// bytes and a NULL, at Arrow `Binary`.
/// pins: sqp-1-spark-string-literals/C-009
#[tokio::test]
async fn cast_binary_over_a_string_column() {
    let (ctx, catalogs) = binary_expr_ctx();
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT CAST(c AS BINARY) AS b FROM VALUES ('ab'), (CAST(NULL AS STRING)) AS t(c) ORDER BY b",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Binary,
        "the column cast must be Arrow Binary"
    );
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Binary column");
    let mut seen: Vec<Option<Vec<u8>>> = (0..column.len())
        .map(|index| (!column.is_null(index)).then(|| column.value(index).to_vec()))
        .collect();
    seen.sort();
    assert_eq!(seen, vec![None, Some(b"ab".to_vec())]);
}

/// A source Spark refuses (INT / BIGINT / DECIMAL / BOOLEAN / DATE) is an analysis error naming the
/// source type and Spark's `DATATYPE_MISMATCH` — never DataFusion's silent int→bytes cast (B2–B7).
/// `VARBINARY` keeps refusing (B12).
/// pins: sqp-1-spark-string-literals/C-009
#[tokio::test]
async fn cast_to_binary_refuses_illegal_sources() {
    let (ctx, catalogs) = binary_expr_ctx();
    // Integer sources cite CAST_WITH_CONF_SUGGESTION (ANSI-off would encode them — B11 tabled).
    for (expr, source) in [
        ("CAST(CAST(1 AS INT) AS BINARY)", "INT"),
        ("CAST(1L AS BINARY)", "BIGINT"),
    ] {
        let error = execute(&ctx, &catalogs, &format!("SELECT {expr}"))
            .await
            .expect_err(&format!("`{expr}` must refuse"))
            .to_string();
        assert!(
            error.contains("DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION")
                && error.contains(source)
                && error.contains("BINARY"),
            "`{expr}` must name {source} + DATATYPE_MISMATCH, got: {error}"
        );
    }
    // Non-integer sources cite CAST_WITHOUT_SUGGESTION.
    for (expr, source) in [
        ("CAST(CAST(1.5 AS DECIMAL(3,1)) AS BINARY)", "DECIMAL(3,1)"),
        ("CAST(true AS BINARY)", "BOOLEAN"),
        ("CAST(DATE '2024-01-01' AS BINARY)", "DATE"),
    ] {
        let error = execute(&ctx, &catalogs, &format!("SELECT {expr}"))
            .await
            .expect_err(&format!("`{expr}` must refuse"))
            .to_string();
        assert!(
            error.contains("DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION") && error.contains(source),
            "`{expr}` must name {source} + DATATYPE_MISMATCH, got: {error}"
        );
    }
    // B12: VARBINARY is never rewritten, so it keeps DataFusion's unsupported-type refusal.
    let varbinary = execute(&ctx, &catalogs, "SELECT CAST('abc' AS VARBINARY)")
        .await
        .expect_err("VARBINARY must refuse")
        .to_string();
    assert!(
        varbinary.contains("VARBINARY"),
        "VARBINARY refusal must name the type, got: {varbinary}"
    );
}

/// `TRY_CAST(<x> AS BINARY)` over a Spark-illegal source refuses like `CAST` but always with
/// `CAST_WITHOUT_SUGGESTION`, never the `CAST_WITH_CONF_SUGGESTION` clause a plain `CAST` of an
/// integer carries. Measured `<pyspark-4.1.2-oracle>`: `TRY_CAST(1 / 1L / true AS BINARY)` all
/// report `CAST_WITHOUT_SUGGESTION`. Reverting the cast-kind thread reds the INT/BIGINT lines.
/// pins: sqp-1-spark-string-literals/C-009
#[tokio::test]
async fn try_cast_to_binary_never_suggests_ansi_off() {
    let (ctx, catalogs) = binary_expr_ctx();
    for (expr, source) in [
        ("TRY_CAST(CAST(1 AS INT) AS BINARY)", "INT"),
        ("TRY_CAST(1L AS BINARY)", "BIGINT"),
        ("TRY_CAST(true AS BINARY)", "BOOLEAN"),
    ] {
        let error = execute(&ctx, &catalogs, &format!("SELECT {expr}"))
            .await
            .expect_err(&format!("`{expr}` must refuse"))
            .to_string();
        assert!(
            error.contains("DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION")
                && error.contains(source)
                && !error.contains("CAST_WITH_CONF_SUGGESTION"),
            "`{expr}` must refuse WITHOUT the ANSI-off suggestion, naming {source}, got: {error}"
        );
    }
    // Control: a plain `CAST` of the same integer carries the suggestion — the arms split by cast kind.
    let plain = execute(&ctx, &catalogs, "SELECT CAST(1L AS BINARY)")
        .await
        .expect_err("plain CAST must refuse")
        .to_string();
    assert!(
        plain.contains("DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION"),
        "a plain CAST of BIGINT keeps CAST_WITH_CONF_SUGGESTION, got: {plain}"
    );
}

/// The cast rewrite must not touch a `BINARY` **column** in DDL: `CREATE TABLE (b BINARY)` still
/// creates an Iceberg `binary` column (the rewrite is scoped to `Expr::Cast`; DDL is intercepted first).
/// pins: sqp-1-spark-string-literals/C-009
#[tokio::test]
async fn binary_ddl_column_is_unchanged() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.bincol (id BIGINT, payload BINARY) USING iceberg",
    )
    .await;
    let table = load_sales_table(&catalogs, "bincol").await;
    let field = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .find(|field| field.name == "payload")
        .expect("payload column");
    assert_eq!(
        field.field_type.to_string(),
        "binary",
        "a BINARY DDL column must stay Iceberg binary"
    );
}

/// A `SessionContext` + empty `CatalogRegistry` wired like production (Spark registry, analyzer
/// rules, ANSI ON, `parse_float_as_decimal`) for the cast pins that need no catalog.
fn binary_expr_ctx() -> (SessionContext, CatalogRegistry) {
    let config =
        crate::extension::apply_spark_float_as_decimal(datafusion::prelude::SessionConfig::new());
    let config = repark_functions::ansi::with_spark_ansi_config(config, true);
    let ctx = SessionContext::new_with_config(config);
    repark_functions::register_all(&ctx);
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    (ctx, CatalogRegistry::new())
}
