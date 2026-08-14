//! G-7b — bit-exact `Decimal128` fixture pins for the Spark SQL door (Rust half of the
//! decimal corpus).
//!
//! Each pin asserts on the **Arrow path** (`collect`): exact `DataType::Decimal128(p, s)` (or
//! the disclosed non-decimal type), nullability, and the raw `i128` scaled integer. Goldens
//! are derived from the merged
//! Python corpus at `python/repark/tests/test_decimal128_parity.py` — every test names the
//! corpus row it mirrors. This module does **not** re-derive or edit the Python corpus.
//!
//! Coverage classes (charter G-7b / archived `docs/history/hardening-h1/g7-decimal-ledger.md` §9):
//! literal inference · division `(p,s)` · 38-clamp family (U4a matches Spark) · avg/promotion ·
//! INT×DECIMAL width (U3 fromLiteral) · ANSI overflow + div-zero · nullability marking.
//! Equality-class money arithmetic is also pinned so the bit-exact idiom has green controls,
//! not only disclosures.
//!
//! Out of scope: fixing any divergence a pin documents; the registry file; Python corpus edits.

use super::super::*;
use super::common::*;

use datafusion::arrow::array::Decimal128Array;

// =================================================================================================
// Collect helpers — one-column scalar pins on the Arrow path (value AND type AND nullability)
// =================================================================================================

/// One-column Decimal128 result: `(precision, scale, nullable, value_or_null)`.
///
/// `value` is the raw Arrow `i128` (scaled integer). `None` means the cell is null.
async fn collect_decimal128(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> (u8, i8, bool, Option<i128>) {
    let frame = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute failed for `{sql}`: {error}"));
    let schema = frame.schema();
    let field = schema.field(0);
    let nullable = field.is_nullable();
    let (precision, scale) = match field.data_type() {
        DataType::Decimal128(precision, scale) => (*precision, *scale),
        other => panic!("expected Decimal128 for `{sql}`, got {other:?}"),
    };
    let batches = frame
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect failed for `{sql}`: {error}"));
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "`{sql}` must yield exactly one row"
    );
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap_or_else(|| panic!("column 0 of `{sql}` is not Decimal128Array"));
    let value = if array.is_null(0) {
        None
    } else {
        Some(array.value(0))
    };
    (precision, scale, nullable, value)
}

// =================================================================================================
// Equality-class money controls (repark == Spark; corpus `repark is None`)
// =================================================================================================

/// Corpus row `add_same_precision_scale` — `(10,2)+(10,2)` → decimal128(11,2) = 5.79.
/// Scaled i128: 579. Equality control for the bit-exact idiom.
#[tokio::test]
async fn pin_add_same_precision_scale_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(1.23 AS DECIMAL(10,2)) + CAST(4.56 AS DECIMAL(10,2)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (11, 2), "result (p,s)");
    assert!(!nullable, "scalar add is non-null today");
    assert_eq!(value, Some(579), "i128 scaled 5.79 at scale 2");
}

/// Corpus row `mul_money_by_quantity` — money × qty → decimal128(21,2) = 59.97.
/// Scaled i128: 5997. Shopping-cart control.
#[tokio::test]
async fn pin_mul_money_by_quantity_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(19.99 AS DECIMAL(10,2)) * CAST(3 AS DECIMAL(10,0)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (21, 2), "result (p,s)");
    assert!(!nullable, "scalar mul is non-null today");
    assert_eq!(value, Some(5997), "i128 scaled 59.97 at scale 2");
}

// =================================================================================================
// Literal inference (DEC-1 / U2 — Spark-door `parse_float_as_decimal=true`)
// =================================================================================================

/// Corpus row `literal_1_23_infers_decimal_in_spark_double_in_repark`.
/// After U2 both doors' Spark half is decimal128(3,2) i128=123. Name kept on the Python
/// corpus row; this Rust pin follows the new type.
#[tokio::test]
async fn pin_literal_1_23_infers_decimal128_3_2_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) =
        collect_decimal128(&ctx, &catalogs, "SELECT 1.23 AS v").await;
    assert_eq!(
        (precision, scale),
        (3, 2),
        "bare 1.23 infers decimal128(3,2)"
    );
    assert!(!nullable, "bare literal is non-null");
    assert_eq!(value, Some(123), "i128 scaled 1.23 at scale 2");
}

// =================================================================================================
// Division result (p,s) (disclosure — repark lands narrower than Spark)
// =================================================================================================

/// Corpus row `div_same_precision_scale`.
/// U4b: Spark (23,13) 0.2697368421053. i128 = 2697368421053.
#[tokio::test]
async fn pin_div_same_precision_scale_repark_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(1.23 AS DECIMAL(10,2)) / CAST(4.56 AS DECIMAL(10,2)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (23, 13), "Spark division result (p,s)");
    assert!(nullable, "Spark decimal `/` is always nullable");
    assert_eq!(
        value,
        Some(2_697_368_421_053),
        "i128 scaled 0.2697368421053 at scale 13"
    );
}

// =================================================================================================
// 38-digit clamp family (disclosure — repark keeps more scale than Spark)
// =================================================================================================

/// Corpus row `mul_38_10_clamps_scale_in_spark`.
/// U4a: Spark `adjustPrecisionScale` → (38,6). i128 = 10^6 (1.0 at s=6).
#[tokio::test]
async fn pin_mul_38_10_clamps_to_38_6_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(1 AS DECIMAL(38,10)) * CAST(1 AS DECIMAL(38,10)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (38, 6), "Spark mul clamp (38,6)");
    assert!(!nullable, "scalar mul non-null today");
    assert_eq!(value, Some(1_000_000), "i128 for 1.0 at scale 6");
}

/// Corpus row `add_38_18_clamps_scale_in_spark`.
/// U4a: Spark add clamp → (38,17). i128 = 2 * 10^17.
#[tokio::test]
async fn pin_add_38_18_clamps_to_38_17_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(1 AS DECIMAL(38,18)) + CAST(1 AS DECIMAL(38,18)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (38, 17), "Spark add clamp (38,17)");
    assert!(!nullable, "scalar add non-null today");
    assert_eq!(
        value,
        Some(200_000_000_000_000_000),
        "i128 for 2.0 at scale 17"
    );
}

/// Corpus row `mul_38_20_plans_in_spark_refuses_in_repark` after DEC-8: Spark clamp (38,6).
#[tokio::test]
async fn pin_mul_38_20_still_refuses_at_plan() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, _nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(1 AS DECIMAL(38,20)) * CAST(1 AS DECIMAL(38,20)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (38, 6), "DEC-8 Spark clamp (38,6)");
    assert_eq!(value, Some(1_000_000), "i128 for 1.0 at scale 6");
}

// =================================================================================================
// avg type + int×decimal promotion (disclosures)
// =================================================================================================

/// Corpus row `avg_money_stays_decimal_in_spark_double_in_repark`.
///
/// Python facade half discloses float64; the **Rust Spark door** already lands Spark's
/// decimal128(14,6) exact 1.650000 (i128 = `1_650_000`). Pin the Rust-door fact bit-exact —
/// the facade gap is a separate entry-point cell, not re-litigated here.
#[tokio::test]
async fn pin_avg_money_stays_decimal128_14_6_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let sql = "SELECT avg(x) AS v FROM (SELECT CAST(1.10 AS DECIMAL(10,2)) AS x \
               UNION ALL SELECT CAST(2.20 AS DECIMAL(10,2))) t";
    let (precision, scale, nullable, value) = collect_decimal128(&ctx, &catalogs, sql).await;
    assert_eq!(
        (precision, scale),
        (14, 6),
        "Rust Spark door avg-of-decimal stays decimal128(14,6) (Spark half of corpus row)"
    );
    assert!(nullable, "avg is nullable");
    assert_eq!(value, Some(1_650_000), "i128 scaled 1.650000 at scale 6");
}

/// Corpus row `int_times_decimal_promotes_wider_in_repark`.
/// U3: integer literal `5` is `DECIMAL(1,0)` → product (12,2). Value 7.50 → i128 = 750.
/// Nullability still non-null (DEC-9 / U5); Spark marks nullable.
#[tokio::test]
async fn pin_int_times_decimal_is_12_2_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT 5 * CAST(1.50 AS DECIMAL(10,2)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (12, 2), "U3 fromLiteral width");
    assert!(!nullable, "repark marks int×decimal non-null (DEC-9)");
    assert_eq!(value, Some(750), "i128 scaled 7.50 at scale 2");
}

/// Typed INT is `forType(INT)=(10,0)`, not min-precision. Must not shrink to (12,2).
#[tokio::test]
async fn pin_cast_int_times_decimal_stays_21_2_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(5 AS INT) * CAST(1.50 AS DECIMAL(10,2)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (21, 2), "typed INT column width");
    assert!(!nullable, "scalar mul non-null today");
    assert_eq!(value, Some(750), "i128 scaled 7.50 at scale 2");
}

// =================================================================================================
// ANSI overflow + divide-by-zero (G13 raise-class disclosures on the repark half)
// =================================================================================================

/// Corpus row `overflow_max_decimal38_plus_one_raises_in_spark` after DEC-6: ANSI ON raises.
#[tokio::test]
async fn pin_overflow_max_decimal38_plus_one_wrong_value_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let sql = "SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) \
               + CAST(1 AS DECIMAL(38,0)) AS v";
    let error = match execute(&ctx, &catalogs, sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("ANSI overflow must raise")
            .to_string(),
    };
    assert!(
        error.contains("NUMERIC_VALUE_OUT_OF_RANGE"),
        "expected NUMERIC_VALUE_OUT_OF_RANGE, got {error}"
    );
}

/// ANSI OFF: photographed wrap becomes NULL at (38,0). Nullability may stay non-null
/// (DEC-9 residue) — pin the value only.
#[tokio::test]
async fn pin_overflow_max_decimal38_plus_one_null_when_ansi_false() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_with_ansi(&warehouse, false).await;
    let sql = "SELECT CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0)) \
               + CAST(1 AS DECIMAL(38,0)) AS v";
    let (precision, scale, _nullable, value) = collect_decimal128(&ctx, &catalogs, sql).await;
    assert_eq!(
        (precision, scale),
        (38, 0),
        "overflow result type stays (38,0)"
    );
    assert_eq!(
        value, None,
        "ansi=false overflow yields NULL, not the 10^38 wrap"
    );
}

/// Corpus row `div_by_zero_decimal38_raises_in_spark_null_in_repark` after U5: default ANSI
/// ON raises `DIVIDE_BY_ZERO` (shared-raise with Spark). Name kept on the Python row.
#[tokio::test]
async fn pin_div_by_zero_decimal38_raises_under_default_ansi() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let sql = "SELECT CAST(1 AS DECIMAL(38,0)) / CAST(0 AS DECIMAL(38,0)) AS v";
    let error = match execute(&ctx, &catalogs, sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("ANSI /0 must raise")
            .to_string(),
    };
    assert!(
        error.contains("DIVIDE_BY_ZERO"),
        "expected DIVIDE_BY_ZERO, got {error}"
    );
}

/// ANSI OFF restores the legacy NULL at repark's Arrow division type (38,4).
#[tokio::test]
async fn pin_div_by_zero_decimal38_returns_null_at_38_4_when_ansi_false() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_with_ansi(&warehouse, false).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(1 AS DECIMAL(38,0)) / CAST(0 AS DECIMAL(38,0)) AS v",
    )
    .await;
    assert_eq!(
        (precision, scale),
        (38, 6),
        "repark div-by-zero result type (Spark `/` formula)"
    );
    assert!(nullable, "NULL cell requires a nullable field");
    assert_eq!(value, None, "ansi=false /0 yields NULL, not a raise");
}

// =================================================================================================
// Nullability marking (value+type agree with Spark; nullability diverges)
// =================================================================================================

/// Corpus row `mul_single_digit_nullability_differs`.
/// Value 81 and type (3,0) agree with Spark; repark marks non-null, Spark nullable.
#[tokio::test]
async fn pin_mul_single_digit_nullability_non_null_i128() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    let (precision, scale, nullable, value) = collect_decimal128(
        &ctx,
        &catalogs,
        "SELECT CAST(9 AS DECIMAL(1,0)) * CAST(9 AS DECIMAL(1,0)) AS v",
    )
    .await;
    assert_eq!((precision, scale), (3, 0), "result (p,s)");
    assert!(
        !nullable,
        "repark marks the product non-null (Spark: nullable)"
    );
    assert_eq!(value, Some(81), "i128 value 81 at scale 0");
}
