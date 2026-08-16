//! Spark CAST / TRY_CAST **type-legality** gate — divergence registry rows G6-3 and G6-5.
//!
//! Spark refuses `CAST(DATE AS INT)` and `CAST(INT AS DATE)` at **analysis**
//! (`Cast.checkInputDataTypes` → `CheckAnalysis`), with the class
//! `DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION` and a named remedy (`UNIX_DATE` /
//! `DATE_FROM_UNIX_DATE`). arrow-rs 58.4's `can_cast_types` permits `Date32 → Int32|Int64` as a
//! plain reinterpretation of the Date32 backing value — days since 1970-01-01 — so repark
//! silently answered `18262` for `CAST(DATE '2020-01-01' AS INT)`, and `2020-01-01` for the
//! reverse. Both are silently-wrong results, not loud failures; they are what this module closes.
//!
//! **Why the deny list is exactly `{Date32, Date64} ↔ {Int8, Int16, Int32, Int64}`** (design
//! `planning/hardening/G63-DATE-INT-DESIGN.md` §3.3, measured on `0.2.0` against live PySpark
//! 4.1.2 ANSI):
//!
//! * `Date → Int32|Int64` are the two pairs arrow-rs answers with a wrong VALUE. They are the fix.
//! * `Date → Int8|Int16` already refuse, but with a DataFusion `NotImplemented` needle wrapped in
//!   `Optimizer rule 'simplify_expressions' failed`. Including them converts a wrong exception
//!   class into Spark's on two recipes nothing pins — strictly closer to Spark, no behaviour
//!   change beyond the message.
//! * `Int* → Date32|Date64` is the same class in the reverse direction (G6-5), with the identical
//!   Spark remedy shape.
//! * `Float*` / `Decimal*` / `Boolean` targets are **excluded on purpose**. They already refuse
//!   loudly today, and `Column.__truediv__` wraps BOTH operands in `Cast(… AS Float64)`, so
//!   `F.col("d") / 2` would otherwise be told it "wrote a CAST" it never wrote.
//!
//! **Not the store-assignment matrix.** `repark_iceberg::write::store_assign::ansi_store_assignable`
//! answers a different question and is deliberately laxer in places this one is strict and
//! stricter in places this one is lax (it permits `Date32 → Timestamp` and refuses
//! `Timestamp → Int64`; the cast matrix does the reverse). Wiring this gate to that predicate
//! would break TZ-5's `CAST(TIMESTAMP AS BIGINT)` in the same stroke. Two matrices, two homes,
//! one shared error idiom.
//!
//! **Not ANSI-gated.** Spark's check lives in `Cast.checkInputDataTypes`, which is a check on the
//! **type pair**, not on the eval mode: `spark.sql.ansi.enabled` (and `TryCast`, which is
//! `Cast(evalMode = TRY)`) change what happens to a VALUE that a legal cast cannot represent,
//! never which type pairs are castable. So the gate takes no `ansi_enabled` parameter and fires
//! in both modes — the same reason `TRY_CAST` gets the identical refusal (design §3.5).

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;

/// Which keyword the user spelled — Spark echoes it back inside `Cannot resolve "…"`, and the
/// recorded oracle shows `TRY_CAST(d AS INT)` for the try door with the class unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CastKeyword {
    Cast,
    TryCast,
}

impl CastKeyword {
    const fn spelling(self) -> &'static str {
        match self {
            Self::Cast => "CAST",
            Self::TryCast => "TRY_CAST",
        }
    }
}

/// Spark's cast-legality deny list, narrowest form. A DENY list, not an allow list: every pair
/// not named here keeps today's behaviour byte-for-byte.
pub(super) fn spark_refuses_cast(src: &DataType, dst: &DataType) -> bool {
    (is_date(src) && is_spark_int(dst)) || (is_spark_int(src) && is_date(dst))
}

fn is_date(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}

fn is_spark_int(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

/// The Spark SQL type name for the deny-list types — the spelling Spark's message quotes.
fn spark_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int64 => "BIGINT",
        // Only deny-list types reach this function, and `Int32` is the remaining one.
        _ => "INT",
    }
}

/// The function Spark's own message tells the caller to use instead. Both are real, correct and
/// measured-equal in repark: `unix_date(DATE '2020-01-01')` and
/// `date_from_unix_date(18262)` agree with Spark on both engines.
///
/// `unix_date` is also the reason this gate MUST live in the analyzer: `datafusion-spark` lowers
/// it to `Expr::Cast(arg, Int32)` in `ScalarUDFImpl::simplify`, which runs in the OPTIMIZER. A
/// gate one stage later would refuse the very remedy it names (design §3.4).
fn conversion_function(src: &DataType) -> &'static str {
    if is_date(src) {
        "UNIX_DATE"
    } else {
        "DATE_FROM_UNIX_DATE"
    }
}

/// ===========================================================================================
/// Refuse a cast Spark refuses, in Spark's own words.
///
/// `inner` is the expression being cast — it is echoed into the `Cannot resolve "…"` clause the
/// way Spark echoes the child expression.
/// ===========================================================================================
///
/// # Errors
/// [`DataFusionError::Plan`] carrying `[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]` when
/// `(src, dst)` is on the deny list. A `Plan` error carrying a bracketed Spark class folds to
/// `repark_common::Error::Analysis` → `AnalysisException` at the PyO3 boundary, which is the
/// class Spark raises (precedent: `repark_spark::window_range`'s `RANGE_FRAME_INVALID_TYPE`).
pub(super) fn refuse_spark_illegal_cast(
    keyword: CastKeyword,
    inner: &Expr,
    src: &DataType,
    dst: &DataType,
) -> Result<()> {
    if !spark_refuses_cast(src, dst) {
        return Ok(());
    }
    let (from, to) = (spark_type_name(src), spark_type_name(dst));
    Err(DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION] Cannot resolve \"{}({inner} AS {to})\" \
         due to data type mismatch: cannot cast \"{from}\" to \"{to}\". To convert values from \
         \"{from}\" to \"{to}\", you can use the functions `{}` instead. SQLSTATE: 42K09",
        keyword.spelling(),
        conversion_function(src),
    )))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::logical_expr::col;

    use super::{CastKeyword, refuse_spark_illegal_cast, spark_refuses_cast};

    /// The G6-3 pairs (the two silently-wrong ones plus the two already-loud narrow ones) and
    /// the G6-5 reverse, in both Date widths.
    #[test]
    fn the_deny_matrix_is_exactly_date_to_int_and_back() {
        use DataType::{Date32, Date64, Int8, Int16, Int32, Int64};
        for date in [&Date32, &Date64] {
            for int in [&Int8, &Int16, &Int32, &Int64] {
                assert!(spark_refuses_cast(date, int), "{date:?} -> {int:?}");
                assert!(spark_refuses_cast(int, date), "{int:?} -> {date:?}");
            }
        }
    }

    /// Everything the design's §5.2 blast-radius table says must NOT move. The `Timestamp` rows
    /// are the load-bearing ones: TZ-5 / B-TZ-4 / TZ-8 share the analyzer arm this gate heads.
    #[test]
    fn adjacent_temporal_and_numeric_pairs_are_untouched() {
        use DataType::{Boolean, Date32, Float64, Int32, Int64, Utf8, Utf8View};
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        // TZ-5: CAST(ts AS BIGINT) — the row that must keep answering 1577836800.
        assert!(!spark_refuses_cast(&micros, &Int64));
        assert!(!spark_refuses_cast(&micros, &Int32));
        // TZ-8 / B-TZ-4.
        assert!(!spark_refuses_cast(&micros, &Date32));
        assert!(!spark_refuses_cast(&micros, &Utf8));
        // DATE to non-integer targets: excluded on purpose (§3.3).
        assert!(!spark_refuses_cast(&Date32, &Float64));
        assert!(!spark_refuses_cast(&Date32, &Boolean));
        assert!(!spark_refuses_cast(&Date32, &Utf8View));
        assert!(!spark_refuses_cast(&Date32, &micros));
        assert!(!spark_refuses_cast(&Date32, &DataType::Decimal128(10, 0)));
        // The rest of the G6 corpus.
        assert!(!spark_refuses_cast(&Utf8, &Int32));
        assert!(!spark_refuses_cast(&Utf8, &Date32));
        assert!(!spark_refuses_cast(&Int32, &DataType::Int8));
        assert!(!spark_refuses_cast(&Int64, &Float64));
    }

    #[test]
    fn the_cast_refusal_carries_sparks_class_types_and_remedy() {
        let message = refuse_spark_illegal_cast(
            CastKeyword::Cast,
            &col("d"),
            &DataType::Date32,
            &DataType::Int32,
        )
        .expect_err("date -> int must refuse")
        .to_string();
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]"),
            "{message}"
        );
        assert!(
            message.contains("Cannot resolve \"CAST(d AS INT)\""),
            "{message}"
        );
        assert!(
            message.contains("cannot cast \"DATE\" to \"INT\""),
            "{message}"
        );
        assert!(message.contains("`UNIX_DATE`"), "{message}");
        assert!(message.contains("SQLSTATE: 42K09"), "{message}");
    }

    #[test]
    fn the_try_cast_refusal_spells_try_cast_and_keeps_the_class() {
        let message = refuse_spark_illegal_cast(
            CastKeyword::TryCast,
            &col("d"),
            &DataType::Date32,
            &DataType::Int64,
        )
        .expect_err("try_cast date -> bigint must refuse")
        .to_string();
        assert!(
            message.contains("Cannot resolve \"TRY_CAST(d AS BIGINT)\""),
            "{message}"
        );
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]"),
            "{message}"
        );
        assert!(
            message.contains("cannot cast \"DATE\" to \"BIGINT\""),
            "{message}"
        );
    }

    #[test]
    fn the_reverse_direction_names_date_from_unix_date() {
        let message = refuse_spark_illegal_cast(
            CastKeyword::Cast,
            &col("n"),
            &DataType::Int32,
            &DataType::Date32,
        )
        .expect_err("int -> date must refuse")
        .to_string();
        assert!(
            message.contains("cannot cast \"INT\" to \"DATE\""),
            "{message}"
        );
        assert!(message.contains("`DATE_FROM_UNIX_DATE`"), "{message}");
    }

    /// A pair off the deny list is a no-op, not a refusal with an empty message.
    #[test]
    fn a_legal_pair_returns_ok() {
        assert!(
            refuse_spark_illegal_cast(
                CastKeyword::Cast,
                &col("s"),
                &DataType::Utf8,
                &DataType::Int32,
            )
            .is_ok()
        );
    }
}
