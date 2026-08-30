//! Spark's analysis-time `CAST` / `TRY_CAST` deny list for DATE↔integer pairs.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Expr;

/// Which keyword the user spelled; Spark echoes it inside `Cannot resolve "…"`.
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

/// Spark's narrow cast-legality deny list: every pair not named here keeps existing behavior.
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
        _ => "INT",
    }
}

/// Return Spark's named conversion function; run this gate before simplify lowers `unix_date`.
fn conversion_function(src: &DataType) -> &'static str {
    if is_date(src) {
        "UNIX_DATE"
    } else {
        "DATE_FROM_UNIX_DATE"
    }
}

/// Refuse a denied cast with Spark's type names, class, remedy, and child expression.
/// # Errors
/// Plan error with Spark's `[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION]` class when denied.
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

    /// Cover the G6-3 pairs and the G6-5 reverse in both Date widths.
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

    /// Everything the design's §5.2 blast-radius table says must NOT move.
    #[test]
    fn adjacent_temporal_and_numeric_pairs_are_untouched() {
        use DataType::{Boolean, Date32, Float64, Int32, Int64, Utf8, Utf8View};
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert!(!spark_refuses_cast(&micros, &Int64));
        assert!(!spark_refuses_cast(&micros, &Int32));
        assert!(!spark_refuses_cast(&micros, &Date32));
        assert!(!spark_refuses_cast(&micros, &Utf8));
        assert!(!spark_refuses_cast(&Date32, &Float64));
        assert!(!spark_refuses_cast(&Date32, &Boolean));
        assert!(!spark_refuses_cast(&Date32, &Utf8View));
        assert!(!spark_refuses_cast(&Date32, &micros));
        assert!(!spark_refuses_cast(&Date32, &DataType::Decimal128(10, 0)));
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
