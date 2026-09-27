//! Spark's analysis-time `CAST` / `TRY_CAST` deny list for DATE↔integer pairs.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
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
    (is_date(src) && is_spark_int(dst))
        || (is_spark_int(src) && is_date(dst))
        || (is_ntz_micros(src) && is_spark_numeric(dst))
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

fn is_ntz_micros(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
}

fn is_spark_numeric(data_type: &DataType) -> bool {
    is_spark_int(data_type)
        || matches!(
            data_type,
            DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
        )
}

/// The Spark SQL type name for the deny-list types — the spelling Spark's message quotes.
fn spark_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Date32 | DataType::Date64 => "DATE".to_owned(),
        DataType::Int8 => "TINYINT".to_owned(),
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::Float32 => "FLOAT".to_owned(),
        DataType::Float64 => "DOUBLE".to_owned(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("DECIMAL({precision},{scale})"),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP_NTZ".to_owned(),
        _ => "INT".to_owned(),
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
    if is_ntz_micros(src) && is_spark_numeric(dst) {
        let to = spark_type_name(dst);
        return Err(DataFusionError::Plan(format!(
            "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] Cannot resolve \"{}({inner} AS {to})\" \
             due to data type mismatch: cannot cast \"TIMESTAMP_NTZ\" to \"{to}\". SQLSTATE: 42K09",
            keyword.spelling(),
        )));
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
    fn the_deny_matrix_is_date_int_and_ntz_numeric() {
        use DataType::{Date32, Date64, Int8, Int16, Int32, Int64};
        for date in [&Date32, &Date64] {
            for int in [&Int8, &Int16, &Int32, &Int64] {
                assert!(spark_refuses_cast(date, int), "{date:?} -> {int:?}");
                assert!(spark_refuses_cast(int, date), "{int:?} -> {date:?}");
            }
        }
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        for numeric in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(10, 0),
            DataType::Decimal256(10, 0),
        ] {
            assert!(
                spark_refuses_cast(&micros, &numeric),
                "{micros:?} -> {numeric:?}"
            );
        }
    }

    /// Everything the design's §5.2 blast-radius table says must NOT move.
    #[test]
    fn adjacent_temporal_and_numeric_pairs_are_untouched() {
        use DataType::{Boolean, Date32, Float64, Int32, Int64, Utf8, Utf8View};
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let ltz = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert!(!spark_refuses_cast(&nanos, &Int64));
        assert!(!spark_refuses_cast(&ltz, &Int64));
        assert!(!spark_refuses_cast(&micros, &Date32));
        assert!(!spark_refuses_cast(&micros, &Utf8));
        assert!(!spark_refuses_cast(&micros, &Boolean));
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
    fn the_ntz_numeric_refusal_carries_the_without_suggestion_class() {
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let message =
            refuse_spark_illegal_cast(CastKeyword::Cast, &col("c"), &micros, &DataType::Int64)
                .expect_err("ntz -> bigint must refuse")
                .to_string();
        assert!(
            message.contains("[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]"),
            "{message}"
        );
        assert!(
            message.contains("Cannot resolve \"CAST(c AS BIGINT)\""),
            "{message}"
        );
        assert!(
            message.contains("cannot cast \"TIMESTAMP_NTZ\" to \"BIGINT\""),
            "{message}"
        );
        assert!(message.contains("SQLSTATE: 42K09"), "{message}");
        let tried =
            refuse_spark_illegal_cast(CastKeyword::TryCast, &col("c"), &micros, &DataType::Float64)
                .expect_err("try_cast ntz -> double must refuse")
                .to_string();
        assert!(
            tried.contains("Cannot resolve \"TRY_CAST(c AS DOUBLE)\""),
            "{tried}"
        );
        assert!(
            tried.contains("cannot cast \"TIMESTAMP_NTZ\" to \"DOUBLE\""),
            "{tried}"
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
