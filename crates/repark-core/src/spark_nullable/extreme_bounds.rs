use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};

#[must_use]
pub(super) fn without_type_extreme_bounds(mut statistics: Statistics) -> Statistics {
    drop_type_extreme_bounds(&mut statistics);
    statistics
}

pub(super) fn drop_type_extreme_bounds(statistics: &mut Statistics) {
    statistics
        .column_statistics
        .iter_mut()
        .for_each(drop_column_extreme_bounds);
}

#[must_use]
pub(super) fn has_type_extreme_bound(statistics: &Statistics) -> bool {
    statistics.column_statistics.iter().any(|column| {
        column.min_value.get_value().is_some_and(is_type_minimum)
            || column.max_value.get_value().is_some_and(is_type_maximum)
    })
}

fn drop_column_extreme_bounds(column: &mut ColumnStatistics) {
    if column.min_value.get_value().is_some_and(is_type_minimum) {
        column.min_value = Precision::Absent;
    }
    if column.max_value.get_value().is_some_and(is_type_maximum) {
        column.max_value = Precision::Absent;
    }
}

fn is_type_minimum(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int64(Some(i64::MIN))
            | ScalarValue::UInt64(Some(u64::MIN))
            | ScalarValue::Date64(Some(i64::MIN))
            | ScalarValue::TimestampSecond(Some(i64::MIN), _)
            | ScalarValue::TimestampMillisecond(Some(i64::MIN), _)
            | ScalarValue::TimestampMicrosecond(Some(i64::MIN), _)
            | ScalarValue::TimestampNanosecond(Some(i64::MIN), _)
    )
}

fn is_type_maximum(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int64(Some(i64::MAX))
            | ScalarValue::UInt64(Some(u64::MAX))
            | ScalarValue::Date64(Some(i64::MAX))
            | ScalarValue::TimestampSecond(Some(i64::MAX), _)
            | ScalarValue::TimestampMillisecond(Some(i64::MAX), _)
            | ScalarValue::TimestampMicrosecond(Some(i64::MAX), _)
            | ScalarValue::TimestampNanosecond(Some(i64::MAX), _)
    )
}
