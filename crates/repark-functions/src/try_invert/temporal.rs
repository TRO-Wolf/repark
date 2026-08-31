//! DATE/TIMESTAMP ± INTERVAL and INTERVAL / numeric for try_* kernels.

use std::sync::Arc;

use chrono::{DateTime, Days, Months, NaiveDate, TimeDelta};
use datafusion::arrow::array::{Array, ArrayRef, Date32Array, Float64Array, PrimitiveArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
    IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
};
use datafusion::arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};

use super::arith::{TryKind, kind_name, primitive};

fn invoke_interval_day_time(kind: TryKind, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left = primitive::<IntervalDayTimeType>(arrays[0].as_ref())?;
    let right = primitive::<IntervalDayTimeType>(arrays[1].as_ref())?;
    if left.len() != right.len() {
        return exec_err!("'{}' argument lengths differ", kind_name(kind));
    }
    let mut values: Vec<Option<IntervalDayTime>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if !left.is_valid(row) || !right.is_valid(row) {
            values.push(None);
            continue;
        }
        values.push(eval_interval_day_time(
            kind,
            left.value(row),
            right.value(row),
        ));
    }
    Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
        IntervalDayTimeType,
    >::from(values))))
}

const MICROS_PER_DAY: i64 = 86_400_000_000;
const MICROS_PER_MILLI: i64 = 1_000;

fn interval_to_micros(value: IntervalDayTime) -> Option<i64> {
    let days = i64::from(value.days);
    let millis = i64::from(value.milliseconds);
    days.checked_mul(MICROS_PER_DAY)?
        .checked_add(millis.checked_mul(MICROS_PER_MILLI)?)
}

fn micros_to_interval(micros: i64) -> Option<IntervalDayTime> {
    let days = i32::try_from(micros.div_euclid(MICROS_PER_DAY)).ok()?;
    let rem = micros.rem_euclid(MICROS_PER_DAY);
    let milliseconds = i32::try_from(rem / MICROS_PER_MILLI).ok()?;
    Some(IntervalDayTime { days, milliseconds })
}

fn invoke_interval_month_day_nano(
    kind: TryKind,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left = primitive::<IntervalMonthDayNanoType>(arrays[0].as_ref())?;
    let right = primitive::<IntervalMonthDayNanoType>(arrays[1].as_ref())?;
    if left.len() != right.len() {
        return exec_err!("'{}' argument lengths differ", kind_name(kind));
    }
    let mut values: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if !left.is_valid(row) || !right.is_valid(row) {
            values.push(None);
            continue;
        }
        values.push(eval_interval_month_day_nano(
            kind,
            left.value(row),
            right.value(row),
        ));
    }
    Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
        IntervalMonthDayNanoType,
    >::from(values))))
}

fn eval_interval_month_day_nano(
    kind: TryKind,
    left: IntervalMonthDayNano,
    right: IntervalMonthDayNano,
) -> Option<IntervalMonthDayNano> {
    let (months, days, nanos) = match kind {
        TryKind::Add => (
            left.months.checked_add(right.months)?,
            left.days.checked_add(right.days)?,
            left.nanoseconds.checked_add(right.nanoseconds)?,
        ),
        TryKind::Sub => (
            left.months.checked_sub(right.months)?,
            left.days.checked_sub(right.days)?,
            left.nanoseconds.checked_sub(right.nanoseconds)?,
        ),
        _ => return None,
    };
    duration_micros(days, nanos)?;
    Some(IntervalMonthDayNano {
        months,
        days,
        nanoseconds: nanos,
    })
}

fn duration_micros(days: i32, nanos: i64) -> Option<i64> {
    i64::from(days)
        .checked_mul(MICROS_PER_DAY)?
        .checked_add(nanos.div_euclid(1_000))
}

fn eval_interval_day_time(
    kind: TryKind,
    left: IntervalDayTime,
    right: IntervalDayTime,
) -> Option<IntervalDayTime> {
    let left_micros = interval_to_micros(left)?;
    let right_micros = interval_to_micros(right)?;
    let result = match kind {
        TryKind::Add => left_micros.checked_add(right_micros)?,
        TryKind::Sub => left_micros.checked_sub(right_micros)?,
        _ => return None,
    };
    micros_to_interval(result)
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

pub(crate) fn invoke_interval_result(
    kind: TryKind,
    unit: IntervalUnit,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[1].data_type().is_numeric() {
        return invoke_interval_divide(unit, &arrays);
    }
    match unit {
        IntervalUnit::DayTime => invoke_interval_day_time(kind, args),
        IntervalUnit::MonthDayNano => invoke_interval_month_day_nano(kind, args),
        IntervalUnit::YearMonth => exec_err!(
            "'{}' promised Interval(YearMonth) but cannot invoke interval-interval add",
            kind_name(kind)
        ),
    }
}

fn invoke_interval_divide(unit: IntervalUnit, arrays: &[ArrayRef]) -> Result<ColumnarValue> {
    let left = &arrays[0];
    let right = &arrays[1];
    if left.len() != right.len() {
        return exec_err!("'try_divide' argument lengths differ");
    }
    match unit {
        IntervalUnit::MonthDayNano => {
            let values = primitive::<IntervalMonthDayNanoType>(left.as_ref())?;
            let mut out: Vec<Option<IntervalMonthDayNano>> = Vec::with_capacity(values.len());
            for row in 0..values.len() {
                if !values.is_valid(row) {
                    out.push(None);
                    continue;
                }
                match numeric_f64(right.as_ref(), row)? {
                    None => out.push(None),
                    Some(divisor) => out.push(divide_month_day_nano(values.value(row), divisor)),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
                IntervalMonthDayNanoType,
            >::from(out))))
        }
        IntervalUnit::DayTime => {
            let values = primitive::<IntervalDayTimeType>(left.as_ref())?;
            let mut out: Vec<Option<IntervalDayTime>> = Vec::with_capacity(values.len());
            for row in 0..values.len() {
                if !values.is_valid(row) {
                    out.push(None);
                    continue;
                }
                match numeric_f64(right.as_ref(), row)? {
                    None => out.push(None),
                    Some(divisor) => out.push(divide_day_time(values.value(row), divisor)),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
                IntervalDayTimeType,
            >::from(out))))
        }
        IntervalUnit::YearMonth => {
            let values = primitive::<IntervalYearMonthType>(left.as_ref())?;
            let mut out: Vec<Option<i32>> = Vec::with_capacity(values.len());
            for row in 0..values.len() {
                if !values.is_valid(row) {
                    out.push(None);
                    continue;
                }
                match numeric_f64(right.as_ref(), row)? {
                    None => out.push(None),
                    Some(divisor) if divisor == 0.0 || !divisor.is_finite() => out.push(None),
                    Some(divisor) => {
                        #[allow(clippy::cast_possible_truncation)]
                        out.push(Some((f64::from(values.value(row)) / divisor).trunc() as i32));
                    }
                }
            }
            Ok(ColumnarValue::Array(Arc::new(PrimitiveArray::<
                IntervalYearMonthType,
            >::from(out))))
        }
    }
}

fn numeric_f64(array: &dyn Array, row: usize) -> Result<Option<f64>> {
    if !array.is_valid(row) {
        return Ok(None);
    }
    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(floats.value(row)));
    }
    if let Some(ints) = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>() {
        #[allow(clippy::cast_precision_loss)]
        return Ok(Some(ints.value(row) as f64));
    }
    exec_err!(
        "try_divide interval divisor expected Float64 or Int64, got {}",
        array.data_type()
    )
}

fn divide_month_day_nano(
    value: IntervalMonthDayNano,
    divisor: f64,
) -> Option<IntervalMonthDayNano> {
    if divisor == 0.0 || !divisor.is_finite() {
        return None;
    }
    let day_nanos = i64::from(value.days).checked_mul(NANOS_PER_DAY)?;
    let total = day_nanos.checked_add(value.nanoseconds)?;
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    let result_nanos = (total as f64 / divisor).round() as i64;
    #[allow(clippy::cast_possible_truncation)]
    let months = (f64::from(value.months) / divisor).trunc() as i32;
    let days = i32::try_from(result_nanos.div_euclid(NANOS_PER_DAY)).ok()?;
    Some(IntervalMonthDayNano {
        months,
        days,
        nanoseconds: result_nanos.rem_euclid(NANOS_PER_DAY),
    })
}

fn divide_day_time(value: IntervalDayTime, divisor: f64) -> Option<IntervalDayTime> {
    if divisor == 0.0 || !divisor.is_finite() {
        return None;
    }
    let micros = interval_to_micros(value)?;
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    let result = (micros as f64 / divisor).round() as i64;
    micros_to_interval(result)
}

pub(crate) fn invoke_date_interval(
    kind: TryKind,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let (dates, intervals, sign) = temporal_sides(kind, &arrays)?;
    if dates.len() != intervals.len() {
        return exec_err!("'{}' argument lengths differ", kind_name(kind));
    }
    let date_values = primitive::<Date32Type>(dates)?;
    let mut out: Vec<Option<i32>> = Vec::with_capacity(date_values.len());
    for row in 0..date_values.len() {
        if !date_values.is_valid(row) || !intervals.is_valid(row) {
            out.push(None);
            continue;
        }
        let Some((months, days, nanos)) = interval_parts(intervals, row) else {
            out.push(None);
            continue;
        };
        out.push(shift_date32(
            date_values.value(row),
            months.checked_mul(sign),
            days.checked_mul(sign),
            nanos.checked_mul(i64::from(sign)),
        ));
    }
    Ok(ColumnarValue::Array(Arc::new(Date32Array::from(out))))
}

pub(crate) fn invoke_timestamp_interval(
    kind: TryKind,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let return_type = args.return_field.data_type().clone();
    let tz = match &return_type {
        DataType::Timestamp(_, tz) => tz.clone(),
        other => {
            return exec_err!(
                "'{}' promised {other} but cannot invoke timestamp interval",
                kind_name(kind)
            );
        }
    };
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let (timestamps, intervals, sign) = temporal_sides(kind, &arrays)?;
    if timestamps.len() != intervals.len() {
        return exec_err!("'{}' argument lengths differ", kind_name(kind));
    }
    let micros = cast(
        timestamps,
        &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
    )?;
    let ts_values = primitive::<TimestampMicrosecondType>(micros.as_ref())?;
    let mut out: Vec<Option<i64>> = Vec::with_capacity(ts_values.len());
    for row in 0..ts_values.len() {
        if !ts_values.is_valid(row) || !intervals.is_valid(row) {
            out.push(None);
            continue;
        }
        let Some((months, days, nanos)) = interval_parts(intervals, row) else {
            out.push(None);
            continue;
        };
        out.push(shift_timestamp_micros(
            ts_values.value(row),
            months.checked_mul(sign),
            days.checked_mul(sign),
            nanos.checked_mul(i64::from(sign)),
        ));
    }
    let micro_array = PrimitiveArray::<TimestampMicrosecondType>::from(out).with_timezone_opt(tz);
    let array = cast(&micro_array, &return_type)?;
    Ok(ColumnarValue::Array(array))
}

fn temporal_sides(
    kind: TryKind,
    arrays: &[datafusion::arrow::array::ArrayRef],
) -> Result<(&dyn Array, &dyn Array, i32)> {
    let left = arrays[0].as_ref();
    let right = arrays[1].as_ref();
    let sign = match kind {
        TryKind::Add => 1,
        TryKind::Sub => -1,
        _ => {
            return exec_err!(
                "'{}' does not add or subtract temporal values",
                kind_name(kind)
            );
        }
    };
    match (left.data_type(), right.data_type()) {
        (DataType::Date32 | DataType::Timestamp(_, _), DataType::Interval(_)) => {
            Ok((left, right, sign))
        }
        (DataType::Interval(_), DataType::Date32 | DataType::Timestamp(_, _))
            if matches!(kind, TryKind::Add) =>
        {
            Ok((right, left, sign))
        }
        (left_type, right_type) => exec_err!(
            "'{}' does not support types {left_type} and {right_type}",
            kind_name(kind)
        ),
    }
}

fn interval_parts(array: &dyn Array, row: usize) -> Option<(i32, i32, i64)> {
    match array.data_type() {
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<IntervalMonthDayNanoType>>()?;
            let value = values.value(row);
            Some((value.months, value.days, value.nanoseconds))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<IntervalDayTimeType>>()?;
            let value = values.value(row);
            Some((0, value.days, i64::from(value.milliseconds) * 1_000_000))
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let values = array
                .as_any()
                .downcast_ref::<PrimitiveArray<IntervalYearMonthType>>()?;
            Some((values.value(row), 0, 0))
        }
        _ => None,
    }
}

fn add_calendar_months(date: NaiveDate, months: i32) -> Option<NaiveDate> {
    if months >= 0 {
        date.checked_add_months(Months::new(u32::try_from(months).ok()?))
    } else {
        date.checked_sub_months(Months::new(u32::try_from(months.checked_neg()?).ok()?))
    }
}

fn shift_days(date: NaiveDate, days: i64) -> Option<NaiveDate> {
    if days >= 0 {
        date.checked_add_days(Days::new(u64::try_from(days).ok()?))
    } else {
        date.checked_sub_days(Days::new(u64::try_from(days.checked_neg()?).ok()?))
    }
}

fn shift_date32(
    days: i32,
    months: Option<i32>,
    extra_days: Option<i32>,
    nanos: Option<i64>,
) -> Option<i32> {
    let months = months?;
    let extra_days = extra_days?;
    let nanos = nanos?;
    let date = Date32Type::to_naive_date_opt(days)?;
    let date = add_calendar_months(date, months)?;
    let date = shift_days(date, i64::from(extra_days))?;
    let date = shift_days(date, nanos.div_euclid(NANOS_PER_DAY))?;
    Some(Date32Type::from_naive_date(date))
}

fn shift_timestamp_micros(
    micros: i64,
    months: Option<i32>,
    extra_days: Option<i32>,
    nanos: Option<i64>,
) -> Option<i64> {
    let months = months?;
    let extra_days = extra_days?;
    let nanos = nanos?;
    let datetime = DateTime::from_timestamp_micros(micros)?.naive_utc();
    let date = add_calendar_months(datetime.date(), months)?;
    let datetime = date.and_time(datetime.time());
    let datetime = datetime.checked_add_signed(TimeDelta::days(i64::from(extra_days)))?;
    let datetime = datetime.checked_add_signed(TimeDelta::nanoseconds(nanos))?;
    Some(datetime.and_utc().timestamp_micros())
}
