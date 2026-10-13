use datafusion::arrow::datatypes::IntervalMonthDayNano;
use datafusion::common::{DataFusionError, Result};

use crate::cardinality::{
    DEFAULT_MAX_ARRAY_ELEMENTS, refuse_if_over_ceiling, sequence_cardinality,
};

fn boundary_error(start: &str, stop: &str, stride: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "requirement failed: Illegal sequence boundaries: {start} to {stop} by {stride}"
    ))
}

pub(crate) fn int_row(start: i64, stop: i64, stride: Option<i64>) -> Result<Vec<i64>> {
    let mut values = Vec::new();
    push_ints(
        &mut values,
        start,
        stop,
        resolve_int_stride(start, stop, stride)?,
    )?;
    Ok(values)
}

pub(crate) fn resolve_int_stride(start: i64, stop: i64, stride: Option<i64>) -> Result<i64> {
    match stride {
        None => Ok(if stop >= start { 1 } else { -1 }),
        Some(given) => {
            if given == 0 || (given > 0 && start > stop) || (given < 0 && start < stop) {
                return Err(int_step_error(start, stop, given));
            }
            Ok(given)
        }
    }
}

fn usize_count(cardinality: u64) -> Result<usize> {
    usize::try_from(cardinality)
        .map_err(|_| DataFusionError::Execution("sequence row count does not fit usize".to_owned()))
}

pub(crate) fn push_ints(values: &mut Vec<i64>, start: i64, stop: i64, stride: i64) -> Result<()> {
    if let Some(cardinality) = sequence_cardinality(start, stop, stride) {
        refuse_if_over_ceiling("sequence", cardinality, DEFAULT_MAX_ARRAY_ELEMENTS)?;
        let count = usize_count(cardinality)?;
        values.reserve(count);
        let mut current = start;
        for index in 0..count {
            values.push(current);
            if index + 1 < count {
                current = current.checked_add(stride).ok_or_else(|| {
                    DataFusionError::Execution("sequence int step out of range".to_owned())
                })?;
            }
        }
        return Ok(());
    }
    let mut current = start;
    loop {
        values.push(current);
        refuse_if_over_ceiling(
            "sequence",
            u64::try_from(values.len()).map_err(|_| {
                DataFusionError::Execution("sequence row count overflows".to_owned())
            })?,
            DEFAULT_MAX_ARRAY_ELEMENTS,
        )?;
        if current == stop {
            break;
        }
        match current.checked_add(stride) {
            Some(next) if (stride > 0 && next <= stop) || (stride < 0 && next >= stop) => {
                current = next;
            }
            _ => break,
        }
    }
    Ok(())
}

fn int_step_error(start: i64, stop: i64, stride: i64) -> DataFusionError {
    boundary_error(&start.to_string(), &stop.to_string(), &stride.to_string())
}

pub(crate) fn date_row(
    start: i32,
    stop: i32,
    stride: Option<IntervalMonthDayNano>,
) -> Result<Option<Vec<i32>>> {
    match stride {
        None => {
            let default = if stop >= start { 1 } else { -1 };
            Ok(Some(collect_dates(
                start,
                stop,
                &IntervalMonthDayNano::new(0, default, 0),
            )?))
        }
        Some(interval) => {
            if interval.months == 0 && interval.days == 0 && interval.nanoseconds == 0 {
                return Err(boundary_error(
                    &format_days(start)?,
                    &format_days(stop)?,
                    &format_interval(&interval),
                ));
            }
            if interval.nanoseconds != 0 {
                return Err(DataFusionError::Execution(format!(
                    "sequence date step carries sub-day nanos: {}",
                    format_interval(&interval)
                )));
            }
            if start == stop {
                return Ok(Some(vec![start]));
            }
            let sign = if interval.months != 0 {
                interval.months.signum()
            } else {
                interval.days.signum()
            };
            let want = if stop > start { 1 } else { -1 };
            if sign != want {
                return Err(boundary_error(
                    &format_days(start)?,
                    &format_days(stop)?,
                    &format_interval(&interval),
                ));
            }
            Ok(Some(collect_dates(start, stop, &interval)?))
        }
    }
}

fn day_count(start: i32, stop: i32, days: i32) -> Result<usize> {
    let cardinality = sequence_cardinality(i64::from(start), i64::from(stop), i64::from(days))
        .ok_or_else(|| DataFusionError::Execution("sequence day span out of range".to_owned()))?;
    refuse_if_over_ceiling("sequence", cardinality, DEFAULT_MAX_ARRAY_ELEMENTS)?;
    usize_count(cardinality)
}

fn shift_days_from_start(origin: chrono::NaiveDate, months: i32, days: i32) -> Result<i32> {
    let shifted = crate::datetime::spark_add_months(origin, months)
        .ok_or_else(|| DataFusionError::Execution("sequence month step out of range".to_owned()))?;
    let shifted = shifted
        .checked_add_signed(chrono::TimeDelta::try_days(i64::from(days)).ok_or_else(|| {
            DataFusionError::Execution("sequence day step out of range".to_owned())
        })?)
        .ok_or_else(|| DataFusionError::Execution("sequence day step out of range".to_owned()))?;
    days_from_ymd(shifted)
}

fn push_over_ceiling(len: usize) -> Result<()> {
    refuse_if_over_ceiling(
        "sequence",
        u64::try_from(len)
            .map_err(|_| DataFusionError::Execution("sequence row count overflows".to_owned()))?,
        DEFAULT_MAX_ARRAY_ELEMENTS,
    )
}

fn collect_dates(start: i32, stop: i32, stride: &IntervalMonthDayNano) -> Result<Vec<i32>> {
    if stride.months == 0 {
        let count = day_count(start, stop, stride.days)?;
        let mut values = Vec::with_capacity(count);
        let mut current = start;
        for index in 0..count {
            values.push(current);
            if index + 1 < count {
                current = current.checked_add(stride.days).ok_or_else(|| {
                    DataFusionError::Execution("sequence date out of range".to_owned())
                })?;
            }
        }
        return Ok(values);
    }
    let origin = ymd_from_days(start)?;
    let ascending = stop > start;
    let mut values = Vec::new();
    let mut previous: Option<i32> = None;
    let mut index: i32 = 0;
    loop {
        let months = stride.months.checked_mul(index).ok_or_else(|| {
            DataFusionError::Execution("sequence month step out of range".to_owned())
        })?;
        let days = stride.days.checked_mul(index).ok_or_else(|| {
            DataFusionError::Execution("sequence day step out of range".to_owned())
        })?;
        let element = shift_days_from_start(origin, months, days)?;
        if previous == Some(element) {
            break;
        }
        if (ascending && element > stop) || (!ascending && element < stop) {
            break;
        }
        values.push(element);
        push_over_ceiling(values.len())?;
        previous = Some(element);
        index = index.checked_add(1).ok_or_else(|| {
            DataFusionError::Execution("sequence month step out of range".to_owned())
        })?;
    }
    Ok(values)
}

pub(crate) fn timestamp_row(
    start: i64,
    stop: i64,
    stride: Option<IntervalMonthDayNano>,
) -> Result<Option<Vec<i64>>> {
    match stride {
        None => {
            let default = if stop >= start {
                IntervalMonthDayNano::new(0, 0, 1_000_000_000)
            } else {
                IntervalMonthDayNano::new(0, 0, -1_000_000_000)
            };
            Ok(Some(collect_timestamps(start, stop, &default)?))
        }
        Some(interval) => {
            if interval.months == 0 && interval.days == 0 && interval.nanoseconds == 0 {
                return Err(boundary_error(
                    &format_micros(start)?,
                    &format_micros(stop)?,
                    &format_interval(&interval),
                ));
            }
            if interval.nanoseconds % 1_000 != 0 {
                return Err(DataFusionError::Execution(format!(
                    "sequence timestamp step carries sub-microsecond nanos: {}",
                    format_interval(&interval)
                )));
            }
            if start == stop {
                return Ok(Some(vec![start]));
            }
            let sign = if interval.months != 0 {
                i64::from(interval.months.signum())
            } else if interval.days != 0 {
                i64::from(interval.days.signum())
            } else {
                interval.nanoseconds.signum()
            };
            let want = if stop > start { 1 } else { -1 };
            if sign != want {
                return Err(boundary_error(
                    &format_micros(start)?,
                    &format_micros(stop)?,
                    &format_interval(&interval),
                ));
            }
            Ok(Some(collect_timestamps(start, stop, &interval)?))
        }
    }
}

fn shift_micros_from_start(start: i64, months: i32, days: i64, nanos: i64) -> Result<i64> {
    let naive = crate::datetime::datetime_from_micros(start)
        .ok_or_else(|| DataFusionError::Execution("sequence timestamp out of range".to_owned()))?;
    let date = if months != 0 {
        crate::datetime::spark_add_months(naive.date(), months).ok_or_else(|| {
            DataFusionError::Execution("sequence month step out of range".to_owned())
        })?
    } else {
        naive.date()
    };
    let shifted = date.and_time(naive.time());
    let shifted = shifted
        .checked_add_signed(chrono::TimeDelta::try_days(days).ok_or_else(|| {
            DataFusionError::Execution("sequence timestamp out of range".to_owned())
        })?)
        .ok_or_else(|| DataFusionError::Execution("sequence timestamp out of range".to_owned()))?;
    shifted
        .and_utc()
        .timestamp_micros()
        .checked_add(nanos / 1_000)
        .ok_or_else(|| DataFusionError::Execution("sequence timestamp out of range".to_owned()))
}

fn sequence_cardinality_from_micros(first: i64, last: i64, stride: i64) -> Option<u64> {
    if stride == 0 {
        return None;
    }
    let span = (i128::from(last) - i128::from(first)) / i128::from(stride);
    if span < 0 {
        return None;
    }
    u64::try_from(span).ok()?.checked_add(1)
}

fn collect_timestamps(start: i64, stop: i64, stride: &IntervalMonthDayNano) -> Result<Vec<i64>> {
    if stride.months == 0 {
        let step_micros = i64::from(stride.days)
            .checked_mul(86_400_000_000)
            .and_then(|day_micros| day_micros.checked_add(stride.nanoseconds / 1_000));
        if let Some(step) = step_micros
            && let Some(cardinality) = sequence_cardinality_from_micros(start, stop, step)
        {
            refuse_if_over_ceiling("sequence", cardinality, DEFAULT_MAX_ARRAY_ELEMENTS)?;
            let count = usize_count(cardinality)?;
            let mut values = Vec::with_capacity(count);
            let mut current = start;
            for index in 0..count {
                values.push(current);
                if index + 1 < count {
                    current = current.checked_add(step).ok_or_else(|| {
                        DataFusionError::Execution("sequence timestamp out of range".to_owned())
                    })?;
                }
            }
            return Ok(values);
        }
    }
    let ascending = stop > start;
    let mut values = Vec::new();
    let mut previous: Option<i64> = None;
    let mut index: i32 = 0;
    loop {
        let months = stride.months.checked_mul(index).ok_or_else(|| {
            DataFusionError::Execution("sequence month step out of range".to_owned())
        })?;
        let days = i64::from(stride.days)
            .checked_mul(i64::from(index))
            .ok_or_else(|| {
                DataFusionError::Execution("sequence day step out of range".to_owned())
            })?;
        let nanos = stride
            .nanoseconds
            .checked_mul(i64::from(index))
            .ok_or_else(|| {
                DataFusionError::Execution("sequence timestamp out of range".to_owned())
            })?;
        let element = shift_micros_from_start(start, months, days, nanos)?;
        if previous == Some(element) {
            break;
        }
        if (ascending && element > stop) || (!ascending && element < stop) {
            break;
        }
        values.push(element);
        push_over_ceiling(values.len())?;
        previous = Some(element);
        index = index.checked_add(1).ok_or_else(|| {
            DataFusionError::Execution("sequence month step out of range".to_owned())
        })?;
    }
    Ok(values)
}

fn epoch_date() -> Result<chrono::NaiveDate> {
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| DataFusionError::Execution("sequence date out of range".to_owned()))
}

fn ymd_from_days(days: i32) -> Result<chrono::NaiveDate> {
    let epoch = epoch_date()?;
    let delta = chrono::TimeDelta::try_days(i64::from(days))
        .ok_or_else(|| DataFusionError::Execution("sequence date out of range".to_owned()))?;
    epoch
        .checked_add_signed(delta)
        .ok_or_else(|| DataFusionError::Execution("sequence date out of range".to_owned()))
}

fn days_from_ymd(date: chrono::NaiveDate) -> Result<i32> {
    let span = date.signed_duration_since(epoch_date()?).num_days();
    i32::try_from(span)
        .map_err(|_| DataFusionError::Execution("sequence date out of range".to_owned()))
}

pub(crate) fn format_days(days: i32) -> Result<String> {
    Ok(ymd_from_days(days)?.to_string())
}

fn format_micros(micros: i64) -> Result<String> {
    crate::datetime::datetime_from_micros(micros)
        .map(|naive| naive.to_string())
        .ok_or_else(|| DataFusionError::Execution("sequence timestamp out of range".to_owned()))
}

fn format_interval(interval: &IntervalMonthDayNano) -> String {
    let mut parts = Vec::new();
    if interval.months != 0 {
        parts.push(format!("{} months", interval.months));
    }
    if interval.days != 0 {
        parts.push(format!("{} days", interval.days));
    }
    if interval.nanoseconds != 0 {
        parts.push(format!("{} nanos", interval.nanoseconds));
    }
    if parts.is_empty() {
        parts.push("0".to_owned());
    }
    parts.join(" ")
}
