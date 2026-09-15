use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Int64Array, IntervalMonthDayNanoArray, ListArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, IntervalMonthDayNano, IntervalUnit, TimeUnit,
};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

#[must_use]
pub fn sequence_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSequence::new()))
}

#[derive(Debug)]
struct SparkSequence {
    signature: Signature,
}

impl SparkSequence {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSequence {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSequence {}

impl Hash for SparkSequence {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    Int,
    Date,
    Timestamp,
}

fn family_of(data_type: &DataType) -> Option<Family> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Some(Family::Int),
        DataType::Date32 | DataType::Date64 => Some(Family::Date),
        DataType::Timestamp(_, _) => Some(Family::Timestamp),
        _ => None,
    }
}

fn int_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        _ => None,
    }
}

fn wrong_input_types(arg_types: &[DataType]) -> DataFusionError {
    let rendered: Vec<String> = arg_types
        .iter()
        .map(crate::collection::spark_type_name)
        .collect();
    DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES] Cannot resolve \"sequence({})\" due to \
         data type mismatch: `sequence` uses the wrong parameter type. The parameter type must \
         conform to:",
        rendered.join(", ")
    ))
}

fn month_day_nano() -> DataType {
    DataType::Interval(IntervalUnit::MonthDayNano)
}

fn plan_sequence(arg_types: &[DataType]) -> Result<Family> {
    if arg_types.len() != 2 && arg_types.len() != 3 {
        return Err(DataFusionError::Plan(format!(
            "'sequence' expects (start, stop[, step]), got {} argument(s)",
            arg_types.len()
        )));
    }
    let start_family = if arg_types[0] == DataType::Null {
        None
    } else {
        Some(family_of(&arg_types[0]).ok_or_else(|| wrong_input_types(arg_types))?)
    };
    let stop_family = if arg_types[1] == DataType::Null {
        None
    } else {
        Some(family_of(&arg_types[1]).ok_or_else(|| wrong_input_types(arg_types))?)
    };
    let family = match (start_family, stop_family) {
        (Some(first), Some(second)) if first == second => first,
        (Some(first), None) | (None, Some(first)) => first,
        _ => return Err(wrong_input_types(arg_types)),
    };
    let step_type = arg_types.get(2).unwrap_or(&DataType::Null);
    match family {
        Family::Int => {
            if *step_type != DataType::Null && family_of(step_type) != Some(Family::Int) {
                return Err(wrong_input_types(arg_types));
            }
            for arg_type in arg_types {
                if *arg_type != DataType::Null && int_rank(arg_type).is_none() {
                    return Err(wrong_input_types(arg_types));
                }
            }
            Ok(family)
        }
        Family::Date | Family::Timestamp => {
            if *step_type != DataType::Null && *step_type != month_day_nano() {
                return Err(wrong_input_types(arg_types));
            }
            Ok(family)
        }
    }
}

fn sequence_element(family: Family, arg_types: &[DataType]) -> DataType {
    match family {
        Family::Int => {
            let mut widest = DataType::Int32;
            let mut rank: Option<u8> = None;
            for arg_type in arg_types {
                if let Some(next) = int_rank(arg_type)
                    && rank.is_none_or(|current| next > current)
                {
                    rank = Some(next);
                    widest = arg_type.clone();
                }
            }
            widest
        }
        Family::Date => DataType::Date32,
        Family::Timestamp => {
            if arg_types[0] == DataType::Null {
                arg_types[1].clone()
            } else {
                arg_types[0].clone()
            }
        }
    }
}

impl ScalarUDFImpl for SparkSequence {
    crate::shim_udf_boilerplate!("sequence");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let family = plan_sequence(arg_types)?;
        Ok(DataType::List(Arc::new(Field::new(
            "element",
            sequence_element(family, arg_types),
            false,
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let declared: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            "sequence",
            self.return_type(&declared)?,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        plan_sequence(arg_types)?;
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: arg_values,
            return_field,
            ..
        } = args;
        let DataType::List(element) = return_field.data_type() else {
            return exec_err!("sequence needs a list return");
        };
        let arrays = ColumnarValue::values_to_arrays(&arg_values)?;
        let row_count = arrays.first().map_or(0, Array::len);
        match element.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                invoke_ints(&arrays, row_count, element)
            }
            DataType::Date32 => invoke_dates(&arrays, row_count, element),
            DataType::Timestamp(_, _) => invoke_timestamps(&arrays, row_count, element),
            other => exec_err!("sequence cannot build {other} elements"),
        }
    }
}

fn invoke_ints(arrays: &[ArrayRef], row_count: usize, element: &FieldRef) -> Result<ColumnarValue> {
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    let starts = as_i64(&arrays[0])?;
    let stops = as_i64(&arrays[1])?;
    let strides = if arrays.len() > 2 {
        Some(as_i64(&arrays[2])?)
    } else {
        None
    };
    let mut values: Vec<i64> = Vec::new();
    for row in 0..row_count {
        let explicit = strides.as_ref().map(|strides| {
            if strides.is_null(row) {
                None
            } else {
                Some(strides.value(row))
            }
        });
        if starts.is_null(row) || stops.is_null(row) || explicit == Some(None) {
            validity.push(false);
            any_null = true;
        } else {
            match int_row(starts.value(row), stops.value(row), explicit.flatten()) {
                Err(error) => return Err(error),
                Ok(pieces) => {
                    validity.push(true);
                    values.extend(pieces);
                }
            }
        }
        offsets.push(fit_i32(values.len())?);
    }
    let built: ArrayRef = Arc::new(Int64Array::from(values));
    let shaped = cast(built.as_ref(), element.data_type())?;
    finish_list(element, offsets, validity, any_null, shaped)
}

fn invoke_dates(
    arrays: &[ArrayRef],
    row_count: usize,
    element: &FieldRef,
) -> Result<ColumnarValue> {
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    let starts = as_date_days(&arrays[0])?;
    let stops = as_date_days(&arrays[1])?;
    let strides = if arrays.len() > 2 {
        Some(as_interval(&arrays[2])?)
    } else {
        None
    };
    let mut values: Vec<i32> = Vec::new();
    for row in 0..row_count {
        let step_null = strides.as_ref().is_some_and(|strides| strides.is_null(row));
        if starts.is_null(row) || stops.is_null(row) || step_null {
            validity.push(false);
            any_null = true;
        } else {
            let interval = strides.as_ref().map(|strides| strides.value(row));
            match date_row(starts.value(row), stops.value(row), interval) {
                Err(error) => return Err(error),
                Ok(None) => {
                    validity.push(false);
                    any_null = true;
                }
                Ok(Some(pieces)) => {
                    validity.push(true);
                    values.extend(pieces);
                }
            }
        }
        offsets.push(fit_i32(values.len())?);
    }
    let shaped: ArrayRef = Arc::new(Date32Array::from(values));
    finish_list(element, offsets, validity, any_null, shaped)
}

fn invoke_timestamps(
    arrays: &[ArrayRef],
    row_count: usize,
    element: &FieldRef,
) -> Result<ColumnarValue> {
    let mut offsets: Vec<i32> = Vec::with_capacity(row_count + 1);
    offsets.push(0);
    let mut validity: Vec<bool> = Vec::with_capacity(row_count);
    let mut any_null = false;
    let starts = as_micros(&arrays[0])?;
    let stops = as_micros(&arrays[1])?;
    let strides = if arrays.len() > 2 {
        Some(as_interval(&arrays[2])?)
    } else {
        None
    };
    let mut values: Vec<i64> = Vec::new();
    for row in 0..row_count {
        let step_null = strides.as_ref().is_some_and(|strides| strides.is_null(row));
        if starts.is_null(row) || stops.is_null(row) || step_null {
            validity.push(false);
            any_null = true;
        } else {
            let interval = strides.as_ref().map(|strides| strides.value(row));
            match timestamp_row(starts.value(row), stops.value(row), interval) {
                Err(error) => return Err(error),
                Ok(None) => {
                    validity.push(false);
                    any_null = true;
                }
                Ok(Some(pieces)) => {
                    validity.push(true);
                    values.extend(pieces);
                }
            }
        }
        offsets.push(fit_i32(values.len())?);
    }
    let built: ArrayRef = Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"));
    let shaped = cast(built.as_ref(), element.data_type())?;
    finish_list(element, offsets, validity, any_null, shaped)
}

fn downcast_primitive<T: datafusion::arrow::array::ArrowPrimitiveType>(
    array: &ArrayRef,
    what: &str,
) -> Result<datafusion::arrow::array::PrimitiveArray<T>> {
    array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::PrimitiveArray<T>>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution(format!("sequence {what} values unreadable")))
}

fn as_i64(array: &ArrayRef) -> Result<Int64Array> {
    if array.data_type() == &DataType::Int64 {
        return downcast_primitive(array, "int");
    }
    downcast_primitive(&cast(array.as_ref(), &DataType::Int64)?, "int")
}

fn as_date_days(array: &ArrayRef) -> Result<Date32Array> {
    if array.data_type() == &DataType::Date32 {
        return downcast_primitive(array, "date");
    }
    downcast_primitive(&cast(array.as_ref(), &DataType::Date32)?, "date")
}

fn microsecond_utc() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn as_micros(array: &ArrayRef) -> Result<TimestampMicrosecondArray> {
    if array.data_type() == &microsecond_utc() {
        return downcast_primitive(array, "timestamp");
    }
    match array.data_type() {
        DataType::Timestamp(_, _) => {
            downcast_primitive(&cast(array.as_ref(), &microsecond_utc())?, "timestamp")
        }
        _ => Err(DataFusionError::Execution(
            "sequence timestamp arm needs timestamp values".to_owned(),
        )),
    }
}

fn as_interval(array: &ArrayRef) -> Result<IntervalMonthDayNanoArray> {
    downcast_primitive(array, "interval")
}

fn fit_i32(value: usize) -> Result<i32> {
    i32::try_from(value)
        .map_err(|_| DataFusionError::Execution("sequence row count does not fit i32".to_owned()))
}

fn finish_list(
    element: &FieldRef,
    offsets: Vec<i32>,
    validity: Vec<bool>,
    any_null: bool,
    values: ArrayRef,
) -> Result<ColumnarValue> {
    let nulls = if any_null {
        Some(NullBuffer::from(validity))
    } else {
        None
    };
    Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
        Arc::clone(element),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    )?)))
}

fn boundary_error(start: &str, stop: &str, stride: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "requirement failed: Illegal sequence boundaries: {start} to {stop} by {stride}"
    ))
}

fn int_row(start: i64, stop: i64, stride: Option<i64>) -> Result<Vec<i64>> {
    match stride {
        None => {
            let default = if stop >= start { 1 } else { -1 };
            Ok(collect_ints(start, stop, default))
        }
        Some(given) => {
            if given == 0 || (given > 0 && start > stop) || (given < 0 && start < stop) {
                return Err(int_step_error(start, stop, given));
            }
            Ok(collect_ints(start, stop, given))
        }
    }
}

fn collect_ints(start: i64, stop: i64, stride: i64) -> Vec<i64> {
    let mut values = Vec::new();
    let mut current = start;
    loop {
        values.push(current);
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
    values
}

fn int_step_error(start: i64, stop: i64, stride: i64) -> DataFusionError {
    boundary_error(&start.to_string(), &stop.to_string(), &stride.to_string())
}

fn date_row(
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

fn collect_dates(start: i32, stop: i32, stride: &IntervalMonthDayNano) -> Result<Vec<i32>> {
    let mut values = vec![start];
    let mut current = start;
    loop {
        if current == stop {
            break;
        }
        let next = add_date_interval(current, stride)?;
        if (stop > start && next > stop) || (stop < start && next < stop) || next == current {
            break;
        }
        values.push(next);
        current = next;
    }
    Ok(values)
}

fn timestamp_row(
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

fn collect_timestamps(start: i64, stop: i64, stride: &IntervalMonthDayNano) -> Result<Vec<i64>> {
    let mut values = vec![start];
    let mut current = start;
    loop {
        if current == stop {
            break;
        }
        let next = add_timestamp_interval(current, stride)?;
        if (stop > start && next > stop) || (stop < start && next < stop) || next == current {
            break;
        }
        values.push(next);
        current = next;
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

fn add_date_interval(days: i32, step: &IntervalMonthDayNano) -> Result<i32> {
    let date = ymd_from_days(days)?;
    let shifted = if step.months != 0 {
        crate::datetime::spark_add_months(date, step.months).ok_or_else(|| {
            DataFusionError::Execution("sequence month step out of range".to_owned())
        })?
    } else {
        date
    };
    let delta = chrono::TimeDelta::try_days(i64::from(step.days))
        .ok_or_else(|| DataFusionError::Execution("sequence day step out of range".to_owned()))?;
    let shifted = shifted
        .checked_add_signed(delta)
        .ok_or_else(|| DataFusionError::Execution("sequence day step out of range".to_owned()))?;
    days_from_ymd(shifted)
}

fn add_timestamp_interval(micros: i64, step: &IntervalMonthDayNano) -> Result<i64> {
    let naive = crate::datetime::datetime_from_micros(micros)
        .ok_or_else(|| DataFusionError::Execution("sequence timestamp out of range".to_owned()))?;
    let shifted = if step.months != 0 {
        let date =
            crate::datetime::spark_add_months(naive.date(), step.months).ok_or_else(|| {
                DataFusionError::Execution("sequence month step out of range".to_owned())
            })?;
        date.and_time(naive.time())
    } else {
        naive
    };
    let delta = chrono::TimeDelta::try_days(i64::from(step.days))
        .ok_or_else(|| DataFusionError::Execution("sequence timestamp out of range".to_owned()))?;
    let shifted = shifted
        .checked_add_signed(delta)
        .ok_or_else(|| DataFusionError::Execution("sequence timestamp out of range".to_owned()))?;
    shifted
        .and_utc()
        .timestamp_micros()
        .checked_add(step.nanoseconds / 1_000)
        .ok_or_else(|| DataFusionError::Execution("sequence timestamp out of range".to_owned()))
}

fn format_days(days: i32) -> Result<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn values_of(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let lists = batches[0].column(0).as_list::<i32>();
        assert_eq!(lists.len(), 1);
        let inner = lists.value(0);
        if let Some(numbers) = inner.as_any().downcast_ref::<Int64Array>() {
            return numbers.values().to_vec();
        }
        inner
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .expect("int values")
            .values()
            .iter()
            .map(|value| i64::from(*value))
            .collect()
    }

    #[tokio::test]
    async fn sequence_int_widths_and_steps() {
        let ctx = ctx();
        assert_eq!(
            values_of(&ctx, "SELECT sequence(1, 3)").await,
            vec![1, 2, 3]
        );
        assert_eq!(
            values_of(&ctx, "SELECT sequence(3, 1)").await,
            vec![3, 2, 1]
        );
        assert_eq!(
            values_of(&ctx, "SELECT sequence(1, 10, 3)").await,
            vec![1, 4, 7, 10]
        );
        let batches = ctx
            .sql("SELECT sequence(CAST(1 AS TINYINT), CAST(3 AS TINYINT))")
            .await
            .expect("plan tinyint")
            .collect()
            .await
            .expect("execute tinyint");
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Int8, false)))
        );
    }

    #[tokio::test]
    async fn sequence_bigint_keeps_width() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT sequence(CAST(1 AS BIGINT), CAST(3 AS BIGINT))")
            .await
            .expect("plan bigint")
            .collect()
            .await
            .expect("execute bigint");
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Int64, false)))
        );
    }

    #[tokio::test]
    async fn sequence_step_errors_carry_spark_text() {
        let ctx = ctx();
        for (sql, text) in [
            (
                "SELECT sequence(1, 3, 0)",
                "requirement failed: Illegal sequence boundaries: 1 to 3 by 0",
            ),
            (
                "SELECT sequence(1, 3, -1)",
                "requirement failed: Illegal sequence boundaries: 1 to 3 by -1",
            ),
        ] {
            let error = ctx
                .sql(sql)
                .await
                .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
                .collect()
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(text), "{sql}: {error}");
        }
    }

    #[tokio::test]
    async fn sequence_decimal_bound_refuses_wrong_input_types() {
        let ctx = ctx();
        let error = ctx
            .sql("SELECT sequence(1.5, 3)")
            .await
            .err()
            .unwrap_or_else(|| panic!("should refuse"))
            .to_string();
        assert!(
            error.contains("DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn sequence_null_bound_answers_null_with_empty_contains() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT sequence(1, CAST(NULL AS INT))")
            .await
            .expect("plan null bound")
            .collect()
            .await
            .expect("execute null bound");
        assert_eq!(
            batches[0].column(0).data_type(),
            &DataType::List(Arc::new(Field::new("element", DataType::Int32, false)))
        );
        assert!(batches[0].column(0).is_null(0));
    }

    #[tokio::test]
    async fn sequence_dates_and_month_step() {
        let ctx = ctx();
        let batches = ctx
            .sql("SELECT sequence(DATE'2024-01-01', DATE'2024-01-03')")
            .await
            .expect("plan dates")
            .collect()
            .await
            .expect("execute dates");
        let lists = batches[0].column(0).as_list::<i32>();
        let row = lists.value(0);
        let days = row
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date values");
        assert_eq!(days.values(), &[19723, 19724, 19725]);
        let batches = ctx
            .sql("SELECT sequence(DATE'2024-01-01', DATE'2024-03-01', INTERVAL 1 MONTH)")
            .await
            .expect("plan month step")
            .collect()
            .await
            .expect("execute month step");
        let lists = batches[0].column(0).as_list::<i32>();
        let row = lists.value(0);
        let days = row
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date values");
        assert_eq!(days.values(), &[19723, 19754, 19783]);
    }
}
