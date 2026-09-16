use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, ListArray, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

pub const WINDOW_FUNCTION_NAME: &str = "window";
pub const WINDOW_STARTS_NAME: &str = "__repark_window_starts__";
pub const WINDOW_OUTPUT_NAME: &str = "window";
const START_FIELD_NAME: &str = "start";
const END_FIELD_NAME: &str = "end";

#[must_use]
pub fn window_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTimeWindow::new()))
}

#[must_use]
pub fn window_starts_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkWindowStarts::new()))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        window_udf(),
        window_starts_udf(),
        crate::spark_window_time::window_time_udf(),
    ]
}

pub(crate) fn parse_window_duration(text: &str) -> Result<i64> {
    let total = total_duration_micros(text, MonthRefusal::Legacy)?;
    if total <= 0 {
        return Err(cannot_parse_interval(text));
    }
    i64::try_from(total).map_err(|_| cannot_parse_interval(text))
}

pub(crate) fn parse_session_gap(text: &str) -> Result<i64> {
    let total = total_duration_micros(text, MonthRefusal::Session)?;
    i64::try_from(total).map_err(|_| cannot_parse_interval(text))
}

pub(crate) fn parse_window_offset(text: &str) -> Result<i64> {
    let total = total_duration_micros(text, MonthRefusal::Legacy)?;
    i64::try_from(total).map_err(|_| cannot_parse_interval(text))
}

pub(crate) fn parse_session_gap_parts(text: &str) -> Result<(i32, i64)> {
    let mut months: Option<i64> = None;
    let mut micros: i128 = 0;
    let mut seen_pair = false;
    for pair in split_duration_pairs(text)? {
        let (amount, unit) = pair;
        if is_month_unit(unit.as_str()) || is_year_unit(unit.as_str()) {
            let step = if is_year_unit(unit.as_str()) {
                amount.checked_mul(12)
            } else {
                Some(amount)
            };
            let step = step.ok_or_else(|| cannot_parse_interval(text))?;
            months = Some(
                months
                    .unwrap_or(0)
                    .checked_add(step)
                    .ok_or_else(|| cannot_parse_interval(text))?,
            );
        } else {
            micros += i128::from(amount)
                * unit_factor_micros(unit.as_str(), text, MonthRefusal::Session)?;
        }
        seen_pair = true;
    }
    if !seen_pair {
        return Err(cannot_parse_interval(text));
    }
    let months = i32::try_from(months.unwrap_or(0)).map_err(|_| cannot_parse_interval(text))?;
    let micros = i64::try_from(micros).map_err(|_| cannot_parse_interval(text))?;
    Ok((months, micros))
}

fn is_month_unit(unit: &str) -> bool {
    unit.eq_ignore_ascii_case("month") || unit.eq_ignore_ascii_case("months")
}

fn is_year_unit(unit: &str) -> bool {
    unit.eq_ignore_ascii_case("year") || unit.eq_ignore_ascii_case("years")
}

pub(crate) fn check_window_spec(
    window: i64,
    slide: i64,
    offset: i64,
    rendered: &str,
) -> Result<()> {
    if slide > window {
        return plan_err!(
            "[DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION] Cannot resolve \"window({rendered}, {window}, {slide}, {offset})\" due to data type mismatch: The `slide_duration`({slide}L) must be <= the `window_duration`({window}L). SQLSTATE: 42K09;"
        );
    }
    let absolute = offset.unsigned_abs();
    if i128::from(absolute) >= i128::from(slide) {
        return plan_err!(
            "[DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION] Cannot resolve \"window({rendered}, {window}, {slide}, {offset})\" due to data type mismatch: The `abs(start_time)`({absolute}L) must be < the `slide_duration`({slide}L). SQLSTATE: 42K09;"
        );
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum MonthRefusal {
    Legacy,
    Session,
}

fn total_duration_micros(text: &str, refusal: MonthRefusal) -> Result<i128> {
    let mut total: i128 = 0;
    let mut seen_pair = false;
    for pair in split_duration_pairs(text)? {
        let (amount, unit) = pair;
        total += i128::from(amount) * unit_factor_micros(unit.as_str(), text, refusal)?;
        seen_pair = true;
    }
    if !seen_pair {
        return Err(cannot_parse_interval(text));
    }
    Ok(total)
}

fn split_duration_pairs(text: &str) -> Result<Vec<(i64, String)>> {
    let mut pairs = Vec::new();
    let mut pending: Option<i64> = None;
    let mut seen_word = false;
    for word in text.split_whitespace() {
        seen_word = true;
        let digits = word
            .strip_prefix('-')
            .unwrap_or(word)
            .chars()
            .take_while(char::is_ascii_digit)
            .count()
            + usize::from(word.starts_with('-'));
        let (amount_text, unit_text) = word.split_at(digits);
        if unit_text.is_empty() {
            if pending.is_some() {
                return Err(cannot_parse_interval(text));
            }
            match amount_text.parse::<i64>() {
                Ok(amount) => pending = Some(amount),
                Err(_) => return Err(cannot_parse_interval(text)),
            }
            continue;
        }
        let amount = if amount_text.is_empty() {
            match pending.take() {
                Some(amount) => amount,
                None => return Err(cannot_parse_interval(text)),
            }
        } else {
            if pending.is_some() {
                return Err(cannot_parse_interval(text));
            }
            match amount_text.parse::<i64>() {
                Ok(amount) => amount,
                Err(_) => return Err(cannot_parse_interval(text)),
            }
        };
        pairs.push((amount, unit_text.to_string()));
    }
    if !seen_word || pending.is_some() {
        return Err(cannot_parse_interval(text));
    }
    Ok(pairs)
}

fn unit_factor_micros(unit: &str, text: &str, refusal: MonthRefusal) -> Result<i128> {
    if unit.eq_ignore_ascii_case("microsecond") || unit.eq_ignore_ascii_case("microseconds") {
        Ok(1)
    } else if unit.eq_ignore_ascii_case("millisecond") || unit.eq_ignore_ascii_case("milliseconds")
    {
        Ok(1_000)
    } else if unit.eq_ignore_ascii_case("second") || unit.eq_ignore_ascii_case("seconds") {
        Ok(1_000_000)
    } else if unit.eq_ignore_ascii_case("minute") || unit.eq_ignore_ascii_case("minutes") {
        Ok(60_000_000)
    } else if unit.eq_ignore_ascii_case("hour") || unit.eq_ignore_ascii_case("hours") {
        Ok(3_600_000_000)
    } else if unit.eq_ignore_ascii_case("day") || unit.eq_ignore_ascii_case("days") {
        Ok(86_400_000_000)
    } else if unit.eq_ignore_ascii_case("week") || unit.eq_ignore_ascii_case("weeks") {
        Ok(604_800_000_000)
    } else if is_month_unit(unit) || is_year_unit(unit) {
        match refusal {
            MonthRefusal::Legacy => plan_err!(
                "[_LEGACY_ERROR_TEMP_3231] Intervals greater than a month is not supported ({text})."
            ),
            MonthRefusal::Session => plan_err!(
                "'session_window' gapDuration '{text}' uses months or years, which have no fixed \
                 length in microseconds"
            ),
        }
    } else {
        Err(cannot_parse_interval(unit))
    }
}

fn cannot_parse_interval(text: &str) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(format!(
        "[CANNOT_PARSE_INTERVAL] Unable to parse '{text}'. Please ensure that the value provided \
         is in a valid format for defining an interval. You can reference the documentation for \
         the correct format. If the issue persists, please double check that the input value is \
         not null or empty and try again. SQLSTATE: 22006"
    ))
}

pub(crate) fn floor_grid_start(instant: i128, slide: i128, offset: i128) -> i128 {
    offset + (instant - offset).div_euclid(slide) * slide
}

pub(crate) fn overlapping_starts(instant: i64, window: i64, slide: i64, offset: i64) -> Vec<i64> {
    let instant = i128::from(instant);
    let window = i128::from(window);
    let slide = i128::from(slide);
    let offset = i128::from(offset);
    let last = floor_grid_start(instant, slide, offset);
    let first = floor_grid_start(instant - window, slide, offset) + slide;
    if first > last {
        return Vec::new();
    }
    let mut starts = Vec::new();
    let mut start = first;
    while start <= last {
        if let Ok(narrow) = i64::try_from(start) {
            starts.push(narrow);
        }
        start += slide;
    }
    starts
}

fn micros_per_unit(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => 1_000_000,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1,
        TimeUnit::Nanosecond => 0,
    }
}

pub(crate) fn timestamp_micros_batch(data_type: &DataType, array: &ArrayRef) -> Result<Vec<i64>> {
    match data_type {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let values = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::TimestampSecondArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "'window' could not read a TIMESTAMP(SECOND) value".to_string(),
                    )
                })?;
            values
                .values()
                .iter()
                .map(|value| {
                    i64::try_from(i128::from(*value) * 1_000_000).map_err(|_| {
                        datafusion::common::DataFusionError::Execution(
                            "'window' timestamp is out of the microsecond range".to_string(),
                        )
                    })
                })
                .collect()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::TimestampMillisecondArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "'window' could not read a TIMESTAMP(MILLISECOND) value".to_string(),
                    )
                })?;
            values
                .values()
                .iter()
                .map(|value| {
                    i64::try_from(i128::from(*value) * 1_000).map_err(|_| {
                        datafusion::common::DataFusionError::Execution(
                            "'window' timestamp is out of the microsecond range".to_string(),
                        )
                    })
                })
                .collect()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(array
            .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>()
            .values()
            .to_vec()),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(
                        "'window' could not read a TIMESTAMP(NANOSECOND) value".to_string(),
                    )
                })?;
            Ok(values
                .values()
                .iter()
                .map(|value| value.div_euclid(1_000))
                .collect())
        }
        other => {
            exec_err!("'window' timeColumn must be a TIMESTAMP, got {other}")
        }
    }
}

pub(crate) fn micros_to_timestamp_array(
    data_type: &DataType,
    values: Vec<i64>,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    let (unit, timezone) = match data_type {
        DataType::Timestamp(unit, timezone) => (*unit, timezone.clone()),
        other => {
            return exec_err!("'window' timeColumn must be a TIMESTAMP, got {other}");
        }
    };
    let factor = micros_per_unit(unit);
    let scaled = if factor <= 1 {
        if factor == 0 {
            values
                .iter()
                .map(|micros| {
                    i64::try_from(i128::from(*micros) * 1_000).map_err(|_| {
                        datafusion::common::DataFusionError::Execution(
                            "'window' bucket is out of the nanosecond range".to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<i64>>>()?
        } else {
            values
        }
    } else {
        values
            .iter()
            .map(|micros| {
                i64::try_from(i128::from(*micros).div_euclid(factor)).map_err(|_| {
                    datafusion::common::DataFusionError::Execution(
                        "'window' bucket is out of range".to_string(),
                    )
                })
            })
            .collect::<Result<Vec<i64>>>()?
    };
    let buffer = datafusion::arrow::buffer::ScalarBuffer::from(scaled);
    match unit {
        TimeUnit::Second => Ok(Arc::new(
            datafusion::arrow::array::TimestampSecondArray::new(buffer, nulls)
                .with_timezone_opt(timezone),
        )),
        TimeUnit::Millisecond => Ok(Arc::new(
            datafusion::arrow::array::TimestampMillisecondArray::new(buffer, nulls)
                .with_timezone_opt(timezone),
        )),
        TimeUnit::Microsecond => Ok(Arc::new(
            TimestampMicrosecondArray::new(buffer, nulls).with_timezone_opt(timezone),
        )),
        TimeUnit::Nanosecond => Ok(Arc::new(
            datafusion::arrow::array::TimestampNanosecondArray::new(buffer, nulls)
                .with_timezone_opt(timezone),
        )),
    }
}

pub(crate) fn window_fields(time_type: &DataType) -> Fields {
    Fields::from(vec![
        Arc::new(Field::new(START_FIELD_NAME, time_type.clone(), false)),
        Arc::new(Field::new(END_FIELD_NAME, time_type.clone(), false)),
    ])
}

pub(crate) fn window_struct_type(time_type: &DataType) -> DataType {
    DataType::Struct(window_fields(time_type))
}

fn fallback_struct_type() -> DataType {
    window_struct_type(&DataType::Timestamp(
        TimeUnit::Microsecond,
        Some(Arc::from("UTC")),
    ))
}

fn string_scalar(function: &str, values: &[ColumnarValue], position: usize) -> Result<String> {
    if let Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(text)))) = values.get(position) {
        Ok(text.clone())
    } else {
        exec_err!("'{function}' duration arguments must be literal strings")
    }
}

fn int_scalar(function: &str, values: &[ColumnarValue], position: usize) -> Result<i64> {
    if let Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(value)))) = values.get(position) {
        Ok(*value)
    } else if let Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(value)))) =
        values.get(position)
    {
        Ok(i64::from(*value))
    } else {
        exec_err!("'{function}' takes internal microsecond literals")
    }
}

fn batch_nulls(time: &ArrayRef) -> Option<NullBuffer> {
    time.nulls().filter(|nulls| nulls.null_count() > 0).cloned()
}

fn window_batch(
    fields: Fields,
    time_type: &DataType,
    time: &ArrayRef,
    rows: usize,
    window: i64,
    slide: i64,
    offset: i64,
) -> Result<ArrayRef> {
    if slide <= 0 {
        return exec_err!("'window' slide must be positive, got {slide}");
    }
    let instants = timestamp_micros_batch(time_type, time)?;
    let validity = batch_nulls(time);
    let tumbling = window == slide;
    let mut starts: Vec<i64> = Vec::with_capacity(rows);
    let mut ends: Vec<i64> = Vec::with_capacity(rows);
    for instant in instants.iter().take(rows) {
        if validity
            .as_ref()
            .is_some_and(|nulls| nulls.is_null(starts.len()))
        {
            starts.push(0);
            ends.push(0);
            continue;
        }
        let start = if tumbling {
            floor_grid_start(i128::from(*instant), i128::from(slide), i128::from(offset))
        } else {
            overlapping_starts(*instant, window, slide, offset)
                .last()
                .map_or_else(
                    || {
                        floor_grid_start(
                            i128::from(*instant),
                            i128::from(slide),
                            i128::from(offset),
                        )
                    },
                    |narrow| i128::from(*narrow),
                )
        };
        let end = start + i128::from(window);
        starts.push(i64::try_from(start).map_err(|_| {
            datafusion::common::DataFusionError::Execution(
                "'window' bucket start is out of range".to_string(),
            )
        })?);
        ends.push(i64::try_from(end).map_err(|_| {
            datafusion::common::DataFusionError::Execution(
                "'window' bucket end is out of range".to_string(),
            )
        })?);
    }
    let start_array = micros_to_timestamp_array(time_type, starts, validity.clone())?;
    let end_array = micros_to_timestamp_array(time_type, ends, validity.clone())?;
    Ok(Arc::new(StructArray::try_new(
        fields,
        vec![start_array, end_array],
        validity,
    )?))
}

fn window_list_batch(
    fields: Fields,
    time_type: &DataType,
    time: &ArrayRef,
    rows: usize,
    window: i64,
    slide: i64,
    offset: i64,
) -> Result<ArrayRef> {
    if window <= 0 || slide <= 0 {
        return exec_err!("'window' window and slide must be positive, got {window} and {slide}");
    }
    let instants = timestamp_micros_batch(time_type, time)?;
    let nulls = time.nulls();
    let per_row = usize::try_from(window / slide + 1).map_err(|_| {
        datafusion::common::DataFusionError::Execution(
            "'window' expanded past the batch limit".to_string(),
        )
    })?;
    let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
    let mut starts: Vec<i64> = Vec::with_capacity(rows.saturating_mul(per_row));
    let mut ends: Vec<i64> = Vec::with_capacity(rows.saturating_mul(per_row));
    offsets.push(0);
    let slide_grid = i128::from(slide);
    let window_grid = i128::from(window);
    let offset_grid = i128::from(offset);
    for (row, instant) in instants.iter().enumerate().take(rows) {
        if nulls.is_some_and(|valid| valid.is_null(row)) {
            offsets.push(i32::try_from(starts.len()).map_err(|_| {
                datafusion::common::DataFusionError::Execution(
                    "'window' expanded past the batch limit".to_string(),
                )
            })?);
            continue;
        }
        let last = floor_grid_start(i128::from(*instant), slide_grid, offset_grid);
        let first = floor_grid_start(i128::from(*instant) - window_grid, slide_grid, offset_grid)
            + slide_grid;
        let mut start = first;
        while start <= last {
            let narrow = i64::try_from(start).map_err(|_| {
                datafusion::common::DataFusionError::Execution(
                    "'window' bucket start is out of range".to_string(),
                )
            })?;
            let stop = narrow.checked_add(window).ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(
                    "'window' bucket end is out of range".to_string(),
                )
            })?;
            starts.push(narrow);
            ends.push(stop);
            start += slide_grid;
        }
        offsets.push(i32::try_from(starts.len()).map_err(|_| {
            datafusion::common::DataFusionError::Execution(
                "'window' expanded past the batch limit".to_string(),
            )
        })?);
    }
    let start_array = micros_to_timestamp_array(time_type, starts, None)?;
    let end_array = micros_to_timestamp_array(time_type, ends, None)?;
    let structs = StructArray::try_new(fields, vec![start_array, end_array], None)?;
    Ok(Arc::new(ListArray::try_new(
        Arc::new(Field::new("item", structs.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(structs),
        None,
    )?))
}

#[derive(Debug)]
struct SparkTimeWindow {
    signature: Signature,
}

impl SparkTimeWindow {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkTimeWindow {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkTimeWindow {}

impl Hash for SparkTimeWindow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn time_argument_type(arg_types: &[DataType]) -> Result<DataType> {
    match arg_types.first() {
        Some(DataType::Timestamp(unit, timezone)) => {
            Ok(DataType::Timestamp(*unit, timezone.clone()))
        }
        Some(DataType::Date32) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        Some(other) => {
            exec_err!("'window' timeColumn must be a TIMESTAMP, got {other}")
        }
        None => exec_err!("'window' requires a time column"),
    }
}

fn utf8_argument_type(function: &str, arg_types: &[DataType], position: usize) -> Result<DataType> {
    match arg_types.get(position) {
        Some(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null) => {
            Ok(DataType::Utf8)
        }
        Some(other) => {
            exec_err!("'{function}' duration arguments must be strings, got {other}")
        }
        None => exec_err!("'{function}' is missing duration argument {}", position + 1),
    }
}

fn window_durations(values: &[ColumnarValue], rendered: &str) -> Result<(i64, i64, i64)> {
    let window = parse_window_duration(string_scalar(WINDOW_FUNCTION_NAME, values, 1)?.as_str())?;
    let slide = match values.get(2) {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(None) | ScalarValue::Null)) | None => window,
        Some(_) => parse_window_duration(string_scalar(WINDOW_FUNCTION_NAME, values, 2)?.as_str())?,
    };
    let offset = match values.get(3) {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(None) | ScalarValue::Null)) | None => 0,
        Some(_) => parse_window_offset(string_scalar(WINDOW_FUNCTION_NAME, values, 3)?.as_str())?,
    };
    check_window_spec(window, slide, offset, rendered)?;
    Ok((window, slide, offset))
}

impl ScalarUDFImpl for SparkTimeWindow {
    fn name(&self) -> &str {
        WINDOW_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Timestamp(_, _)) => {
                Ok(window_struct_type(&time_argument_type(arg_types)?))
            }
            _ => Ok(fallback_struct_type()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let time_type = match args.arg_fields.first().map(|field| field.data_type()) {
            Some(DataType::Timestamp(_, _)) => time_argument_type(
                &args
                    .arg_fields
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect::<Vec<_>>(),
            )?,
            _ => fallback_struct_type_inner(),
        };
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            window_struct_type(&time_type),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=4).contains(&arg_types.len()) {
            return exec_err!(
                "'window' requires 2 to 4 arguments, got {}",
                arg_types.len()
            );
        }
        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(time_argument_type(arg_types)?);
        for position in 1..arg_types.len() {
            coerced.push(utf8_argument_type(
                WINDOW_FUNCTION_NAME,
                arg_types,
                position,
            )?);
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let fields = match args.return_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => window_fields(&fallback_struct_type_inner()),
        };
        let time_type = match args.arg_fields.first().map(|field| field.data_type()) {
            Some(typed @ DataType::Timestamp(_, _)) => typed.clone(),
            _ => fallback_struct_type_inner(),
        };
        let rendered = args
            .arg_fields
            .first()
            .map_or_else(|| "timeColumn".to_string(), |field| field.name().clone());
        let (window, slide, offset) = window_durations(&args.args, rendered.as_str())?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(time) = arrays.first() else {
            return exec_err!("'window' requires a time column");
        };
        Ok(ColumnarValue::Array(window_batch(
            fields,
            &time_type,
            time,
            args.number_rows,
            window,
            slide,
            offset,
        )?))
    }
}

fn fallback_struct_type_inner() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

#[derive(Debug)]
struct SparkWindowStarts {
    signature: Signature,
}

impl SparkWindowStarts {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkWindowStarts {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkWindowStarts {}

impl Hash for SparkWindowStarts {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn int_argument_type(arg_types: &[DataType], position: usize) -> Result<DataType> {
    match arg_types.get(position) {
        Some(DataType::Int64 | DataType::Int32) => Ok(DataType::Int64),
        Some(other) => {
            exec_err!(
                "'{}' takes internal microsecond literals, got {other}",
                WINDOW_STARTS_NAME
            )
        }
        None => exec_err!(
            "'{}' is missing argument {}",
            WINDOW_STARTS_NAME,
            position + 1
        ),
    }
}

impl ScalarUDFImpl for SparkWindowStarts {
    fn name(&self) -> &str {
        WINDOW_STARTS_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let time_type = match arg_types.first() {
            Some(DataType::Timestamp(_, _)) => time_argument_type(arg_types)?,
            _ => fallback_struct_type_inner(),
        };
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            window_struct_type(&time_type),
            true,
        ))))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let time_type = match args.arg_fields.first().map(|field| field.data_type()) {
            Some(DataType::Timestamp(_, _)) => time_argument_type(
                &args
                    .arg_fields
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect::<Vec<_>>(),
            )?,
            _ => fallback_struct_type_inner(),
        };
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(Arc::new(Field::new(
                "item",
                window_struct_type(&time_type),
                true,
            ))),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 4 {
            return exec_err!(
                "'{}' requires 4 arguments, got {}",
                WINDOW_STARTS_NAME,
                arg_types.len()
            );
        }
        Ok(vec![
            time_argument_type(arg_types)?,
            int_argument_type(arg_types, 1)?,
            int_argument_type(arg_types, 2)?,
            int_argument_type(arg_types, 3)?,
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let fields = match args.return_field.data_type() {
            DataType::List(item) if matches!(item.data_type(), DataType::Struct(_)) => {
                match item.data_type() {
                    DataType::Struct(fields) => fields.clone(),
                    _ => window_fields(&fallback_struct_type_inner()),
                }
            }
            _ => window_fields(&fallback_struct_type_inner()),
        };
        let time_type = match args.arg_fields.first().map(|field| field.data_type()) {
            Some(typed @ DataType::Timestamp(_, _)) => typed.clone(),
            _ => fallback_struct_type_inner(),
        };
        let window = int_scalar(WINDOW_STARTS_NAME, &args.args, 1)?;
        let slide = int_scalar(WINDOW_STARTS_NAME, &args.args, 2)?;
        let offset = int_scalar(WINDOW_STARTS_NAME, &args.args, 3)?;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(time) = arrays.first() else {
            return exec_err!("'{}' requires a time column", WINDOW_STARTS_NAME);
        };
        Ok(ColumnarValue::Array(window_list_batch(
            fields,
            &time_type,
            time,
            args.number_rows,
            window,
            slide,
            offset,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Array, StructArray};
    use datafusion::arrow::datatypes::TimestampNanosecondType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::{SessionConfig, SessionContext};

    const MINUTE: i64 = 60_000_000;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    fn ctx_two_partitions() -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        crate::register_all(&ctx);
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(!batches.is_empty(), "expected rows for {sql}");
        datafusion::arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap()
    }

    async fn sql_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame.collect().await.expect_err(sql).to_string(),
        }
    }

    fn window_micros(batch: &RecordBatch) -> Vec<(Option<i64>, Option<i64>)> {
        let structs = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap_or_else(|| panic!("expected Struct, got {:?}", batch.schema()));
        let (starts, ends) = (structs.column(0), structs.column(1));
        (0..structs.len())
            .map(|row| {
                if structs.is_null(row) {
                    (None, None)
                } else {
                    (
                        Some(timestamp_child_to_micros(starts, row)),
                        Some(timestamp_child_to_micros(ends, row)),
                    )
                }
            })
            .collect()
    }

    fn timestamp_child_to_micros(array: &ArrayRef, row: usize) -> i64 {
        match array.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => {
                array
                    .as_primitive::<datafusion::arrow::datatypes::TimestampSecondType>()
                    .value(row)
                    * 1_000_000
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                array
                    .as_primitive::<datafusion::arrow::datatypes::TimestampMillisecondType>()
                    .value(row)
                    * 1_000
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => array
                .as_primitive::<datafusion::arrow::datatypes::TimestampMicrosecondType>()
                .value(row),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                array.as_primitive::<TimestampNanosecondType>().value(row) / 1_000
            }
            other => panic!("expected a timestamp child, got {other}"),
        }
    }

    fn frame_sql() -> String {
        "SELECT * FROM (VALUES (TIMESTAMP '2024-01-01 10:07:30'), (TIMESTAMP '2024-01-01 10:12:00'), \
         (TIMESTAMP '2024-01-01 10:31:00')) AS t(ts)"
            .to_string()
    }

    #[test]
    fn duration_units_parse_to_microseconds() {
        assert_eq!(parse_window_duration("10 minutes").unwrap(), 10 * MINUTE);
        assert_eq!(parse_window_duration("1 hour").unwrap(), 60 * MINUTE);
        assert_eq!(parse_window_duration("5 seconds").unwrap(), 5_000_000);
        assert_eq!(parse_window_duration("1 day").unwrap(), 1_440 * MINUTE);
        assert_eq!(parse_window_duration("2 weeks").unwrap(), 20_160 * MINUTE);
        assert_eq!(parse_window_duration("500 milliseconds").unwrap(), 500_000);
        assert_eq!(parse_window_duration("7 microseconds").unwrap(), 7);
        assert_eq!(parse_window_duration("1 HOUR").unwrap(), 60 * MINUTE);
        assert_eq!(
            parse_window_duration("1 day 2 hours").unwrap(),
            1_560 * MINUTE
        );
        assert_eq!(parse_window_duration("10minutes").unwrap(), 10 * MINUTE);
    }

    #[test]
    fn start_offsets_parse_signed() {
        assert_eq!(parse_window_offset("-2 minutes").unwrap(), -2 * MINUTE);
        assert_eq!(parse_window_offset("0 seconds").unwrap(), 0);
        assert_eq!(parse_window_offset("2 minutes").unwrap(), 2 * MINUTE);
        let error = parse_window_offset("10 parsecs").expect_err("must refuse");
        assert!(
            error.to_string().contains("[CANNOT_PARSE_INTERVAL]"),
            "expected the condition, got {error}"
        );
    }

    #[test]
    fn slide_above_window_carries_the_constraint() {
        let error = check_window_spec(5 * MINUTE, 10 * MINUTE, 0, "ts").expect_err("must refuse");
        let message = error.to_string();
        assert!(
            message.contains("[DATATYPE_MISMATCH.PARAMETER_CONSTRAINT_VIOLATION]"),
            "expected the condition, got {message}"
        );
        assert!(
            message.contains(
                "The `slide_duration`(600000000L) must be <= the `window_duration`(300000000L)"
            ),
            "expected the rendering, got {message}"
        );
    }

    #[test]
    fn abs_start_at_slide_carries_the_constraint() {
        let error =
            check_window_spec(10 * MINUTE, 5 * MINUTE, -5 * MINUTE, "ts").expect_err("must refuse");
        let message = error.to_string();
        assert!(
            message.contains(
                "The `abs(start_time)`(300000000L) must be < the `slide_duration`(300000000L)"
            ),
            "expected the rendering, got {message}"
        );
        check_window_spec(10 * MINUTE, 5 * MINUTE, -2 * MINUTE, "ts").unwrap();
        check_window_spec(10 * MINUTE, 10 * MINUTE, 0, "ts").unwrap();
    }

    #[test]
    fn bad_durations_carry_cannot_parse_interval() {
        for text in ["10 parsecs", "", "   ", "minutes", "10", "10 lightyears"] {
            let error = parse_window_duration(text).expect_err(text);
            assert!(
                error.to_string().contains("[CANNOT_PARSE_INTERVAL]"),
                "{text}: expected the condition, got {error}"
            );
        }
    }

    #[test]
    fn bucket_grids_match_spark() {
        let ten = 10 * MINUTE;
        let five = 5 * MINUTE;
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            overlapping_starts(base + 450_000_000, ten, ten, 0),
            vec![base]
        );
        assert_eq!(
            overlapping_starts(base + 450_000_000, ten, five, 0),
            vec![base, base + 300_000_000]
        );
        assert_eq!(
            overlapping_starts(base + 450_000_000, ten, ten, 2 * MINUTE),
            vec![base + 120_000_000]
        );
        assert!(overlapping_starts(base + 450_000_000, 5 * MINUTE, ten, 0).is_empty());
    }

    #[tokio::test]
    async fn tumbling_buckets_match_spark() {
        let ctx = ctx();
        let sql = format!(
            "SELECT window(ts, '10 minutes') AS w FROM ({}) ORDER BY ts",
            frame_sql()
        );
        let values = window_micros(&batch(&ctx, &sql).await);
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            values,
            vec![
                (Some(base), Some(base + 600_000_000)),
                (Some(base + 600_000_000), Some(base + 1_200_000_000)),
                (Some(base + 1_800_000_000), Some(base + 2_400_000_000)),
            ]
        );
    }

    #[tokio::test]
    async fn starttime_offsets_the_grid() {
        let ctx = ctx();
        let sql = format!(
            "SELECT window(ts, '10 minutes', '10 minutes', '2 minutes') AS w FROM ({}) ORDER BY ts",
            frame_sql()
        );
        let values = window_micros(&batch(&ctx, &sql).await);
        let base = 1_704_103_200_000_000_i64;
        assert_eq!(
            values
                .iter()
                .map(|(start, _)| start.unwrap())
                .collect::<Vec<_>>(),
            vec![base + 120_000_000, base + 720_000_000, base + 1_320_000_000]
        );
    }

    #[tokio::test]
    async fn null_time_is_null_window() {
        let ctx = ctx();
        let sql = "SELECT window(CAST(NULL AS TIMESTAMP), '10 minutes') AS w";
        let values = window_micros(&batch(&ctx, sql).await);
        assert_eq!(values, vec![(None, None)]);
    }

    #[tokio::test]
    async fn bad_duration_carries_the_condition() {
        let ctx = ctx();
        let message = sql_error(&ctx, "SELECT window(ts, '10 parsecs') AS w FROM (SELECT TIMESTAMP '2024-01-01 10:00:00' AS ts)").await;
        assert!(
            message.contains("[CANNOT_PARSE_INTERVAL]"),
            "expected the condition, got {message}"
        );
    }

    #[tokio::test]
    async fn window_output_names_start_and_end() {
        let ctx = ctx();
        let sql = "SELECT window(TIMESTAMP '2024-01-01 10:00:00', '10 minutes') AS w";
        let produced = batch(&ctx, sql).await;
        let structs = produced
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let names: Vec<&str> = structs
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, vec!["start", "end"]);
    }

    #[tokio::test]
    async fn tumbling_groupby_is_correct_on_two_partitions() {
        let ctx = ctx_two_partitions();
        let sql = format!(
            "SELECT window(ts, '10 minutes') AS w, count(*) AS c FROM ({}) GROUP BY w",
            frame_sql()
        );
        let produced = batch(&ctx, &sql).await;
        assert_eq!(produced.num_rows(), 3);
        let counts: Vec<i64> = produced
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(counts, vec![1, 1, 1]);
    }
}
