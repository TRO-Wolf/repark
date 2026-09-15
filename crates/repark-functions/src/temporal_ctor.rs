use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Date32Type, TimeUnit};
use chrono::{Datelike, FixedOffset, NaiveDate, NaiveDateTime, Timelike};
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF};
use datafusion::prelude::SessionContext;

use crate::datetime::{
    datetime_from_micros, local_datetime_from_micros, micros_from_local_datetime,
};
use crate::session_time_zone::session_time_zone_from_options;

pub mod adddiff;
pub mod arith;
pub mod date_alias;
pub mod intervals;
pub mod make;

pub(crate) use intervals::{precast_secs, secs_to_micros};

#[must_use]
pub fn make_timestamp_udf(try_mode: bool) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::zoned(
        try_mode,
    )))
}

#[must_use]
pub fn make_timestamp_ltz_udf(try_mode: bool) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::ltz(try_mode)))
}

#[must_use]
pub fn make_timestamp_ntz_udf(try_mode: bool) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::ntz(try_mode)))
}

pub(crate) const TIMESTAMP_TZ: &str = "UTC";
pub(crate) const MICROS_PER_SECOND: i64 = 1_000_000;
pub(crate) const NANOS_PER_MICRO: i64 = 1_000;

pub fn register(ctx: &SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::zoned(false))),
        Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::zoned(true))),
        Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::ltz(false))),
        Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::ltz(true))),
        Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::ntz(false))),
        Arc::new(ScalarUDF::new_from_impl(make::MakeTimestamp::ntz(true))),
    ]
    .into_iter()
    .chain(adddiff::functions())
    .chain(date_alias::functions())
    .chain(arith::functions())
    .chain(intervals::functions())
    .collect()
}

#[must_use]
pub fn interval_string_cast_rule() -> Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync> {
    intervals::interval_string_cast_rule()
}

pub(crate) fn exec_error(text: String) -> DataFusionError {
    DataFusionError::Execution(text)
}

pub(crate) fn plan_error(text: String) -> DataFusionError {
    DataFusionError::Plan(text)
}

fn invalid_timezone(zone: &str) -> DataFusionError {
    exec_error(format!(
        "[INVALID_TIMEZONE] The timezone: {zone} is invalid. The timezone must be either a \
         region-based zone ID or a zone offset. Region IDs must have the form 'area/city', such \
         as 'America/Los_Angeles'. SQLSTATE: 22023"
    ))
}

fn month_out_of_bounds(month: i64) -> DataFusionError {
    exec_error(format!(
        "[DATETIME_FIELD_OUT_OF_BOUNDS.WITH_SUGGESTION] Invalid value for MonthOfYear (valid \
         values 1 - 12): {month}. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to \
         bypass this error. SQLSTATE: 22023"
    ))
}

fn hour_out_of_bounds(hour: i64) -> DataFusionError {
    exec_error(format!(
        "[DATETIME_FIELD_OUT_OF_BOUNDS.WITH_SUGGESTION] Invalid value for HourOfDay (valid \
         values 0 - 23): {hour}. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to \
         bypass this error. SQLSTATE: 22023"
    ))
}

fn minute_out_of_bounds(minute: i64) -> DataFusionError {
    exec_error(format!(
        "[DATETIME_FIELD_OUT_OF_BOUNDS.WITH_SUGGESTION] Invalid value for MinuteOfHour (valid \
         values 0 - 59): {minute}. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to \
         bypass this error. SQLSTATE: 22023"
    ))
}

pub(crate) fn second_out_of_bounds(second: i64) -> DataFusionError {
    exec_error(format!(
        "[DATETIME_FIELD_OUT_OF_BOUNDS.WITH_SUGGESTION] Invalid value for SecondOfMinute (valid \
         values 0 - 59): {second}. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to \
         bypass this error. SQLSTATE: 22023"
    ))
}

fn invalid_fraction_of_second(total: i64, frac: i64) -> DataFusionError {
    let frac_text = format!("{frac:06}").trim_end_matches('0').to_string();
    exec_error(format!(
        "[INVALID_FRACTION_OF_SECOND] Valid range for seconds is [0, 60] (inclusive), but the \
         provided value is {total}.{frac_text}. To avoid this error, use `try_make_timestamp`, \
         which returns NULL on error."
    ))
}

pub(crate) fn invalid_date_text(month: u32, day: u32) -> DataFusionError {
    let name = match month {
        1 => "JANUARY",
        2 => "FEBRUARY",
        3 => "MARCH",
        4 => "APRIL",
        5 => "MAY",
        6 => "JUNE",
        7 => "JULY",
        8 => "AUGUST",
        9 => "SEPTEMBER",
        10 => "OCTOBER",
        11 => "NOVEMBER",
        _ => "DECEMBER",
    };
    exec_error(format!(
        "[DATETIME_FIELD_OUT_OF_BOUNDS.WITH_SUGGESTION] Invalid date '{name} {day}'. If \
         necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error. SQLSTATE: \
         22023"
    ))
}

pub(crate) fn datetime_overflow() -> DataFusionError {
    exec_error("[DATETIME_OVERFLOW] Datetime operation overflow.".to_string())
}

pub(crate) fn days_in_month(year: i32, month: u32) -> Option<u32> {
    let (next_year, next_month) = if month == 12 {
        (year.checked_add(1)?, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)?
        .pred_opt()
        .map(|date| date.day())
}

pub(crate) fn is_last_day(year: i32, month: u32, day: u32) -> bool {
    days_in_month(year, month).is_some_and(|last| last == day)
}

pub(crate) fn add_months_ymd(
    year: i32,
    month: u32,
    day: u32,
    delta: i64,
) -> Option<(i32, u32, u32)> {
    let total = i64::from(year)
        .checked_mul(12)?
        .checked_add(i64::from(month) - 1)?
        .checked_add(delta)?;
    let new_year = i32::try_from(total.div_euclid(12)).ok()?;
    let new_month = u32::try_from(total.rem_euclid(12) + 1).ok()?;
    let last = days_in_month(new_year, new_month)?;
    Some((new_year, new_month, day.min(last)))
}

pub(crate) fn resolve_session_zone(options: &ConfigOptions) -> Result<Tz> {
    let zone = session_time_zone_from_options(options);
    Tz::from_str(zone).map_err(|error| {
        exec_error(format!(
            "session timezone {zone:?} could not be resolved at query time ({error})"
        ))
    })
}

pub(crate) fn parse_zone(text: &str) -> Result<ZoneOffset> {
    if text == "Z" || text.starts_with(&['+', '-'][..]) || has_offset_prefix(text) {
        return parse_fixed_offset(text)
            .map(ZoneOffset::Fixed)
            .ok_or_else(|| invalid_timezone(text));
    }
    Tz::from_str(text)
        .map(ZoneOffset::Named)
        .map_err(|_| invalid_timezone(text))
}

fn has_offset_prefix(text: &str) -> bool {
    text.starts_with("GMT") || text.starts_with("UTC") || text.starts_with("UT")
}

fn parse_fixed_offset(text: &str) -> Option<FixedOffset> {
    if text == "Z" {
        return FixedOffset::east_opt(0);
    }
    let body = text
        .strip_prefix("GMT")
        .or_else(|| text.strip_prefix("UTC"))
        .or_else(|| text.strip_prefix("UT"))
        .unwrap_or(text);
    if body.is_empty() {
        return FixedOffset::east_opt(0);
    }
    let (sign, digits) = match body.strip_prefix('+') {
        Some(rest) => (1, rest),
        None => (-1, body.strip_prefix('-')?),
    };
    let fields: Vec<&str> = digits.split(':').collect();
    let (hours_text, mins_text, secs_text) = match fields.as_slice() {
        [hours] if hours.len() == 4 => (&hours[..2], &hours[2..], "00"),
        [hours] if hours.len() == 6 => (&hours[..2], &hours[2..4], &hours[4..]),
        [hours] if hours.len() <= 2 => (*hours, "00", "00"),
        [hours, mins] => (*hours, *mins, "00"),
        [hours, mins, secs] => (*hours, *mins, *secs),
        _ => return None,
    };
    let hours: i32 = hours_text.parse().ok()?;
    let mins: i32 = mins_text.parse().ok()?;
    let secs: i32 = secs_text.parse().ok()?;
    if hours < 0 || !(0..=59).contains(&mins) || !(0..=59).contains(&secs) {
        return None;
    }
    let total = hours
        .checked_mul(3_600)?
        .checked_add(mins.checked_mul(60)?)?
        .checked_add(secs)?;
    if total > 18 * 3_600 {
        return None;
    }
    FixedOffset::east_opt(sign * total)
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ZoneOffset {
    Named(Tz),
    Fixed(FixedOffset),
}

pub(crate) fn wall_to_instant_micros(wall: NaiveDateTime, zone: ZoneOffset) -> Option<i64> {
    match zone {
        ZoneOffset::Named(named) => micros_from_local_datetime(wall, named, None),
        ZoneOffset::Fixed(offset) => wall
            .and_local_timezone(offset)
            .single()
            .map(|zoned| zoned.timestamp_micros()),
    }
}

pub(crate) fn instant_to_wall_micros(
    instant_micros: i64,
    zone: ZoneOffset,
) -> Option<NaiveDateTime> {
    match zone {
        ZoneOffset::Named(named) => local_datetime_from_micros(instant_micros, named),
        ZoneOffset::Fixed(offset) => {
            let shift = chrono::TimeDelta::try_seconds(i64::from(offset.local_minus_utc()))?;
            datetime_from_micros(instant_micros).and_then(|naive| naive.checked_add_signed(shift))
        }
    }
}

pub(crate) fn zone_argument(array: &dyn Array, row: usize) -> Result<Option<ZoneOffset>> {
    if array.is_null(row) {
        return Ok(None);
    }
    if matches!(array.data_type(), DataType::Utf8) {
        let values = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| exec_error("string cast did not yield Utf8".to_string()))?;
        return parse_zone(values.value(row)).map(Some);
    }
    let casted = cast(array, &DataType::Utf8)
        .map_err(|_| plan_error("make_timestamp timezone expects a STRING".to_string()))?;
    let values = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| exec_error("string cast did not yield Utf8".to_string()))?;
    parse_zone(values.value(row)).map(Some)
}

pub(crate) fn precast_ts(array: &ArrayRef) -> Result<ArrayRef> {
    let DataType::Timestamp(unit, tz) = array.data_type() else {
        return Ok(Arc::clone(array));
    };
    if *unit == TimeUnit::Microsecond {
        return Ok(Arc::clone(array));
    }
    precast_column(
        array,
        &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        DataFusionError::Execution("timestamp cast failed".to_string()),
    )
}

pub(crate) enum ZoneSource {
    Rows,
    Fixed(Option<ZoneOffset>),
}

pub(crate) fn hoisted_zone(arg: &ColumnarValue) -> Result<ZoneSource> {
    match scalar_text(arg) {
        ScalarText::Rows => Ok(ZoneSource::Rows),
        ScalarText::Null => Ok(ZoneSource::Fixed(None)),
        ScalarText::Text(text) => parse_zone(&text).map(|zone| ZoneSource::Fixed(Some(zone))),
        ScalarText::Unexpected(_) => Err(plan_error(
            "make_timestamp timezone expects a STRING".to_string(),
        )),
    }
}

pub(crate) fn int_field(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    if array.is_null(row) {
        return Ok(None);
    }
    if matches!(array.data_type(), DataType::Int64) {
        let values = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| exec_error("integral cast did not yield Int64".to_string()))?;
        return Ok(Some(values.value(row)));
    }
    let casted = cast(array, &DataType::Int64).map_err(|_| integral_error(array.data_type()))?;
    let values = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| exec_error("integral cast did not yield Int64".to_string()))?;
    Ok(Some(values.value(row)))
}

fn integral_error(other: &DataType) -> DataFusionError {
    plan_error(format!(
        "temporal constructor expects an integral argument, got {other}"
    ))
}

pub(crate) fn precast_column(
    array: &ArrayRef,
    target: &DataType,
    error: DataFusionError,
) -> Result<ArrayRef> {
    if array.data_type() == target
        || matches!(array.data_type(), DataType::Null)
        || array.null_count() == array.len()
    {
        return Ok(Arc::clone(array));
    }
    cast(array, target).map_err(|_| error)
}

pub(crate) fn precast_int(array: &ArrayRef) -> Result<ArrayRef> {
    precast_column(array, &DataType::Int64, integral_error(array.data_type()))
}

pub(crate) fn precast_str(array: &ArrayRef) -> Result<ArrayRef> {
    precast_column(
        array,
        &DataType::Utf8,
        exec_error("string cast did not yield Utf8".to_string()),
    )
}

pub(crate) enum ScalarText {
    Rows,
    Null,
    Text(String),
    Unexpected(DataType),
}

pub(crate) fn scalar_text(arg: &ColumnarValue) -> ScalarText {
    let ColumnarValue::Scalar(scalar) = arg else {
        return ScalarText::Rows;
    };
    match scalar {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            match value {
                Some(text) => ScalarText::Text(text.clone()),
                None => ScalarText::Null,
            }
        }
        ScalarValue::Null => ScalarText::Null,
        _ => ScalarText::Unexpected(scalar.data_type()),
    }
}

const DATE_FORMATS: [&str; 5] = [
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
];

pub(crate) fn parse_wall_text(text: &str) -> Option<NaiveDateTime> {
    for format in DATE_FORMATS {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, format) {
            return Some(naive);
        }
    }
    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
}

fn date_to_ymd(array: &dyn Array, row: usize) -> Result<Option<(i32, u32, u32)>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Date32 => {
            let values = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| exec_error("date column is not Date32".to_string()))?;
            Ok(Date32Type::to_naive_date_opt(values.value(row))
                .map(|date| (date.year(), date.month(), date.day())))
        }
        DataType::Date64 => {
            let casted = cast(array, &DataType::Date32)
                .map_err(|_| exec_error("date64 cast did not yield Date32".to_string()))?;
            date_to_ymd(casted.as_ref(), row)
        }
        DataType::Timestamp(_, _) => {
            let casted = cast(array, &DataType::Timestamp(TimeUnit::Microsecond, None))
                .map_err(|_| exec_error("timestamp cast failed".to_string()))?;
            let values = casted
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| exec_error("timestamp cast failed".to_string()))?;
            Ok(datetime_from_micros(values.value(row))
                .map(|naive| (naive.year(), naive.month(), naive.day())))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let casted = cast(array, &DataType::Utf8)
                .map_err(|_| exec_error("string cast did not yield Utf8".to_string()))?;
            let values = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| exec_error("string cast did not yield Utf8".to_string()))?;
            let text = values.value(row);
            parse_wall_text(text)
                .map(|naive| Some((naive.year(), naive.month(), naive.day())))
                .ok_or_else(|| malformed_timestamp_cast(text))
        }
        DataType::Null => Ok(None),
        other => Err(plan_error(format!(
            "temporal constructor expects a DATE argument, got {other}"
        ))),
    }
}

pub(crate) struct WallFields {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub day_micros: i64,
}

pub(crate) fn wall_fields(array: &dyn Array, row: usize, zone: Tz) -> Result<Option<WallFields>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Date32 => Ok(
            date_to_ymd(array, row)?.map(|(year, month, day)| WallFields {
                year,
                month,
                day,
                day_micros: 0,
            }),
        ),
        DataType::Date64 => Ok(
            date_to_ymd(array, row)?.map(|(year, month, day)| WallFields {
                year,
                month,
                day,
                day_micros: 0,
            }),
        ),
        DataType::Timestamp(_, Some(_)) => {
            let casted = cast(array, &DataType::Timestamp(TimeUnit::Microsecond, None))
                .map_err(|_| exec_error("timestamp cast failed".to_string()))?;
            let values = casted
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| exec_error("timestamp cast failed".to_string()))?;
            Ok(
                local_datetime_from_micros(values.value(row), zone).map(|local| WallFields {
                    year: local.year(),
                    month: local.month(),
                    day: local.day(),
                    day_micros: day_micros_of(local),
                }),
            )
        }
        DataType::Timestamp(_, None) => {
            let casted = cast(array, &DataType::Timestamp(TimeUnit::Microsecond, None))
                .map_err(|_| exec_error("timestamp cast failed".to_string()))?;
            let values = casted
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| exec_error("timestamp cast failed".to_string()))?;
            Ok(
                datetime_from_micros(values.value(row)).map(|naive| WallFields {
                    year: naive.year(),
                    month: naive.month(),
                    day: naive.day(),
                    day_micros: day_micros_of(naive),
                }),
            )
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let casted = cast(array, &DataType::Utf8)
                .map_err(|_| exec_error("string cast did not yield Utf8".to_string()))?;
            let values = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| exec_error("string cast did not yield Utf8".to_string()))?;
            let text = values.value(row);
            parse_wall_text(text)
                .map(|naive| {
                    Some(WallFields {
                        year: naive.year(),
                        month: naive.month(),
                        day: naive.day(),
                        day_micros: day_micros_of(naive),
                    })
                })
                .ok_or_else(|| malformed_timestamp_cast(text))
        }
        DataType::Null => Ok(None),
        other => Err(plan_error(format!(
            "temporal constructor expects a date-like argument, got {other}"
        ))),
    }
}

pub(crate) fn day_micros_of(naive: NaiveDateTime) -> i64 {
    i64::from(naive.time().hour()) * 3_600_000_000
        + i64::from(naive.time().minute()) * 60_000_000
        + i64::from(naive.time().second()) * 1_000_000
        + i64::from(naive.time().nanosecond() / 1_000)
}

pub(crate) fn nullable_wall(
    array: &dyn Array,
    row: usize,
    zone: arrow::array::timezone::Tz,
    ansi: bool,
) -> Result<Option<WallFields>> {
    if array.is_null(row) {
        return Ok(None);
    }
    match wall_fields(array, row, zone) {
        Ok(wall) => Ok(wall),
        Err(error) => {
            if ansi {
                Err(error)
            } else {
                Ok(None)
            }
        }
    }
}

pub(crate) fn malformed_timestamp_cast(value: &str) -> DataFusionError {
    exec_error(format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \
         \"TIMESTAMP\" because it is malformed. Correct the value as per the syntax, or change \
         its target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018"
    ))
}

pub(crate) fn broadcast_arrays(arrays: &[ArrayRef]) -> Vec<ArrayRef> {
    let width = arrays.iter().map(Array::len).max().unwrap_or(0);
    arrays
        .iter()
        .map(|array| {
            if array.len() == width || width == 0 {
                Arc::clone(array)
            } else if array.len() == 1 {
                arrow::compute::concat(
                    &std::iter::repeat_n(array.as_ref(), width).collect::<Vec<&dyn Array>>(),
                )
                .unwrap_or_else(|_| Arc::clone(array))
            } else {
                Arc::clone(array)
            }
        })
        .collect()
}
