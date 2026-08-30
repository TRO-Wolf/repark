//! Embedded Spark timestamp casts for epoch seconds, strings, and dates.

use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};
use datafusion::arrow::array::{Array, AsArray, Float64Array, Int64Array, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::datetime::invoke_local_dates;
use crate::session_time_zone::session_time_zone_from_options;

/// The embedded integer-target UDF: floored epoch seconds as `Int64`.
#[must_use]
pub fn spark_epoch_seconds_floor_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkEpochSecondsFloor::new()))
}

/// The embedded real-target UDF: fractional epoch seconds as `Float64`.
#[must_use]
pub fn spark_epoch_seconds_real_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkEpochSecondsReal::new()))
}

/// The embedded string-target UDF: Spark's session-zone LTZ or stored-wall NTZ `Utf8` cast.
#[must_use]
pub fn spark_timestamp_to_string_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTimestampToString::new()))
}

/// The embedded DATE-target UDF: session-zone LTZ or stored-wall NTZ `Date32` cast.
#[must_use]
pub fn spark_timestamp_to_date_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkTimestampToDate::new()))
}

/// Registered `to_date` overwrite using the timestamp cast kernel for TIMESTAMP arguments.
#[must_use]
pub fn to_date_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToDate::new()))
}

/// Return ticks per second for an Arrow [`TimeUnit`].
const fn ticks_per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Return Spark's floor-divided epoch seconds; positive divisors preserve `-0.5 s` as `-1`.
const fn seconds_floor_from_ticks(ticks: i64, per_second: i64) -> i64 {
    ticks.div_euclid(per_second)
}

/// The timestamp `TimeUnit` of the single argument, or a planning error naming what arrived.
fn argument_time_unit(name: &str, data_type: &DataType) -> Result<TimeUnit> {
    match data_type {
        DataType::Timestamp(unit, _) => Ok(*unit),
        other => Err(DataFusionError::Plan(format!(
            "'{name}' expects a TIMESTAMP argument, got {other}"
        ))),
    }
}

/// Return raw timestamp ticks as `Int64` while preserving nulls.
fn timestamp_ticks(array: &dyn Array) -> Result<Int64Array> {
    let ticks = cast(array, &DataType::Int64)?;
    Ok(ticks.as_primitive::<Int64Type>().clone())
}

/// `SparkEpochSecondsFloor` — the integer path: floored epoch seconds as `Int64`.
#[derive(Debug)]
struct SparkEpochSecondsFloor {
    signature: Signature,
}

impl SparkEpochSecondsFloor {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkEpochSecondsFloor {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkEpochSecondsFloor {}

impl Hash for SparkEpochSecondsFloor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkEpochSecondsFloor {
    crate::shim_udf_boilerplate!("__repark_epoch_seconds_floor__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        argument_time_unit(self.name(), &arg_types[0])?;
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int64,
            nullable_like_argument(&args),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let unit = argument_time_unit(self.name(), arrays[0].data_type())?;
        let per_second = ticks_per_second(unit);
        let seconds: Int64Array = timestamp_ticks(arrays[0].as_ref())?
            .iter()
            .map(|ticks| ticks.map(|value| seconds_floor_from_ticks(value, per_second)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(seconds)))
    }
}

/// `SparkEpochSecondsReal` — the float / decimal path: epoch seconds with the fraction kept.
#[derive(Debug)]
struct SparkEpochSecondsReal {
    signature: Signature,
}

impl SparkEpochSecondsReal {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkEpochSecondsReal {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkEpochSecondsReal {}

impl Hash for SparkEpochSecondsReal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkEpochSecondsReal {
    crate::shim_udf_boilerplate!("__repark_epoch_seconds_real__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        argument_time_unit(self.name(), &arg_types[0])?;
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Float64,
            nullable_like_argument(&args),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let unit = argument_time_unit(self.name(), arrays[0].data_type())?;
        #[expect(
            clippy::cast_precision_loss,
            reason = "Spark computes its own TIMESTAMP -> DOUBLE/DECIMAL cast through a double \
                      (`t / MICROS_PER_SECOND.toDouble`), so the f64 hop IS the oracle's \
                      mechanism; the integer path that needs exactness uses \
                      `SparkEpochSecondsFloor` instead"
        )]
        let per_second = ticks_per_second(unit) as f64;
        #[expect(
            clippy::cast_precision_loss,
            reason = "same double hop as the divisor above — see `SparkEpochSecondsFloor` for \
                      the exact-integer path"
        )]
        let seconds: Float64Array = timestamp_ticks(arrays[0].as_ref())?
            .iter()
            .map(|ticks| ticks.map(|value| value as f64 / per_second))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(seconds)))
    }
}

/// `SparkTimestampToString` — B-TZ-4: Spark `CAST(TIMESTAMP AS STRING)` as `Utf8`.
#[derive(Debug)]
struct SparkTimestampToString {
    signature: Signature,
}

impl SparkTimestampToString {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkTimestampToString {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkTimestampToString {}

impl Hash for SparkTimestampToString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkTimestampToString {
    crate::shim_udf_boilerplate!("__repark_timestamp_to_string__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        argument_time_unit(self.name(), &arg_types[0])?;
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Utf8,
            nullable_like_argument(&args),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let data_type = arrays[0].data_type().clone();
        let (unit, zone_annotation) = argument_timestamp_parts(self.name(), &data_type)?;
        let session_zone =
            parse_session_zone(session_time_zone_from_options(args.config_options.as_ref()))?;
        let ticks = timestamp_ticks(arrays[0].as_ref())?;
        let mut builder = StringBuilder::with_capacity(ticks.len(), ticks.len() * 32);
        for row in 0..ticks.len() {
            if ticks.is_null(row) {
                builder.append_null();
                continue;
            }
            match wall_clock_from_ticks(
                ticks.value(row),
                unit,
                zone_annotation.as_ref(),
                session_zone,
            ) {
                Some(wall) => builder.append_value(format_spark_timestamp_string(wall)),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// `SparkTimestampToDate` — TZ-8: Spark `CAST(TIMESTAMP AS DATE)` as `Date32`.
#[derive(Debug)]
struct SparkTimestampToDate {
    signature: Signature,
}

impl SparkTimestampToDate {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkTimestampToDate {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkTimestampToDate {}

impl Hash for SparkTimestampToDate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkTimestampToDate {
    crate::shim_udf_boilerplate!("__repark_timestamp_to_date__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        argument_time_unit(self.name(), &arg_types[0])?;
        Ok(DataType::Date32)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Date32,
            nullable_like_argument(&args),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        argument_time_unit(self.name(), arrays[0].data_type())?;
        let dates = invoke_local_dates(&arrays[0], args.config_options.as_ref())?;
        Ok(ColumnarValue::Array(dates))
    }
}

/// `SparkToDate` — registered `to_date` overwrite (TZ-8 TIMESTAMP arm + date/string pass-through).
#[derive(Debug)]
struct SparkToDate {
    signature: Signature,
}

impl SparkToDate {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkToDate {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkToDate {}

impl Hash for SparkToDate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkToDate {
    crate::shim_udf_boilerplate!("to_date");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        // Spark `to_date` is always nullable (even over a non-null string literal).
        Ok(Arc::new(Field::new(self.name(), DataType::Date32, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let dates = invoke_local_dates(&arrays[0], args.config_options.as_ref())?;
        Ok(ColumnarValue::Array(dates))
    }
}

/// The timestamp unit and zone annotation of the single argument.
fn argument_timestamp_parts(
    name: &str,
    data_type: &DataType,
) -> Result<(TimeUnit, Option<Arc<str>>)> {
    match data_type {
        DataType::Timestamp(unit, zone) => Ok((*unit, zone.clone())),
        other => Err(DataFusionError::Plan(format!(
            "'{name}' expects a TIMESTAMP argument, got {other}"
        ))),
    }
}

fn parse_session_zone(zone_id: &str) -> Result<Tz> {
    Tz::from_str(zone_id).map_err(|error| {
        DataFusionError::Execution(format!(
            "session timezone {zone_id:?} could not be resolved at query time ({error})"
        ))
    })
}

/// Ticks → microseconds.
fn ticks_to_micros(ticks: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => ticks.checked_mul(1_000_000),
        TimeUnit::Millisecond => ticks.checked_mul(1_000),
        TimeUnit::Microsecond => Some(ticks),
        TimeUnit::Nanosecond => Some(ticks.div_euclid(1_000)),
    }
}

/// LTZ (`Some` annotation) → session-zone wall.
fn wall_clock_from_ticks(
    ticks: i64,
    unit: TimeUnit,
    zone_annotation: Option<&Arc<str>>,
    session_zone: Tz,
) -> Option<NaiveDateTime> {
    let micros = ticks_to_micros(ticks, unit)?;
    let utc = DateTime::from_timestamp_micros(micros)?;
    if zone_annotation.is_some() {
        Some(utc.with_timezone(&session_zone).naive_local())
    } else {
        Some(utc.naive_utc())
    }
}

/// Spark `CAST(ts AS STRING)` is `yyyy-MM-dd HH:mm:ss` plus a fraction without trailing zeros.
fn format_spark_timestamp_string(wall: NaiveDateTime) -> String {
    let date = format_iso_local_date(wall.date());
    let time = format!(
        "{:02}:{:02}:{:02}",
        wall.hour(),
        wall.minute(),
        wall.second()
    );
    let nanos = wall.nanosecond();
    if nanos == 0 {
        format!("{date} {time}")
    } else {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{date} {time}.{fraction}")
    }
}

/// `DateTimeFormatter.ISO_LOCAL_DATE` as Spark 4.1.2 emitted it under the record probe.
fn format_iso_local_date(date: NaiveDate) -> String {
    let year = date.year();
    let month = date.month();
    let day = date.day();
    if (0..=9999).contains(&year) {
        format!("{year:04}-{month:02}-{day:02}")
    } else if year > 9999 {
        format!("+{year}-{month:02}-{day:02}")
    } else {
        format!("-{abs:04}-{month:02}-{day:02}", abs = year.unsigned_abs())
    }
}

/// True when the sole argument is nullable (or absent, which the signature makes unreachable).
fn nullable_like_argument(args: &ReturnFieldArgs<'_>) -> bool {
    args.arg_fields
        .first()
        .is_none_or(|field| field.is_nullable())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spark floors negative fractional epoch seconds: `-0.5 s → -1` and `-1.25 s → -2`.
    #[test]
    fn epoch_seconds_floor_is_floor_not_truncation() {
        const NANOS: i64 = 1_000_000_000;
        assert_eq!(seconds_floor_from_ticks(-500_000_000, NANOS), -1);
        assert_eq!(seconds_floor_from_ticks(-1_250_000_000, NANOS), -2);
        assert_eq!(seconds_floor_from_ticks(-1_800_000_000_000, NANOS), -1800);
        assert_eq!(seconds_floor_from_ticks(750_000_000, NANOS), 0);
        assert_eq!(seconds_floor_from_ticks(1_999_999_000, NANOS), 1);
        assert_eq!(seconds_floor_from_ticks(0, NANOS), 0);
        assert_eq!(seconds_floor_from_ticks(-1_000_000_000, NANOS), -1);
    }

    /// Scale each Arrow timestamp unit with its own divisor.
    #[test]
    fn every_time_unit_scales_by_its_own_divisor() {
        assert_eq!(ticks_per_second(TimeUnit::Second), 1);
        assert_eq!(ticks_per_second(TimeUnit::Millisecond), 1_000);
        assert_eq!(ticks_per_second(TimeUnit::Microsecond), 1_000_000);
        assert_eq!(ticks_per_second(TimeUnit::Nanosecond), 1_000_000_000);
        for (unit, ticks) in [
            (TimeUnit::Second, -1_800_i64),
            (TimeUnit::Millisecond, -1_800_000),
            (TimeUnit::Microsecond, -1_800_000_000),
            (TimeUnit::Nanosecond, -1_800_000_000_000),
        ] {
            assert_eq!(
                seconds_floor_from_ticks(ticks, ticks_per_second(unit)),
                -1800,
                "{unit:?}"
            );
        }
        for (unit, ticks) in [
            (TimeUnit::Millisecond, -500_i64),
            (TimeUnit::Microsecond, -500_000),
            (TimeUnit::Nanosecond, -500_000_000),
        ] {
            assert_eq!(
                seconds_floor_from_ticks(ticks, ticks_per_second(unit)),
                -1,
                "{unit:?}"
            );
        }
    }

    /// Extreme nanosecond ticks remain floored without panicking.
    #[test]
    fn extreme_tick_values_floor_without_panicking() {
        const NANOS: i64 = 1_000_000_000;
        assert_eq!(
            seconds_floor_from_ticks(i64::MAX, NANOS),
            i64::MAX / NANOS,
            "the positive extreme truncates and floors alike"
        );
        assert_eq!(
            seconds_floor_from_ticks(i64::MIN, NANOS),
            i64::MIN / NANOS - 1
        );
        assert_eq!(seconds_floor_from_ticks(i64::MIN, 1), i64::MIN);
    }

    /// Reject non-timestamp arguments during planning.
    #[test]
    fn a_non_timestamp_argument_is_a_planning_error() {
        let error = argument_time_unit("__repark_epoch_seconds_floor__", &DataType::Int64)
            .expect_err("Int64 is not a timestamp");
        assert!(
            error.to_string().contains("expects a TIMESTAMP argument"),
            "got {error}"
        );
        assert!(
            argument_time_unit("x", &DataType::Timestamp(TimeUnit::Nanosecond, None)).is_ok(),
            "a timestamp resolves"
        );
    }

    fn wall(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        nanos: u32,
    ) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(year, month, day)
            .and_then(|date| date.and_hms_nano_opt(hour, minute, second, nanos))
            .expect("valid wall clock for a format pin")
    }

    /// Spark `CAST(ts AS STRING)` trims trailing fractional zeros.
    #[test]
    fn spark_timestamp_string_trims_trailing_fraction_zeros() {
        assert_eq!(
            format_spark_timestamp_string(wall(2024, 6, 15, 8, 0, 0, 0)),
            "2024-06-15 08:00:00"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(2024, 6, 15, 8, 0, 0, 500_000_000)),
            "2024-06-15 08:00:00.5"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(2024, 6, 15, 8, 0, 0, 123_400_000)),
            "2024-06-15 08:00:00.1234"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(2024, 6, 15, 8, 0, 0, 123_456_000)),
            "2024-06-15 08:00:00.123456"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(2024, 6, 15, 8, 0, 0, 100_000_000)),
            "2024-06-15 08:00:00.1"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(2024, 6, 15, 8, 0, 0, 1_000)),
            "2024-06-15 08:00:00.000001"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(2024, 6, 15, 8, 0, 0, 123_000_000)),
            "2024-06-15 08:00:00.123"
        );
    }

    /// Recorded year-shape: 0001 / 0000 / −0001 / +10000.
    #[test]
    fn spark_timestamp_string_year_shape_matches_iso_local_date() {
        assert_eq!(
            format_spark_timestamp_string(wall(1, 1, 1, 0, 0, 0, 0)),
            "0001-01-01 00:00:00"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(0, 1, 1, 0, 0, 0, 0)),
            "0000-01-01 00:00:00"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(-1, 1, 1, 0, 0, 0, 0)),
            "-0001-01-01 00:00:00"
        );
        assert_eq!(
            format_spark_timestamp_string(wall(10_000, 1, 1, 0, 0, 0, 0)),
            "+10000-01-01 00:00:00"
        );
    }

    /// LTZ reads the session zone; NTZ keeps the stored wall.
    #[test]
    fn ltz_renders_in_the_session_zone_and_ntz_does_not() {
        let new_york = Tz::from_str("America/New_York").expect("IANA zone");
        let modern_utc_micros = 1_718_452_800_000_000_i64; // 2024-06-15T12:00:00Z
        let utc_annotation = Some(Arc::<str>::from("UTC"));
        let ltz = wall_clock_from_ticks(
            modern_utc_micros,
            TimeUnit::Microsecond,
            utc_annotation.as_ref(),
            new_york,
        )
        .expect("in chrono range");
        let ntz = wall_clock_from_ticks(modern_utc_micros, TimeUnit::Microsecond, None, new_york)
            .expect("in chrono range");
        assert_eq!(format_spark_timestamp_string(ltz), "2024-06-15 08:00:00");
        assert_eq!(format_spark_timestamp_string(ntz), "2024-06-15 12:00:00");
    }

    /// A leftover nanosecond column floors to Spark's microsecond resolution before formatting.
    #[test]
    fn nanosecond_ticks_floor_to_microseconds() {
        assert_eq!(
            ticks_to_micros(123_456_789, TimeUnit::Nanosecond),
            Some(123_456)
        );
        assert_eq!(ticks_to_micros(-1_500, TimeUnit::Nanosecond), Some(-2));
    }

    /// TZ-8: LTZ `2024-06-15T03:00:00Z` is 2024-06-14 in New York; NTZ ticks stay 2024-06-15.
    #[test]
    fn ltz_date_is_session_zone_and_ntz_is_stored_wall() {
        use datafusion::arrow::array::{ArrayRef, Date32Array, TimestampMicrosecondArray};
        use datafusion::arrow::datatypes::Date32Type;
        use datafusion::prelude::SessionConfig;

        use crate::session_time_zone::with_session_time_zone;

        // 2024-06-15T03:00:00Z — 23:00 EDT on the 14th.
        let micros = 1_718_420_400_000_000_i64;
        let ltz: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(micros)]).with_timezone("UTC"));
        let ntz: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(micros)]));
        let config = with_session_time_zone(SessionConfig::new(), "America/New_York");
        let ltz_dates = invoke_local_dates(&ltz, config.options()).expect("LTZ date");
        let ntz_dates = invoke_local_dates(&ntz, config.options()).expect("NTZ date");
        let ltz_values = ltz_dates
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("Date32");
        let ntz_values = ntz_dates
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("Date32");
        assert_eq!(
            ltz_values.value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2024, 6, 14).expect("date")),
            "NY session-zone date of 03:00Z"
        );
        assert_eq!(
            ntz_values.value(0),
            Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2024, 6, 15).expect("date")),
            "NTZ wall date ignores the session zone"
        );
    }
}
