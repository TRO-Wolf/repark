//! Spark calendar functions and date-part shims.

use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, StringBuilder, TimestampMicrosecondArray,
};
use arrow::compute::{DatePart, cast, date_part};
use arrow::datatypes::{
    DataType, Date32Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use chrono::{
    DateTime, Datelike, Days, FixedOffset, MappedLocalTime, NaiveDate, NaiveDateTime, Offset,
    TimeDelta, TimeZone, Timelike,
};
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::session_time_zone::session_time_zone_from_options;

/// `date_trunc` returns a microsecond timestamp with Spark's LTZ wire type.
const TIMESTAMP_UNIT: TimeUnit = TimeUnit::Microsecond;

/// The zone annotation applied to instant arguments before session-zone extraction.
const INSTANT_ZONE: &str = "UTC";

/// The Spark date functions this module contributes.
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        year_udf(),
        yearofweek_udf(),
        quarter_udf(),
        month_udf(),
        weekofyear_udf(),
        dayofmonth_udf(),
        day_udf(),
        dayofyear_udf(),
        dayofweek_udf(),
        weekday_udf(),
        hour_udf(),
        minute_udf(),
        second_udf(),
        make_date_udf(),
        add_months_udf(),
        date_format_udf(),
        trunc_udf(),
        date_trunc_udf(),
    ]
}

/// Spark calendar-field extractors exposed as named UDF constructors.
#[must_use]
pub fn year_udf() -> Arc<ScalarUDF> {
    part_udf("year", DatePart::Year, 0)
}

#[must_use]
pub fn yearofweek_udf() -> Arc<ScalarUDF> {
    part_udf("yearofweek", DatePart::YearISO, 0)
}

#[must_use]
pub fn quarter_udf() -> Arc<ScalarUDF> {
    part_udf("quarter", DatePart::Quarter, 0)
}

#[must_use]
pub fn month_udf() -> Arc<ScalarUDF> {
    part_udf("month", DatePart::Month, 0)
}

#[must_use]
pub fn weekofyear_udf() -> Arc<ScalarUDF> {
    part_udf("weekofyear", DatePart::WeekISO, 0)
}

#[must_use]
pub fn dayofmonth_udf() -> Arc<ScalarUDF> {
    part_udf("dayofmonth", DatePart::Day, 0)
}

#[must_use]
pub fn day_udf() -> Arc<ScalarUDF> {
    part_udf("day", DatePart::Day, 0)
}

#[must_use]
pub fn dayofyear_udf() -> Arc<ScalarUDF> {
    part_udf("dayofyear", DatePart::DayOfYear, 0)
}

/// `dayofweek` is Spark's 1=Sunday..7=Saturday (arrow's `DayOfWeekSunday0` is 0-based; we add 1).
#[must_use]
pub fn dayofweek_udf() -> Arc<ScalarUDF> {
    part_udf("dayofweek", DatePart::DayOfWeekSunday0, 1)
}

#[must_use]
pub fn weekday_udf() -> Arc<ScalarUDF> {
    part_udf("weekday", DatePart::DayOfWeekMonday0, 0)
}

/// Spark `hour(timestamp|time)` — `0`..=`23` (arrow `DatePart::Hour`; accepts Time32/64).
#[must_use]
pub fn hour_udf() -> Arc<ScalarUDF> {
    part_udf("hour", DatePart::Hour, 0)
}

/// Spark `minute(timestamp|time)` — `0`..=`59`.
#[must_use]
pub fn minute_udf() -> Arc<ScalarUDF> {
    part_udf("minute", DatePart::Minute, 0)
}

/// Spark `second(timestamp|time)` — `0`..=`59` (integer seconds; fractional not returned).
#[must_use]
pub fn second_udf() -> Arc<ScalarUDF> {
    part_udf("second", DatePart::Second, 0)
}

#[must_use]
pub fn make_date_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(MakeDate::new()))
}

#[must_use]
pub fn add_months_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(AddMonths::new()))
}

#[must_use]
pub fn date_format_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(DateFormat::new()))
}

#[must_use]
pub fn trunc_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(TruncDate::new()))
}

#[must_use]
pub fn date_trunc_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(DateTrunc::new()))
}

fn part_udf(name: &'static str, part: DatePart, spark_offset: i32) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(DatePartUdf::new(
        name,
        part,
        spark_offset,
    )))
}

/// Coerce dates, timestamps, times, strings, and NULL while preserving LTZ versus NTZ semantics.
fn coerce_date_arg(arg: &DataType) -> Option<DataType> {
    match arg {
        DataType::Timestamp(unit, Some(_)) => {
            Some(DataType::Timestamp(*unit, Some(INSTANT_ZONE.into())))
        }
        DataType::Timestamp(_, None) => Some(arg.clone()),
        DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
            Some(arg.clone())
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
            Some(DataType::Date32)
        }
        _ => None,
    }
}

/// Resolve the session zone once per invocation and retain a typed error for invalid carriers.
fn extraction_time_zone(options: &ConfigOptions) -> Result<Tz> {
    let zone = session_time_zone_from_options(options);
    Tz::from_str(zone).map_err(|error| {
        DataFusionError::Execution(format!(
            "session timezone {zone:?} could not be resolved at query time ({error})"
        ))
    })
}

/// Return the zone annotation of an already-coerced argument.
fn is_instant(arg: &DataType) -> bool {
    matches!(arg, DataType::Timestamp(_, Some(_)))
}

/// Re-annotate an instant array without changing its epoch ticks.
fn resolve_instant_in_zone(array: &ArrayRef, zone: &str) -> Result<ArrayRef> {
    if !is_instant(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    let DataType::Timestamp(unit, _) = array.data_type() else {
        return Ok(Arc::clone(array));
    };
    Ok(cast(
        array.as_ref(),
        &DataType::Timestamp(*unit, Some(zone.into())),
    )?)
}

/// `DatePartUdf` — vectorized calendar-field extraction with a Spark indexing offset.
#[derive(Debug)]
struct DatePartUdf {
    name: &'static str,
    part: DatePart,
    spark_offset: i32,
    signature: Signature,
}

impl DatePartUdf {
    fn new(name: &'static str, part: DatePart, spark_offset: i32) -> Self {
        Self {
            name,
            part,
            spark_offset,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for DatePartUdf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for DatePartUdf {}

impl Hash for DatePartUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for DatePartUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "'{}' expects exactly one argument, got {}",
                self.name,
                arg_types.len()
            )));
        };
        coerce_date_arg(arg).map(|t| vec![t]).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'{}' has no Spark-compatible overload for argument type {arg}",
                self.name
            ))
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let zone = session_time_zone_from_options(args.config_options.as_ref());
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let resolved = resolve_instant_in_zone(&arrays[0], zone)?;
        let extracted = date_part(resolved.as_ref(), self.part)?;
        let extracted = cast(extracted.as_ref(), &DataType::Int32)?;
        let result = if self.spark_offset == 0 {
            extracted
        } else {
            let offset = self.spark_offset;
            let shifted = extracted
                .as_primitive::<Int32Type>()
                .unary::<_, Int32Type>(|v| v + offset);
            Arc::new(shifted) as ArrayRef
        };
        Ok(ColumnarValue::Array(result))
    }
}

/// `MakeDate` — Spark `make_date(year, month, day) -> DATE`; invalid dates return NULL.
#[derive(Debug)]
struct MakeDate {
    name: &'static str,
    signature: Signature,
}

impl MakeDate {
    fn new() -> Self {
        Self {
            name: "make_date",
            signature: Signature::uniform(3, vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

impl PartialEq for MakeDate {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for MakeDate {}

impl Hash for MakeDate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for MakeDate {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let years = cast(arrays[0].as_ref(), &DataType::Int64)?;
        let years = years.as_primitive::<Int64Type>();
        let months = cast(arrays[1].as_ref(), &DataType::Int64)?;
        let months = months.as_primitive::<Int64Type>();
        let days = cast(arrays[2].as_ref(), &DataType::Int64)?;
        let days = days.as_primitive::<Int64Type>();

        let mut builder = Date32Array::builder(years.len());
        for row in 0..years.len() {
            if years.is_null(row) || months.is_null(row) || days.is_null(row) {
                builder.append_null();
                continue;
            }
            let date = match (
                i32::try_from(years.value(row)),
                u32::try_from(months.value(row)),
                u32::try_from(days.value(row)),
            ) {
                (Ok(year), Ok(month), Ok(day)) => NaiveDate::from_ymd_opt(year, month, day),
                _ => None,
            };
            match date {
                Some(valid) => builder.append_value(Date32Type::from_naive_date(valid)),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Coerce calendar-math inputs while preserving instant versus wall-clock semantics.
fn coerce_to_date32(arg: &DataType) -> Option<DataType> {
    match arg {
        DataType::Timestamp(_, Some(_)) => Some(DataType::Timestamp(
            TIMESTAMP_UNIT,
            Some(INSTANT_ZONE.into()),
        )),
        DataType::Timestamp(_, None) => Some(DataType::Timestamp(TIMESTAMP_UNIT, None)),
        DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => Some(DataType::Date32),
        _ => None,
    }
}

/// Coerce `date_format` and `date_trunc` inputs to fixed-point timestamps.
fn coerce_to_timestamp_micros(arg: &DataType) -> Option<DataType> {
    match arg {
        DataType::Timestamp(_, Some(_)) => Some(DataType::Timestamp(
            TIMESTAMP_UNIT,
            Some(INSTANT_ZONE.into()),
        )),
        DataType::Timestamp(_, None) => Some(DataType::Timestamp(TIMESTAMP_UNIT, None)),
        DataType::Date32 | DataType::Date64 | DataType::Null => Some(DataType::Date32),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(DataType::Utf8),
        _ => None,
    }
}

/// Distinguish instant, zone-free, and NTZ values after microsecond coercion.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum LocalSource {
    /// Epoch-relative instant resolved in the session zone.
    Instant,
    /// Zone-free wall clock promoted to a session-zone instant by `date_trunc`.
    ZoneFree,
    /// Spark `TIMESTAMP_NTZ`: wall-clock ticks, no session zone, naive µs on the way out.
    NaiveTimestamp,
}

/// Return local micros, the session zone, and the source representation used by both shims.
fn invoke_local_micros(
    array: &ArrayRef,
    options: &ConfigOptions,
) -> Result<(ArrayRef, Tz, LocalSource)> {
    let source = match array.data_type() {
        DataType::Timestamp(_, Some(_)) => LocalSource::Instant,
        DataType::Timestamp(_, None) => LocalSource::NaiveTimestamp,
        _ => LocalSource::ZoneFree,
    };
    let zone = extraction_time_zone(options)?;
    let micros = cast(array.as_ref(), &DataType::Timestamp(TIMESTAMP_UNIT, None))?;
    Ok((micros, zone, source))
}

/// Read date-like args as `Date32` on the calendar Spark uses.
pub(crate) fn invoke_local_dates(array: &ArrayRef, options: &ConfigOptions) -> Result<ArrayRef> {
    if !is_instant(array.data_type()) {
        return Ok(cast(array.as_ref(), &DataType::Date32)?);
    }
    let zone = extraction_time_zone(options)?;
    let micros = cast(array.as_ref(), &DataType::Timestamp(TIMESTAMP_UNIT, None))?;
    let micros = micros.as_primitive::<TimestampMicrosecondType>();
    let mut builder = Date32Array::builder(micros.len());
    for row in 0..micros.len() {
        if micros.is_null(row) {
            builder.append_null();
            continue;
        }
        match local_datetime_from_micros(micros.value(row), zone) {
            Some(local) => builder.append_value(Date32Type::from_naive_date(local.date())),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// A microsecond timestamp (µs since the Unix epoch, UTC) as a naive local-instant datetime.
pub(crate) fn datetime_from_micros(micros: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(micros).map(|instant| instant.naive_utc())
}

/// Localize a zoneless wall clock (ticks as if the digits were UTC) in `zone` → instant µs.
pub(crate) fn localize_wall_micros_in_zone(wall_micros: i64, zone: Tz) -> Option<i64> {
    datetime_from_micros(wall_micros)
        .and_then(|naive| micros_from_local_datetime(naive, zone, None))
}

/// Return an instant's local datetime in `zone`; `None` means outside chrono's range.
pub(crate) fn local_datetime_from_micros(micros: i64, zone: Tz) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(micros)
        .map(|instant| instant.with_timezone(&zone).naive_local())
}

/// Return the UTC offset at an instant; `None` means outside chrono's range.
fn offset_at_instant(micros: i64, zone: Tz) -> Option<FixedOffset> {
    DateTime::from_timestamp_micros(micros)
        .map(|instant| instant.with_timezone(&zone).offset().fix())
}

/// Look back 26 hours to obtain the pre-gap offset; this bound covers the IANA transition range.
const GAP_LOOKBACK_HOURS: i64 = 26;

/// Return the offset before a DST gap so local walls resolve like Spark's `ofLocal`.
fn offset_before_gap(local: NaiveDateTime, zone: Tz) -> Option<FixedOffset> {
    let probe = local.checked_sub_signed(TimeDelta::try_hours(GAP_LOOKBACK_HOURS)?)?;
    Some(zone.offset_from_utc_datetime(&probe).fix())
}

/// Map a local wall to epoch micros with Spark DST rules for overlaps and gaps.
pub(crate) fn micros_from_local_datetime(
    local: NaiveDateTime,
    zone: Tz,
    preferred: Option<FixedOffset>,
) -> Option<i64> {
    let offset = match zone.offset_from_local_datetime(&local) {
        MappedLocalTime::Single(single) => single.fix(),
        MappedLocalTime::Ambiguous(earliest, latest) => {
            let (earliest, latest) = (earliest.fix(), latest.fix());
            match preferred {
                Some(source) if source == earliest || source == latest => source,
                _ => earliest,
            }
        }
        MappedLocalTime::None => offset_before_gap(local, zone)?,
    };
    let utc =
        local.checked_sub_signed(TimeDelta::try_seconds(i64::from(offset.local_minus_utc()))?)?;
    Some(utc.and_utc().timestamp_micros())
}

/// Days in `(year, month)`: the day before the first of the next month, so leap years fall out.
fn days_in_month(year: i32, month: u32) -> Option<u32> {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_of_next = NaiveDate::from_ymd_opt(next_year, next_month, 1)?;
    Some(first_of_next.pred_opt()?.day())
}

fn spark_add_months(date: NaiveDate, months: i32) -> Option<NaiveDate> {
    let source_month_index = date
        .year()
        .checked_mul(12)?
        .checked_add(i32::try_from(date.month0()).ok()?)?;
    let target_month_index = source_month_index.checked_add(months)?;
    let target_year = target_month_index.div_euclid(12);
    let target_month = u32::try_from(target_month_index.rem_euclid(12)).ok()? + 1;
    let last_day_of_target = days_in_month(target_year, target_month)?;
    let day = if date.day() > last_day_of_target {
        last_day_of_target
    } else {
        date.day()
    };
    NaiveDate::from_ymd_opt(target_year, target_month, day)
}

/// Monday of the ISO week containing `date`, matching Spark `trunc`/`date_trunc` `'WEEK'`.
fn start_of_week(date: NaiveDate) -> Option<NaiveDate> {
    let days_back = u64::from(date.weekday().num_days_from_monday());
    date.checked_sub_days(Days::new(days_back))
}

/// The first day of the quarter (Jan/Apr/Jul/Oct 1) containing `date`.
fn start_of_quarter(date: NaiveDate) -> Option<NaiveDate> {
    let first_month_of_quarter = date.month0() - (date.month0() % 3) + 1;
    NaiveDate::from_ymd_opt(date.year(), first_month_of_quarter, 1)
}

/// Spark `trunc(date, format)` — truncate a DATE to `format`, returning a DATE.
fn trunc_date_to(date: NaiveDate, format: &str) -> Option<NaiveDate> {
    match format.to_ascii_uppercase().as_str() {
        "YEAR" | "YYYY" | "YY" => NaiveDate::from_ymd_opt(date.year(), 1, 1),
        "QUARTER" => start_of_quarter(date),
        "MONTH" | "MON" | "MM" => NaiveDate::from_ymd_opt(date.year(), date.month(), 1),
        "WEEK" => start_of_week(date),
        _ => None,
    }
}

/// Spark `date_trunc(format, timestamp)` — truncate a TIMESTAMP to `format`, returning a TIMESTAMP.
fn trunc_datetime_to(datetime: NaiveDateTime, format: &str) -> Option<NaiveDateTime> {
    let date = datetime.date();
    let at_midnight = |day: NaiveDate| day.and_hms_opt(0, 0, 0);
    match format.to_ascii_uppercase().as_str() {
        "YEAR" | "YYYY" | "YY" => at_midnight(NaiveDate::from_ymd_opt(date.year(), 1, 1)?),
        "QUARTER" => at_midnight(start_of_quarter(date)?),
        "MONTH" | "MON" | "MM" => {
            at_midnight(NaiveDate::from_ymd_opt(date.year(), date.month(), 1)?)
        }
        "WEEK" => at_midnight(start_of_week(date)?),
        "DAY" | "DD" => at_midnight(date),
        "HOUR" => date.and_hms_opt(datetime.hour(), 0, 0),
        "MINUTE" => date.and_hms_opt(datetime.hour(), datetime.minute(), 0),
        "SECOND" => date.and_hms_opt(datetime.hour(), datetime.minute(), datetime.second()),
        "MILLISECOND" => {
            let floored_nanos = (datetime.nanosecond() / 1_000_000) * 1_000_000;
            datetime.with_nanosecond(floored_nanos)
        }
        "MICROSECOND" => Some(datetime),
        _ => None,
    }
}

/// Render `count` repeats of pattern letter `letter` against `datetime` as Spark `date_format`.
fn render_pattern_field(letter: char, count: usize, datetime: NaiveDateTime) -> Result<String> {
    let unsupported = |letter: char| {
        Err(DataFusionError::Execution(format!(
            "date_format: unsupported pattern letter '{letter}'"
        )))
    };
    match letter {
        'y' | 'u' => Ok(if count == 2 {
            format!("{:02}", datetime.year().rem_euclid(100))
        } else {
            format!("{:0width$}", datetime.year(), width = count)
        }),
        'M' | 'L' => Ok(match count {
            1 => datetime.month().to_string(),
            2 => format!("{:02}", datetime.month()),
            3 => datetime.format("%b").to_string(),
            _ => datetime.format("%B").to_string(),
        }),
        'd' => Ok(format!("{:0width$}", datetime.day(), width = count)),
        'D' => Ok(format!("{:0width$}", datetime.ordinal(), width = count)),
        'q' | 'Q' => {
            let quarter = datetime.month0() / 3 + 1;
            Ok(if count <= 2 {
                format!("{quarter:0count$}")
            } else {
                format!("Q{quarter}")
            })
        }
        'E' => Ok(if count <= 3 {
            datetime.format("%a").to_string()
        } else {
            datetime.format("%A").to_string()
        }),
        'H' => Ok(format!("{:0width$}", datetime.hour(), width = count)),
        'm' => Ok(format!("{:0width$}", datetime.minute(), width = count)),
        's' => Ok(format!("{:0width$}", datetime.second(), width = count)),
        other => unsupported(other),
    }
}

/// One token of a pre-compiled Java-style `date_format` pattern (PERF-02).
#[derive(Clone, Debug)]
pub(crate) enum JavaPatternToken {
    /// Verbatim text (quoted runs + non-letter punctuation).
    Literal(String),
    /// A run of `count` identical ASCII pattern letters.
    Field { letter: char, count: usize },
}

/// Compile a Java-style `date_format` pattern into tokens.
pub(crate) fn compile_java_pattern(pattern: &str) -> Result<Vec<JavaPatternToken>> {
    let characters: Vec<char> = pattern.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < characters.len() {
        let current = characters[index];
        if current == '\'' {
            index += 1;
            if index < characters.len() && characters[index] == '\'' {
                tokens.push(JavaPatternToken::Literal("'".to_string()));
                index += 1;
                continue;
            }
            let mut literal = String::new();
            let mut closed = false;
            while index < characters.len() {
                if characters[index] == '\'' {
                    index += 1;
                    closed = true;
                    break;
                }
                literal.push(characters[index]);
                index += 1;
            }
            if !closed {
                return Err(DataFusionError::Execution(format!(
                    "date_format: unterminated quoted literal in pattern {pattern:?}"
                )));
            }
            tokens.push(JavaPatternToken::Literal(literal));
            continue;
        }
        if current.is_ascii_alphabetic() {
            let start = index;
            while index < characters.len() && characters[index] == current {
                index += 1;
            }
            tokens.push(JavaPatternToken::Field {
                letter: current,
                count: index - start,
            });
            continue;
        }
        let mut literal = String::new();
        while index < characters.len() {
            let ch = characters[index];
            if ch == '\'' || ch.is_ascii_alphabetic() {
                break;
            }
            literal.push(ch);
            index += 1;
        }
        tokens.push(JavaPatternToken::Literal(literal));
    }
    Ok(tokens)
}

/// Render a pre-compiled pattern against `datetime`.
pub(crate) fn format_compiled_java_pattern(
    datetime: NaiveDateTime,
    tokens: &[JavaPatternToken],
) -> Result<String> {
    let mut output = String::new();
    for token in tokens {
        match token {
            JavaPatternToken::Literal(text) => output.push_str(text),
            JavaPatternToken::Field { letter, count } => {
                output.push_str(&render_pattern_field(*letter, *count, datetime)?);
            }
        }
    }
    Ok(output)
}

// SAF-001: out-of-range Date32 values return NULL instead of panicking.

/// `AddMonths` — Spark `add_months(start_date, num_months) -> DATE`.
#[derive(Debug)]
struct AddMonths {
    signature: Signature,
}

impl AddMonths {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for AddMonths {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for AddMonths {}

impl Hash for AddMonths {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for AddMonths {
    crate::shim_udf_boilerplate!("add_months");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [start, months] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "'add_months' expects (start_date, num_months), got {} argument(s)",
                arg_types.len()
            )));
        };
        let start = coerce_to_date32(start).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'add_months' cannot accept a start date of type {start}"
            ))
        })?;
        match months {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Null => Ok(vec![start, DataType::Int32]),
            other => Err(DataFusionError::Plan(format!(
                "'add_months' num_months must be an integer, got {other}"
            ))),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let starts = invoke_local_dates(&arrays[0], args.config_options.as_ref())?;
        let starts = starts.as_primitive::<Date32Type>();
        let months = cast(arrays[1].as_ref(), &DataType::Int32)?;
        let months = months.as_primitive::<Int32Type>();
        let mut builder = Date32Array::builder(starts.len());
        for row in 0..starts.len() {
            if starts.is_null(row) || months.is_null(row) {
                builder.append_null();
                continue;
            }
            let Some(start) = Date32Type::to_naive_date_opt(starts.value(row)) else {
                builder.append_null();
                continue;
            };
            match spark_add_months(start, months.value(row)) {
                Some(result) => builder.append_value(Date32Type::from_naive_date(result)),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// `TruncDate` — Spark `trunc(date, format) -> DATE`.
#[derive(Debug)]
struct TruncDate {
    signature: Signature,
}

impl TruncDate {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for TruncDate {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for TruncDate {}

impl Hash for TruncDate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for TruncDate {
    crate::shim_udf_boilerplate!("trunc");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [date, format] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "'trunc' expects (date, format), got {} argument(s)",
                arg_types.len()
            )));
        };
        let date = coerce_to_date32(date).ok_or_else(|| {
            DataFusionError::Plan(format!("'trunc' cannot accept a date of type {date}"))
        })?;
        let _ = format;
        Ok(vec![date, DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let dates = invoke_local_dates(&arrays[0], args.config_options.as_ref())?;
        let dates = dates.as_primitive::<Date32Type>();
        let formats = cast(arrays[1].as_ref(), &DataType::Utf8)?;
        let formats = formats.as_string::<i32>();
        let mut builder = Date32Array::builder(dates.len());
        for row in 0..dates.len() {
            if dates.is_null(row) || formats.is_null(row) {
                builder.append_null();
                continue;
            }
            let Some(date) = Date32Type::to_naive_date_opt(dates.value(row)) else {
                builder.append_null();
                continue;
            };
            match trunc_date_to(date, formats.value(row)) {
                Some(result) => builder.append_value(Date32Type::from_naive_date(result)),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// `DateTrunc` — Spark `date_trunc(format, timestamp) -> TIMESTAMP`.
#[derive(Debug)]
struct DateTrunc {
    signature: Signature,
}

impl DateTrunc {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Stable),
        }
    }
}

impl PartialEq for DateTrunc {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DateTrunc {}

impl Hash for DateTrunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for DateTrunc {
    crate::shim_udf_boilerplate!("date_trunc");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let ntz = arg_types
            .get(1)
            .is_some_and(|data_type| matches!(data_type, DataType::Timestamp(_, None)));
        if ntz {
            Ok(DataType::Timestamp(TIMESTAMP_UNIT, None))
        } else {
            Ok(DataType::Timestamp(
                TIMESTAMP_UNIT,
                Some(INSTANT_ZONE.into()),
            ))
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [format, timestamp] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "'date_trunc' expects (format, timestamp), got {} argument(s)",
                arg_types.len()
            )));
        };
        let _ = format;
        let timestamp = coerce_to_timestamp_micros(timestamp).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'date_trunc' cannot accept a timestamp of type {timestamp}"
            ))
        })?;
        Ok(vec![DataType::Utf8, timestamp])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let formats = cast(arrays[0].as_ref(), &DataType::Utf8)?;
        let formats = formats.as_string::<i32>();
        let (timestamps, zone, source) =
            invoke_local_micros(&arrays[1], args.config_options.as_ref())?;
        let timestamps = timestamps.as_primitive::<TimestampMicrosecondType>();
        let mut builder = TimestampMicrosecondArray::builder(timestamps.len());
        for row in 0..timestamps.len() {
            if formats.is_null(row) || timestamps.is_null(row) {
                builder.append_null();
                continue;
            }
            let micros = timestamps.value(row);
            let truncated = match source {
                LocalSource::Instant => local_datetime_from_micros(micros, zone)
                    .and_then(|local| trunc_datetime_to(local, formats.value(row)))
                    .and_then(|local| {
                        micros_from_local_datetime(local, zone, offset_at_instant(micros, zone))
                    }),
                LocalSource::ZoneFree => datetime_from_micros(micros)
                    .and_then(|datetime| trunc_datetime_to(datetime, formats.value(row)))
                    .and_then(|local| micros_from_local_datetime(local, zone, None)),
                LocalSource::NaiveTimestamp => datetime_from_micros(micros)
                    .and_then(|datetime| trunc_datetime_to(datetime, formats.value(row)))
                    .map(|local| local.and_utc().timestamp_micros()),
            };
            match truncated {
                Some(result) => builder.append_value(result),
                None => builder.append_null(),
            }
        }
        let finished = builder.finish();
        let finished = if source == LocalSource::NaiveTimestamp {
            finished
        } else {
            finished.with_timezone(INSTANT_ZONE)
        };
        Ok(ColumnarValue::Array(Arc::new(finished)))
    }
}

/// `DateFormat` — Spark `date_format(timestamp, format) -> STRING`.
#[derive(Debug)]
struct DateFormat {
    signature: Signature,
}

impl DateFormat {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
        }
    }
}

impl PartialEq for DateFormat {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DateFormat {}

impl Hash for DateFormat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for DateFormat {
    crate::shim_udf_boilerplate!("date_format");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [timestamp, format] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "'date_format' expects (timestamp, format), got {} argument(s)",
                arg_types.len()
            )));
        };
        let timestamp = coerce_to_timestamp_micros(timestamp).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'date_format' cannot accept a timestamp of type {timestamp}"
            ))
        })?;
        let _ = format;
        Ok(vec![timestamp, DataType::Utf8])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (timestamps, zone, source) =
            invoke_local_micros(&arrays[0], args.config_options.as_ref())?;
        let timestamps = timestamps.as_primitive::<TimestampMicrosecondType>();
        let formats = cast(arrays[1].as_ref(), &DataType::Utf8)?;
        let formats = formats.as_string::<i32>();
        let mut cached_pattern: Option<(String, Vec<JavaPatternToken>)> = None;
        let mut builder = StringBuilder::with_capacity(timestamps.len(), 0);
        for row in 0..timestamps.len() {
            if timestamps.is_null(row) || formats.is_null(row) {
                builder.append_null();
                continue;
            }
            let micros = timestamps.value(row);
            let rendered = match source {
                LocalSource::Instant => local_datetime_from_micros(micros, zone),
                LocalSource::ZoneFree | LocalSource::NaiveTimestamp => datetime_from_micros(micros),
            };
            let Some(datetime) = rendered else {
                builder.append_null();
                continue;
            };
            let pattern = formats.value(row);
            let needs_compile = match &cached_pattern {
                Some((previous, _)) => previous.as_str() != pattern,
                None => true,
            };
            if needs_compile {
                let tokens = compile_java_pattern(pattern)?;
                cached_pattern = Some((pattern.to_string(), tokens));
            }
            let tokens = cached_pattern
                .as_ref()
                .map(|(_, tokens)| tokens.as_slice())
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "date_format: internal pattern cache miss".to_string(),
                    )
                })?;
            builder.append_value(format_compiled_java_pattern(datetime, tokens)?);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    /// Build a context with the full repark function set registered (date shim included).
    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx
    }

    /// Run `sql` and return column 0 of the single result row as an `Option<i32>`.
    async fn eval_i32(sql: &str) -> Option<i32> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (!col.is_null(0)).then(|| col.value(0))
    }

    /// Run `sql` and return column 0 of the single result row as an `Option<i32>` (Date32 days).
    async fn eval_date_days(sql: &str) -> Option<i32> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        (!col.is_null(0)).then(|| col.value(0))
    }

    /// Coercion is idempotent and preserves DATE walls across repeated analysis.
    #[test]
    fn coercion_is_idempotent_so_a_second_analysis_cannot_promote_a_date() {
        let inputs = [
            DataType::Date32,
            DataType::Date64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Null,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
        ];
        for input in inputs {
            for (name, coerce) in [
                (
                    "coerce_date_arg",
                    coerce_date_arg as fn(&DataType) -> Option<DataType>,
                ),
                ("coerce_to_timestamp_micros", coerce_to_timestamp_micros),
            ] {
                let Some(once) = coerce(&input) else { continue };
                let twice = coerce(&once)
                    .unwrap_or_else(|| panic!("{name} must accept its own output for {input}"));
                assert_eq!(
                    once, twice,
                    "{name} is not idempotent on {input}: a re-analysis would change the meaning \
                     of the argument"
                );
            }
        }
    }

    /// Only a tz-annotated argument is an instant; the coercion is what puts the annotation there.
    #[test]
    fn only_timestamp_arguments_are_coerced_to_instants() {
        for (input, instant) in [
            (DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("Asia/Tokyo".into())),
                true,
            ),
            (DataType::Date32, false),
            (DataType::Date64, false),
            (DataType::Utf8, false),
            (DataType::Null, false),
            (DataType::Time64(TimeUnit::Nanosecond), false),
        ] {
            for coerce in [
                coerce_date_arg as fn(&DataType) -> Option<DataType>,
                coerce_to_timestamp_micros,
            ] {
                // `date_format`/`date_trunc` do not accept a TIME at all; the extractors do.
                let Some(coerced) = coerce(&input) else {
                    continue;
                };
                assert_eq!(
                    is_instant(&coerced),
                    instant,
                    "{input} coerced to {coerced}: instant-ness must follow the ARGUMENT, never \
                     the session"
                );
            }
        }
    }

    // Golden values use Python's ISO-8601 calendar, the same basis Spark's date functions use.

    #[tokio::test]
    async fn extractors_match_spark_on_a_rich_date() {
        // 2024-03-15 (a Friday, in leap year 2024).
        assert_eq!(eval_i32("SELECT year(DATE '2024-03-15')").await, Some(2024));
        assert_eq!(eval_i32("SELECT month(DATE '2024-03-15')").await, Some(3));
        assert_eq!(
            eval_i32("SELECT dayofmonth(DATE '2024-03-15')").await,
            Some(15)
        );
        assert_eq!(eval_i32("SELECT day(DATE '2024-03-15')").await, Some(15));
        assert_eq!(eval_i32("SELECT quarter(DATE '2024-03-15')").await, Some(1));
        assert_eq!(
            eval_i32("SELECT dayofyear(DATE '2024-03-15')").await,
            Some(75)
        );
        assert_eq!(
            eval_i32("SELECT weekofyear(DATE '2024-03-15')").await,
            Some(11)
        );
        assert_eq!(
            eval_i32("SELECT dayofweek(DATE '2024-03-15')").await,
            Some(6)
        );
        assert_eq!(eval_i32("SELECT weekday(DATE '2024-03-15')").await, Some(4));
    }

    /// `dayofweek` is 1=Sunday..7=Saturday; `weekday` is 0=Monday..6=Sunday.
    #[tokio::test]
    async fn dayofweek_and_weekday_use_spark_indexing() {
        // 2024-01-07 Sunday, 2024-01-08 Monday, 2024-01-13 Saturday.
        assert_eq!(
            eval_i32("SELECT dayofweek(DATE '2024-01-07')").await,
            Some(1)
        );
        assert_eq!(
            eval_i32("SELECT dayofweek(DATE '2024-01-08')").await,
            Some(2)
        );
        assert_eq!(
            eval_i32("SELECT dayofweek(DATE '2024-01-13')").await,
            Some(7)
        );
        assert_eq!(eval_i32("SELECT weekday(DATE '2024-01-07')").await, Some(6));
        assert_eq!(eval_i32("SELECT weekday(DATE '2024-01-08')").await, Some(0));
        assert_eq!(eval_i32("SELECT weekday(DATE '2024-01-13')").await, Some(5));
    }

    /// ISO week-year boundary: 2021-01-01 (a Friday) belongs to ISO week 53 of 2020.
    #[tokio::test]
    async fn weekofyear_and_yearofweek_follow_iso_8601() {
        assert_eq!(eval_i32("SELECT year(DATE '2021-01-01')").await, Some(2021));
        assert_eq!(
            eval_i32("SELECT weekofyear(DATE '2021-01-01')").await,
            Some(53)
        );
        assert_eq!(
            eval_i32("SELECT yearofweek(DATE '2021-01-01')").await,
            Some(2020)
        );
        // 2020-12-31 is also ISO week 53 of 2020.
        assert_eq!(
            eval_i32("SELECT weekofyear(DATE '2020-12-31')").await,
            Some(53)
        );
        assert_eq!(
            eval_i32("SELECT yearofweek(DATE '2020-12-31')").await,
            Some(2020)
        );
    }

    #[tokio::test]
    async fn extractors_propagate_null() {
        assert_eq!(eval_i32("SELECT year(CAST(NULL AS DATE))").await, None);
        assert_eq!(eval_i32("SELECT dayofweek(CAST(NULL AS DATE))").await, None);
    }

    /// Time32/64 and timestamp inputs return Spark calendar fields.
    #[tokio::test]
    async fn hour_minute_second_accept_time_and_timestamp() {
        assert_eq!(eval_i32("SELECT hour(TIME '12:34:56')").await, Some(12));
        assert_eq!(eval_i32("SELECT minute(TIME '12:34:56')").await, Some(34));
        assert_eq!(eval_i32("SELECT second(TIME '12:34:56')").await, Some(56));
        assert_eq!(
            eval_i32("SELECT hour(TIMESTAMP '2017-11-06 15:16:17')").await,
            Some(15)
        );
        assert_eq!(eval_i32("SELECT hour(CAST(NULL AS TIME))").await, None);
    }

    #[tokio::test]
    async fn make_date_builds_valid_dates_and_nulls_invalid() {
        assert_eq!(
            eval_i32("SELECT year(make_date(2024, 2, 29))").await,
            Some(2024)
        );
        assert_eq!(
            eval_i32("SELECT month(make_date(2024, 2, 29))").await,
            Some(2)
        );
        assert_eq!(
            eval_i32("SELECT dayofmonth(make_date(2024, 2, 29))").await,
            Some(29)
        );
        // 2023-02-29 does not exist -> NULL (Spark, ANSI off).
        assert_eq!(eval_date_days("SELECT make_date(2023, 2, 29)").await, None);
        // A negative month is invalid -> NULL.
        assert_eq!(eval_date_days("SELECT make_date(2024, -1, 15)").await, None);
        // NULL component -> NULL.
        assert_eq!(
            eval_date_days("SELECT make_date(2024, CAST(NULL AS INT), 15)").await,
            None
        );
    }

    /// Spark applies these to strings and timestamps of any precision/zone, not just `DATE`.
    #[tokio::test]
    async fn extractors_accept_strings_and_timestamps_like_spark() {
        // String literal -> parsed to a date (Spark coerces 'yyyy-MM-dd').
        assert_eq!(eval_i32("SELECT year('2024-03-15')").await, Some(2024));
        assert_eq!(eval_i32("SELECT dayofweek('2024-01-07')").await, Some(1));
        // Nanosecond timestamp (the default `TIMESTAMP` literal type in DataFusion).
        assert_eq!(
            eval_i32("SELECT year(TIMESTAMP '2024-03-15 10:30:00')").await,
            Some(2024)
        );
        assert_eq!(
            eval_i32("SELECT month(TIMESTAMP '2024-03-15 10:30:00')").await,
            Some(3)
        );
        // Microsecond timestamp (what iceberg-datafusion yields for Iceberg `timestamp` columns).
        assert_eq!(
            eval_i32("SELECT year(CAST('2024-03-15T10:30:00' AS TIMESTAMP(6)))").await,
            Some(2024)
        );
        // An unsupported argument type is a clear planning error, not a silent wrong answer.
        assert!(ctx().sql("SELECT year(42)").await.is_err());
    }

    /// Decode column 0 of the single result row as an ISO date string (`Date32` → `yyyy-MM-dd`).
    async fn eval_date_iso(sql: &str) -> Option<String> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        if col.is_null(0) {
            return None;
        }
        Date32Type::to_naive_date_opt(col.value(0)).map(|date| date.format("%Y-%m-%d").to_string())
    }

    /// Decode column 0 as an ISO timestamp string at microsecond precision.
    async fn eval_timestamp_iso(sql: &str) -> Option<String> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        (!col.is_null(0)).then(|| {
            datetime_from_micros(col.value(0))
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
    }

    /// Decode column 0 of the single result row as a UTF-8 string.
    async fn eval_string(sql: &str) -> Option<String> {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (!col.is_null(0)).then(|| col.value(0).to_string())
    }

    /// Extreme Date32 values return NULL instead of panicking; the in-range control remains exact.
    #[tokio::test]
    async fn extreme_date32_add_months_and_trunc_null_without_panic() {
        let context = ctx();
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
        let days = Date32Array::from(vec![
            Some(i32::MIN),
            Some(i32::MAX),
            Some(0), // 1970-01-01 — in-range control
            None,
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(days)]).unwrap();
        context.register_batch("extreme_dates", batch).unwrap();

        // add_months: extremes → NULL; epoch + 1 month → 1970-02-01; NULL in → NULL out.
        let add_batches = context
            .sql("SELECT add_months(d, 1) AS r FROM extreme_dates")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let add_col = add_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert!(
            add_col.is_null(0),
            "i32::MIN add_months must be NULL (no panic)"
        );
        assert!(
            add_col.is_null(1),
            "i32::MAX add_months must be NULL (no panic)"
        );
        assert!(!add_col.is_null(2), "epoch add_months must stay non-null");
        // 1970-01-01 + 1 month = 1970-02-01 = 31 days since epoch.
        assert_eq!(add_col.value(2), 31);
        assert!(add_col.is_null(3), "NULL input stays NULL");

        // trunc: extremes → NULL; epoch → first of month (same day); NULL in → NULL out.
        let trunc_batches = context
            .sql("SELECT trunc(d, 'MM') AS r FROM extreme_dates")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let trunc_col = trunc_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert!(
            trunc_col.is_null(0),
            "i32::MIN trunc must be NULL (no panic)"
        );
        assert!(
            trunc_col.is_null(1),
            "i32::MAX trunc must be NULL (no panic)"
        );
        assert!(!trunc_col.is_null(2));
        assert_eq!(trunc_col.value(2), 0, "trunc(1970-01-01, MM) stays epoch");
        assert!(trunc_col.is_null(3));
    }

    /// SAF-001 companion: Date32 at chrono boundaries still compute (value pin).
    #[tokio::test]
    async fn chrono_boundary_date32_add_months_computes() {
        assert_eq!(
            eval_date_iso("SELECT add_months(DATE '0001-01-01', 1)").await,
            Some("0001-02-01".to_string())
        );
        assert_eq!(
            eval_date_iso("SELECT trunc(DATE '9999-12-31', 'MM')").await,
            Some("9999-12-01".to_string())
        );
        assert_eq!(
            eval_date_iso("SELECT add_months(DATE '1970-01-01', 1)").await,
            Some("1970-02-01".to_string())
        );
    }

    /// `LargeUtf8` format arguments return safely after coercion.
    #[tokio::test]
    async fn trunc_accepts_large_utf8_format_without_panic() {
        use arrow::array::LargeStringArray;
        let context = ctx();
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("fmt", DataType::LargeUtf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(0), Some(i32::MIN)])),
                Arc::new(LargeStringArray::from(vec![Some("MM"), Some("MM")])),
            ],
        )
        .unwrap();
        context.register_batch("large_fmt", batch).unwrap();
        let batches = context
            .sql("SELECT trunc(d, fmt) AS r FROM large_fmt")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert!(!col.is_null(0));
        assert_eq!(col.value(0), 0, "trunc(epoch, MM) via LargeUtf8 format");
        assert!(
            col.is_null(1),
            "extreme Date32 still NULL (SAF-001) with LargeUtf8 format"
        );
    }

    /// SAF-001: calendar extractors on extreme Date32 must not panic.
    #[tokio::test]
    async fn extreme_date32_year_extractor_no_panic() {
        let context = ctx();
        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
        let days = Date32Array::from(vec![Some(i32::MIN), Some(i32::MAX), Some(0), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(days)]).unwrap();
        context.register_batch("extreme_year", batch).unwrap();
        let batches = context
            .sql("SELECT year(d) AS y FROM extreme_year")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.len(), 4);
        // Epoch control is defined; extremes may be null or a year, and NULL input stays NULL.
        assert!(!col.is_null(2), "year(epoch) must be non-null");
        assert_eq!(col.value(2), 1970);
        assert!(col.is_null(3), "year(NULL) stays NULL");
    }

    #[tokio::test]
    async fn add_months_matches_spark_end_of_month_semantics() {
        assert_eq!(
            eval_date_iso("SELECT add_months(DATE '2015-01-31', 1)").await,
            Some("2015-02-28".to_string())
        );
        // Feb-29 (leap) month-end + 12 months → Feb-28 of the following (non-leap) year.
        assert_eq!(
            eval_date_iso("SELECT add_months(DATE '2016-02-29', 12)").await,
            Some("2017-02-28".to_string())
        );
        // A mid-month day is carried across unchanged, backwards over a year and a month boundary.
        assert_eq!(
            eval_date_iso("SELECT add_months(DATE '2025-03-15', -12)").await,
            Some("2024-03-15".to_string())
        );
        assert_eq!(
            eval_date_iso("SELECT add_months(DATE '2025-01-15', -1)").await,
            Some("2024-12-15".to_string())
        );
        // NULL start → NULL.
        assert_eq!(
            eval_date_iso("SELECT add_months(CAST(NULL AS DATE), 1)").await,
            None
        );
    }

    /// Spark `trunc(date, fmt)` truncates a DATE.
    #[tokio::test]
    async fn trunc_matches_spark_and_nulls_invalid_formats() {
        // 2025-05-14 is a Wednesday in Q2.
        assert_eq!(
            eval_date_iso("SELECT trunc(DATE '2025-05-14', 'MM')").await,
            Some("2025-05-01".to_string())
        );
        assert_eq!(
            eval_date_iso("SELECT trunc(DATE '2025-05-14', 'YEAR')").await,
            Some("2025-01-01".to_string())
        );
        assert_eq!(
            eval_date_iso("SELECT trunc(DATE '2025-05-14', 'QUARTER')").await,
            Some("2025-04-01".to_string())
        );
        // WEEK truncates to Monday (ISO); 2025-05-14 (Wed) → 2025-05-12 (Mon).
        assert_eq!(
            eval_date_iso("SELECT trunc(DATE '2025-05-14', 'week')").await,
            Some("2025-05-12".to_string())
        );
        // 'Q' is NOT a valid Spark trunc format (only 'QUARTER') → NULL, matching Spark.
        assert_eq!(
            eval_date_iso("SELECT trunc(DATE '2025-05-14', 'Q')").await,
            None
        );
        assert_eq!(
            eval_date_iso("SELECT trunc(CAST(NULL AS DATE), 'MM')").await,
            None
        );
    }

    /// Spark `date_trunc(fmt, ts)` truncates a TIMESTAMP (format first) to a microsecond timestamp.
    #[tokio::test]
    async fn date_trunc_matches_spark() {
        // A DATE argument widens to midnight; WEEK → the containing Monday.
        assert_eq!(
            eval_timestamp_iso("SELECT date_trunc('week', DATE '2025-05-14')").await,
            Some("2025-05-12 00:00:00".to_string())
        );
        assert_eq!(
            eval_timestamp_iso("SELECT date_trunc('quarter', DATE '2025-05-14')").await,
            Some("2025-04-01 00:00:00".to_string())
        );
        // Time-of-day granularities keep the higher-order fields.
        assert_eq!(
            eval_timestamp_iso("SELECT date_trunc('MONTH', TIMESTAMP '2025-05-14 13:45:59')").await,
            Some("2025-05-01 00:00:00".to_string())
        );
        assert_eq!(
            eval_timestamp_iso("SELECT date_trunc('HOUR', TIMESTAMP '2025-05-14 13:45:59')").await,
            Some("2025-05-14 13:00:00".to_string())
        );
        // Unknown format → NULL (Spark), not an error.
        assert_eq!(
            eval_timestamp_iso("SELECT date_trunc('bogus', DATE '2025-05-14')").await,
            None
        );
    }

    /// Compiled Java-pattern rendering preserves the expected date-format output.
    #[test]
    fn compile_java_pattern_renders_dim_date_patterns() {
        let datetime = NaiveDateTime::parse_from_str("2025-01-08 13:05:09", "%Y-%m-%d %H:%M:%S")
            .expect("fixture");
        let render = |pattern: &str| {
            let tokens = compile_java_pattern(pattern).expect("compile");
            format_compiled_java_pattern(datetime, &tokens).expect("render")
        };
        assert_eq!(render("yyyyMMdd"), "20250108");
        // SQL writes `'yyyy''Q''q'` which unescapes to pattern yyyy'Q'q (literal Q).
        assert_eq!(render("yyyy'Q'q"), "2025Q1");
        assert_eq!(render("HH:mm:ss"), "13:05:09");
        assert_eq!(render("yyyy''MM"), "2025'01");
        assert_eq!(render("''"), "'");
        assert_eq!(render("yyyy-MM-dd'T'HH:mm:ss"), "2025-01-08T13:05:09");
    }

    /// Unterminated quote must fail at compile (same surface as the old per-row parser).
    #[test]
    fn compile_java_pattern_rejects_unterminated_quote() {
        let err = compile_java_pattern("yyyy'MM").expect_err("unterminated quote must Err");
        let message = err.to_string();
        assert!(
            message.contains("unterminated"),
            "expected unterminated diagnostic, got {message}"
        );
    }

    /// Optional release measurement, enabled with `REPARK_PERF_MEASURE=1`.
    #[test]
    #[allow(clippy::cast_precision_loss)] // ns/row report only
    fn perf_measure_date_format_compile_once() {
        if std::env::var_os("REPARK_PERF_MEASURE").as_deref() != Some(std::ffi::OsStr::new("1")) {
            eprintln!("PERF-02 skipped (set REPARK_PERF_MEASURE=1 to run 1M-row measurement)");
            return;
        }
        let rows = 1_000_000usize;
        let datetime = NaiveDateTime::parse_from_str("2025-01-08 13:05:09", "%Y-%m-%d %H:%M:%S")
            .expect("fixture");
        let pattern = "yyyy-MM-dd HH:mm:ss";
        let tokens = compile_java_pattern(pattern).expect("compile");
        let start = std::time::Instant::now();
        let mut sink = 0usize;
        for index in 0..rows {
            let out = format_compiled_java_pattern(datetime, &tokens).expect("render");
            sink ^= out.len().wrapping_add(index);
        }
        let elapsed = start.elapsed();
        let ns_compiled = elapsed.as_nanos() as f64 / rows as f64;
        eprintln!(
            "PERF-02 date_format_compiled rows={rows} total_ms={:.3} ns_per_row={ns_compiled:.3} sink={sink}",
            elapsed.as_secs_f64() * 1000.0
        );
        let start_recompile = std::time::Instant::now();
        let mut sink_recompile = 0usize;
        for index in 0..rows {
            let row_tokens = compile_java_pattern(pattern).expect("compile");
            let out = format_compiled_java_pattern(datetime, &row_tokens).expect("render");
            sink_recompile ^= out.len().wrapping_add(index);
        }
        let elapsed_recompile = start_recompile.elapsed();
        let ns_recompile = elapsed_recompile.as_nanos() as f64 / rows as f64;
        eprintln!(
            "PERF-02 date_format_recompile_each_row rows={rows} total_ms={:.3} ns_per_row={ns_recompile:.3} sink={sink_recompile}",
            elapsed_recompile.as_secs_f64() * 1000.0
        );
        let _ = (sink, sink_recompile, ns_compiled, ns_recompile);
    }

    /// Spark `date_format(ts, java_pattern)`.
    #[tokio::test]
    async fn date_format_matches_spark_on_the_dim_dates_patterns() {
        // 2025-01-08 is a Wednesday.
        assert_eq!(
            eval_string("SELECT date_format(DATE '2025-01-08', 'yyyyMMdd')").await,
            Some("20250108".to_string())
        );
        assert_eq!(
            eval_string("SELECT date_format(DATE '2025-01-08', 'yyyyMM')").await,
            Some("202501".to_string())
        );
        // Single-quoted 'Q' is a literal; q is the quarter number.
        assert_eq!(
            eval_string("SELECT date_format(DATE '2025-05-14', 'yyyy''Q''q')").await,
            Some("2025Q2".to_string())
        );
        assert_eq!(
            eval_string("SELECT date_format(DATE '2025-01-08', 'MMMM')").await,
            Some("January".to_string())
        );
        assert_eq!(
            eval_string("SELECT date_format(DATE '2025-01-08', 'MMM')").await,
            Some("Jan".to_string())
        );
        assert_eq!(
            eval_string("SELECT date_format(DATE '2025-01-08', 'EEEE')").await,
            Some("Wednesday".to_string())
        );
        assert_eq!(
            eval_string("SELECT date_format(DATE '2025-01-08', 'EEE')").await,
            Some("Wed".to_string())
        );
        // Time components come through when the input is a timestamp.
        assert_eq!(
            eval_string("SELECT date_format(TIMESTAMP '2025-01-08 13:05:09', 'HH:mm:ss')").await,
            Some("13:05:09".to_string())
        );
        // An unsupported pattern letter fails loudly rather than emitting a wrong string.
        assert!(
            ctx()
                .sql("SELECT date_format(DATE '2025-01-08', 'a')")
                .await
                .unwrap()
                .collect()
                .await
                .is_err()
        );
    }
}
