//! Spark-semantics calendar date functions missing from `datafusion-spark`.
//!
//! `datafusion-spark` (52.x) ships `hour` / `minute` / `second` / `last_day` / `next_day` /
//! `date_add` / `date_sub` / `make_interval` / `make_dt_interval`, but its `hour`/`minute`/
//! `second` only accept **Timestamp** (not Spark `TimeType` / Arrow Time32/64). This module
//! re-ships those three via [`DatePartUdf`] (Time + Timestamp + Date + string) and also fills
//! the bare calendar extractors Spark SQL exposes (`year`, `month`, `dayofweek`, ...) — most
//! importantly:
//!
//! - `dayofweek` is **1-based on Sunday** in Spark (1=Sunday .. 7=Saturday), unlike arrow's
//!   `DatePart::DayOfWeekSunday0` which is 0-based; we add 1.
//! - `weekofyear` and `yearofweek` follow **ISO-8601** (week 1 contains the first Thursday), which
//!   is exactly arrow's `WeekISO` / `YearISO`. So `weekofyear('2021-01-01') = 53` and
//!   `yearofweek('2021-01-01') = 2020` even though `year('2021-01-01') = 2021`.
//!
//! Each extractor delegates to arrow's vectorized `date_part` kernel (which natively handles
//! `Date32` and `Timestamp` inputs) and applies a Spark indexing offset. `make_date` builds a
//! `Date32` from three integer columns, returning NULL for an invalid `(year, month, day)` — the
//! behaviour Spark gives with `spark.sql.ansi.enabled = false` (our default).

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, StringBuilder, TimestampMicrosecondArray,
};
use arrow::compute::{DatePart, cast, date_part};
use arrow::datatypes::{
    DataType, Date32Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
};
use chrono::{DateTime, Datelike, Days, NaiveDate, NaiveDateTime, Timelike};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Spark `date_trunc` returns a microsecond timestamp without a zone; this is the shim's output
/// unit for that function and the unit its input is coerced to before truncation.
const TIMESTAMP_UNIT: TimeUnit = TimeUnit::Microsecond;

/// ===========================================================================================
/// The Spark date functions this module contributes.
///
/// Returned as ready-to-register `ScalarUDF`s; `crate::register_all` installs these into a
/// `SessionContext` after `datafusion-spark`'s own set (so a name clash resolves in our favour).
/// `day` is registered as a Spark-compatible alias of `dayofmonth`.
/// ===========================================================================================
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
        // Overwrite datafusion-spark hour/minute/second so TimeType (X1 lit(time)) works
        // (octo C3 / Apache test_hour|minute|second).
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

/// The Spark calendar-field extractors, each exposed as a named constructor so the crate's
/// `expr_fn` builders can reference exactly one UDF instance (rather than searching the registry).
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

/// The single coerced argument type a [`DatePartUdf`] receives, given an input type. Spark applies
/// these functions to dates, timestamps (any unit/zone) and strings; we mirror that:
/// - temporal types (`Date32/64`, `Timestamp(_, _)`, `Time32/64`) pass straight to `date_part`;
/// - strings are coerced to `Date32` (Spark parses `'yyyy-MM-dd'`);
/// - a bare `NULL` literal is treated as a `Date32` null.
///
/// Returns `None` for an unsupported type so the caller can raise a clear error.
fn coerce_date_arg(arg: &DataType) -> Option<DataType> {
    match arg {
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time32(_)
        | DataType::Time64(_) => Some(arg.clone()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
            Some(DataType::Date32)
        }
        _ => None,
    }
}

/// ===========================================================================================
/// `DatePartUdf` — a calendar-field extractor backed by arrow's `date_part`.
///
/// One generic implementation covers every single-field Spark extractor; the per-function
/// difference is only `(part, spark_offset)`. `spark_offset` is added to each non-null result to
/// reconcile arrow's indexing with Spark's (non-zero only for `dayofweek`). Equality/Hash are
/// keyed on `name` alone — the name uniquely identifies the function, and arrow's `DatePart` is
/// not `Hash`, so we cannot derive them.
/// ===========================================================================================
#[derive(Debug)]
struct DatePartUdf {
    name: &'static str,
    part: DatePart,
    spark_offset: i32,
    signature: Signature,
}

impl DatePartUdf {
    fn new(name: &'static str, part: DatePart, spark_offset: i32) -> Self {
        // `user_defined` defers argument coercion to `coerce_types` below, so we can accept the full
        // Spark input range (date / timestamp-any / string) rather than a fixed type list.
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
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let extracted = date_part(arrays[0].as_ref(), self.part)?;
        // SAF-002: calendar kernels document Int32 output; defensive cast → typed Err on drift.
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

/// ===========================================================================================
/// `MakeDate` — Spark `make_date(year, month, day) -> DATE`.
///
/// Returns NULL when the three components do not form a valid calendar date (e.g. `(2023, 2, 29)`
/// or a negative month), matching Spark with ANSI mode off.
/// ===========================================================================================
#[derive(Debug)]
struct MakeDate {
    name: &'static str,
    signature: Signature,
}

impl MakeDate {
    fn new() -> Self {
        // Int64 is DataFusion's natural integer-literal type, and Int32/smaller integer columns
        // widen into it — so this one signature accepts `make_date(2024, 2, 29)` and integer
        // column args alike.
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
        // SAF-002: signature is uniform Int64×3; defensive cast so physical mismatch → typed Err.
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
            // Any component out of range (negative month, year beyond i32, Feb 30, ...) -> NULL,
            // matching Spark `make_date` with ANSI mode off.
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

/// Coerce a Spark date-function argument to `Date32` for the calendar-math shims (`add_months`,
/// `trunc`). Dates pass through; timestamps and strings coerce to `Date32` (Spark truncates a
/// timestamp to its date and parses `'yyyy-MM-dd'`); a bare `NULL` is a `Date32` null. Returns
/// `None` for a type Spark would reject, so the caller can raise a clear planning error.
fn coerce_to_date32(arg: &DataType) -> Option<DataType> {
    match arg {
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => Some(DataType::Date32),
        _ => None,
    }
}

/// Coerce a Spark date-function argument to a microsecond timestamp for `date_format` / `date_trunc`
/// (both need the time-of-day components). A `Date32` widens to midnight, a string parses, and a
/// zoned/other-unit timestamp reinterprets to the microsecond instant. `None` for an unsupported type.
fn coerce_to_timestamp_micros(arg: &DataType) -> Option<DataType> {
    match arg {
        DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => Some(DataType::Timestamp(TIMESTAMP_UNIT, None)),
        _ => None,
    }
}

/// A microsecond timestamp (µs since the Unix epoch, UTC) as a naive local-instant datetime.
/// `None` if the value is more than ~262 000 years from the common era.
fn datetime_from_micros(micros: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(micros).map(|instant| instant.naive_utc())
}

/// Number of days in `(year, month)` — computed as the day before the first of the next month, so
/// leap years fall out naturally. `None` only if the neighbouring first-of-month is unrepresentable.
fn days_in_month(year: i32, month: u32) -> Option<u32> {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let first_of_next = NaiveDate::from_ymd_opt(next_year, next_month, 1)?;
    Some(first_of_next.pred_opt()?.day())
}

/// ===========================================================================================
/// Spark `add_months(start, numMonths)` — end-of-month-preserving month arithmetic.
///
/// Spark clamps the day when the start is the last day of its month, OR when the start day does not
/// exist in the target month: `add_months('2015-01-31', 1) = '2015-02-28'` and
/// `add_months('2016-02-29', 12) = '2017-02-28'`. Any other day is carried across unchanged.
/// `numMonths` may be negative. Returns `None` (→ NULL) only for a target year outside the calendar.
/// ===========================================================================================
fn spark_add_months(date: NaiveDate, months: i32) -> Option<NaiveDate> {
    let source_month_index = date
        .year()
        .checked_mul(12)?
        .checked_add(i32::try_from(date.month0()).ok()?)?;
    let target_month_index = source_month_index.checked_add(months)?;
    let target_year = target_month_index.div_euclid(12);
    let target_month = u32::try_from(target_month_index.rem_euclid(12)).ok()? + 1;

    let last_day_of_source = days_in_month(date.year(), date.month())?;
    let last_day_of_target = days_in_month(target_year, target_month)?;
    let day = if date.day() == last_day_of_source || date.day() > last_day_of_target {
        last_day_of_target
    } else {
        date.day()
    };
    NaiveDate::from_ymd_opt(target_year, target_month, day)
}

/// The first day of the calendar week (Monday) containing `date` — Spark's `trunc`/`date_trunc`
/// `'WEEK'` anchor (ISO-8601, matching `weekofyear`).
fn start_of_week(date: NaiveDate) -> Option<NaiveDate> {
    let days_back = u64::from(date.weekday().num_days_from_monday());
    date.checked_sub_days(Days::new(days_back))
}

/// The first day of the quarter (Jan/Apr/Jul/Oct 1) containing `date`.
fn start_of_quarter(date: NaiveDate) -> Option<NaiveDate> {
    let first_month_of_quarter = date.month0() - (date.month0() % 3) + 1;
    NaiveDate::from_ymd_opt(date.year(), first_month_of_quarter, 1)
}

/// ===========================================================================================
/// Spark `trunc(date, format)` — truncate a DATE to `format`, returning a DATE.
///
/// Valid formats (case-insensitive) are exactly Spark's: `YEAR`/`YYYY`/`YY`, `MONTH`/`MON`/`MM`,
/// `WEEK`, `QUARTER`. Any other format string (e.g. `'Q'`, `'DAY'`) is invalid and yields `None`
/// (→ NULL), matching Spark — `trunc` has no day/hour granularity (that is `date_trunc`).
/// ===========================================================================================
fn trunc_date_to(date: NaiveDate, format: &str) -> Option<NaiveDate> {
    match format.to_ascii_uppercase().as_str() {
        "YEAR" | "YYYY" | "YY" => NaiveDate::from_ymd_opt(date.year(), 1, 1),
        "QUARTER" => start_of_quarter(date),
        "MONTH" | "MON" | "MM" => NaiveDate::from_ymd_opt(date.year(), date.month(), 1),
        "WEEK" => start_of_week(date),
        _ => None,
    }
}

/// ===========================================================================================
/// Spark `date_trunc(format, timestamp)` — truncate a TIMESTAMP to `format`, returning a TIMESTAMP.
///
/// Supports every Spark granularity (case-insensitive): `YEAR`/`YYYY`/`YY`, `QUARTER`,
/// `MONTH`/`MON`/`MM`, `WEEK`, `DAY`/`DD`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `MICROSECOND`.
/// An unknown format yields `None` (→ NULL), matching Spark.
/// ===========================================================================================
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

/// Render one run of `count` repeats of pattern letter `letter` against `datetime`, Spark
/// `date_format` semantics (a subset of Java `DateTimeFormatter`). Names use English (the Spark
/// default locale). Returns `Err` for a letter this shim does not implement — a clear execution
/// error beats a silently-wrong string.
fn render_pattern_field(letter: char, count: usize, datetime: NaiveDateTime) -> Result<String> {
    let unsupported = |letter: char| {
        Err(DataFusionError::Execution(format!(
            "date_format: unsupported pattern letter '{letter}'"
        )))
    };
    match letter {
        // `yy` is the last two digits; any other width is the full year zero-padded to `count`.
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

/// One token of a pre-compiled Java-style `date_format` pattern (r24 PERF-02).
///
/// Compiling once per invocation (the pattern is a scalar literal in every realistic call)
/// avoids re-parsing `pattern.chars().collect()` on every row.
#[derive(Clone, Debug)]
enum JavaPatternToken {
    /// Verbatim text (quoted runs + non-letter punctuation).
    Literal(String),
    /// A run of `count` identical ASCII pattern letters.
    Field { letter: char, count: usize },
}

/// Compile a Java-style `date_format` pattern into tokens. Single-quoted runs are literal text
/// (`''` is a literal apostrophe); ASCII letters are pattern fields; every other character is
/// emitted verbatim. Returns `Err` on an unterminated quote (same surface as the per-row path).
fn compile_java_pattern(pattern: &str) -> Result<Vec<JavaPatternToken>> {
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
        // Coalesce adjacent non-letter / non-quote punctuation into one literal token.
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

/// Render a pre-compiled pattern against `datetime`. Returns `Err` on an unsupported field.
fn format_compiled_java_pattern(
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

// (`shim_udf_boilerplate!` comes from `lib.rs` via textual macro scope.)

// === r20 A1: saf-datetime ===
// SAF-001: Date32 values outside chrono's `NaiveDate` range (≈ years −262143…+262142)
// previously panicked via `to_naive_date_opt(...).expect("valid date32")` in `add_months` /
// `trunc`. Map those rows → NULL (same class as `MakeDate` on invalid calendar triples).
// Live Spark 4.1.2 (ANSI off) computes proleptic extreme years for i32::MIN/MAX days and does
// not NULL them — we cannot match that without replacing chrono; the pin documents NULL + no
// panic, with an honest residual vs Spark's extreme-year arithmetic.
//
// SAF-002 (datetime sites): every `as_primitive`/`as_string` in this module sits after
// `coerce_types` (or a kernel that documents its output type). Documented per invoke site.

/// ===========================================================================================
/// `AddMonths` — Spark `add_months(start_date, num_months) -> DATE`.
/// ===========================================================================================
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
    shim_udf_boilerplate!("add_months");

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
        // Any integer (or NULL) num_months widens to Int32; a non-integer is a clear error.
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
        // SAF-002: after `coerce_types` → Date32 + Int32; defensive cast so a physical-type
        // mismatch becomes a typed engine error instead of `as_primitive` panic (string.rs
        // Utf8View lesson).
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let starts = cast(arrays[0].as_ref(), &DataType::Date32)?;
        let starts = starts.as_primitive::<Date32Type>();
        let months = cast(arrays[1].as_ref(), &DataType::Int32)?;
        let months = months.as_primitive::<Int32Type>();
        let mut builder = Date32Array::builder(starts.len());
        for row in 0..starts.len() {
            if starts.is_null(row) || months.is_null(row) {
                builder.append_null();
                continue;
            }
            // SAF-001: out-of-chrono Date32 → NULL (no panic).
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

/// ===========================================================================================
/// `TruncDate` — Spark `trunc(date, format) -> DATE`.
/// ===========================================================================================
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
    shim_udf_boilerplate!("trunc");

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
        // SAF-002: after `coerce_types` → Date32 + Utf8; defensive cast (Utf8View/LargeUtf8
        // must not panic via bare `as_string::<i32>`).
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let dates = cast(arrays[0].as_ref(), &DataType::Date32)?;
        let dates = dates.as_primitive::<Date32Type>();
        let formats = cast(arrays[1].as_ref(), &DataType::Utf8)?;
        let formats = formats.as_string::<i32>();
        let mut builder = Date32Array::builder(dates.len());
        for row in 0..dates.len() {
            if dates.is_null(row) || formats.is_null(row) {
                builder.append_null();
                continue;
            }
            // SAF-001: out-of-chrono Date32 → NULL (no panic).
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

/// ===========================================================================================
/// `DateTrunc` — Spark `date_trunc(format, timestamp) -> TIMESTAMP`.
///
/// Note Spark's argument order — the format is FIRST. This shim registers over DataFusion's native
/// `date_trunc` so the Spark ordering, granularity set, and microsecond output type all hold.
/// ===========================================================================================
#[derive(Debug)]
struct DateTrunc {
    signature: Signature,
}

impl DateTrunc {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
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
    shim_udf_boilerplate!("date_trunc");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TIMESTAMP_UNIT, None))
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
        // SAF-002: after `coerce_types` → Utf8 + Timestamp(Microsecond); defensive cast.
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let formats = cast(arrays[0].as_ref(), &DataType::Utf8)?;
        let formats = formats.as_string::<i32>();
        let timestamps = cast(
            arrays[1].as_ref(),
            &DataType::Timestamp(TIMESTAMP_UNIT, None),
        )?;
        let timestamps = timestamps.as_primitive::<TimestampMicrosecondType>();
        let mut builder = TimestampMicrosecondArray::builder(timestamps.len());
        for row in 0..timestamps.len() {
            if formats.is_null(row) || timestamps.is_null(row) {
                builder.append_null();
                continue;
            }
            let truncated = datetime_from_micros(timestamps.value(row))
                .and_then(|datetime| trunc_datetime_to(datetime, formats.value(row)));
            match truncated {
                Some(result) => builder.append_value(result.and_utc().timestamp_micros()),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// ===========================================================================================
/// `DateFormat` — Spark `date_format(timestamp, format) -> STRING`.
/// ===========================================================================================
#[derive(Debug)]
struct DateFormat {
    signature: Signature,
}

impl DateFormat {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
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
    shim_udf_boilerplate!("date_format");

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
        // SAF-002: after `coerce_types` → Timestamp(Microsecond) + Utf8; defensive cast.
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let timestamps = cast(
            arrays[0].as_ref(),
            &DataType::Timestamp(TIMESTAMP_UNIT, None),
        )?;
        let timestamps = timestamps.as_primitive::<TimestampMicrosecondType>();
        let formats = cast(arrays[1].as_ref(), &DataType::Utf8)?;
        let formats = formats.as_string::<i32>();
        // PERF-02: compile the Java pattern once per invocation when the format column is
        // constant (scalar literal → all equal after values_to_arrays). Per-row recompile only
        // when the format actually changes — zero behavior change vs per-row compile.
        let mut cached_pattern: Option<(String, Vec<JavaPatternToken>)> = None;
        let mut builder = StringBuilder::with_capacity(timestamps.len(), 0);
        for row in 0..timestamps.len() {
            if timestamps.is_null(row) || formats.is_null(row) {
                builder.append_null();
                continue;
            }
            let Some(datetime) = datetime_from_micros(timestamps.value(row)) else {
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

    // Golden values were computed independently with Python's ISO-8601 calendar
    // (`datetime.date.isocalendar`), which is the same basis Spark's date functions use.

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

    /// The headline Spark-semantics trap: `dayofweek` is 1=Sunday..7=Saturday, and `weekday` is
    /// 0=Monday..6=Sunday. Sweep a full week so an off-by-one or wrong anchor cannot pass.
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

    /// X1 lit(time) + Apache `test_hour|minute|second` — Time64 accepted (octo C3).
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
        // 2024-02-29 is valid (leap year); the round-trip year confirms the Date32 is correct.
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

    /// Spark applies these to strings and timestamps of any precision/zone, not just `DATE`. The
    /// `user_defined` signature + `coerce_types` must accept all of them (a fixed type list missed
    /// string args and non-microsecond timestamps — DataFusion's `TIMESTAMP` literal is nanosecond).
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
    /// Out-of-chrono Date32 values decode as `None` (SAF-001: production maps them to NULL).
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

    /// Decode column 0 of the single result row as an ISO timestamp string (µs → `yyyy-MM-dd HH:mm:ss`).
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

    /// SAF-001: extreme Date32 (`i32::MIN` / `i32::MAX` days) must not panic in `add_months` / `trunc`.
    ///
    /// Live Spark 4.1.2 (ANSI off, `date_from_unix_date` + CAST AS STRING oracle, 2026-08-03):
    /// - `i32::MIN` → non-null proleptic date string `-5877641-06-23`; `add_months(...,1)` non-null
    /// - `i32::MAX` → non-null `+5881580-07-11`; `trunc(...,'MM')` non-null
    /// Chrono's `NaiveDate` cannot represent those years (`to_naive_date_opt` → None), so repark
    /// maps the row to NULL (MakeDate-class safe path) rather than panic or invent a calendar.
    /// Value AND null-ness are pinned below; the Spark residual (computes vs NULL) is documented.
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
        // Year 1 / year 9999 are inside chrono and match Spark 4.1.2 (oracle 2026-08-03).
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

    /// SAF-002 / octo A1-C1-002: format args arriving as `LargeUtf8` must not panic
    /// (`as_string::<i32>` alone would). Defensive cast → `Utf8` then trunc.
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

    /// SAF-001 companion: calendar extractors on extreme Date32 must not panic (arrow kernel
    /// or our Int32 offset path). Null-ness is kernel-defined; we pin no-panic + type.
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
        // Epoch control is defined; extremes may be null or a computed year — either is fine
        // as long as we did not panic and NULL input stays NULL.
        assert!(!col.is_null(2), "year(epoch) must be non-null");
        assert_eq!(col.value(2), 1970);
        assert!(col.is_null(3), "year(NULL) stays NULL");
    }

    /// Spark `add_months` clamps to the last day when the start is month-end OR the day overflows the
    /// target month — the trap a naive "same day, N months later" gets wrong. Sweep both branches
    /// plus the sign of `num_months`.
    #[tokio::test]
    async fn add_months_matches_spark_end_of_month_semantics() {
        // Jan-31 is month-end → clamp to Feb-28 (2015 is not a leap year).
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

    /// Spark `trunc(date, fmt)` truncates a DATE. Sweep every valid granularity; an invalid format
    /// (Spark accepts `QUARTER`, not `'Q'`) must return NULL, not throw.
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

    /// Spark `date_trunc(fmt, ts)` truncates a TIMESTAMP (note the format-first argument order) and
    /// returns a microsecond timestamp. Sweep a date-granularity and a time-granularity plus the
    /// NULL-on-invalid-format path.
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

    /// PERF-02 compile+render helpers stay bit-identical to the prior per-row parser.
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
        // octo C2-Q-001: doubled apostrophe + punct coalesce (PERF-02 token split edges).
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

    /// r24 A3 PERF-02 measurement (release); not a correctness pin. Records compile-once vs
    /// recompile-per-row ns/row for the ledger (≥1M rows).
    ///
    /// Gated: set `REPARK_PERF_MEASURE=1` (octo C1-Q-004) — default suite must not pay 1M iters.
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

    /// Spark `date_format(ts, java_pattern)`. Covers the exact patterns the `silver_dim_jobs.py`
    /// dim-dates transform uses (numeric fields, a quoted literal, month/day names) plus an
    /// unsupported pattern letter, which must raise rather than emit a wrong string.
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
        // Single-quoted 'Q' is a literal; q is the quarter number. 2025-05-14 is Q2.
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
