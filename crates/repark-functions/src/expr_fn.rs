//! Logical-`Expr` builders for the Spark date functions the `DataFrame` facade wires up.
//!
//! The Python facade (`repark.functions`) builds column expressions through `repark-python`, which
//! must produce a self-contained [`Expr`] for each function *without* a `SessionContext` to resolve
//! names against (a `PyColumn` is standalone). These builders return an [`Expr`] that embeds the UDF
//! instance directly, so the resulting expression is valid on its own and is byte-for-byte the same
//! function the SQL path resolves — [`crate::register_all`] installs the same UDFs by name.
//!
//! The calendar extractors and the calendar-math shims come from [`crate::datetime`]; `to_date`
//! comes from [`crate::timestamp_cast`] (TZ-8); `date_add` and `last_day` come from
//! `datafusion-spark` (which this crate already registers). The Spark argument
//! order is preserved (notably `date_trunc(format, timestamp)` — format first).

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Cast, Expr, ScalarUDF};
use datafusion_spark::function::bitmap::expr_fn as spark_bitmap;
use datafusion_spark::function::bitwise::expr_fn as spark_bitwise;
use datafusion_spark::function::datetime::expr_fn as spark_datetime;
use datafusion_spark::function::math::expr_fn as spark_math;
use datafusion_spark::function::string::expr_fn as spark_string;
use datafusion_spark::function::url as spark_url_udfs;

use crate::datetime;

/// Wrap `udf` applied to `args` as a scalar-function [`Expr`].
fn call(udf: Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, args))
}

/// Spark `year(date)` — the calendar year (e.g. `2021`).
#[must_use]
pub fn year(arg: Expr) -> Expr {
    call(datetime::year_udf(), vec![arg])
}

/// Spark `month(date)` — the month of year, `1`..=`12`.
#[must_use]
pub fn month(arg: Expr) -> Expr {
    call(datetime::month_udf(), vec![arg])
}

/// Spark `quarter(date)` — the quarter of year, `1`..=`4`.
#[must_use]
pub fn quarter(arg: Expr) -> Expr {
    call(datetime::quarter_udf(), vec![arg])
}

/// Spark `weekofyear(date)` — the ISO-8601 week number, `1`..=`53`.
#[must_use]
pub fn weekofyear(arg: Expr) -> Expr {
    call(datetime::weekofyear_udf(), vec![arg])
}

/// Spark `dayofweek(date)` — `1`=Sunday .. `7`=Saturday (Spark's 1-based-on-Sunday indexing).
#[must_use]
pub fn dayofweek(arg: Expr) -> Expr {
    call(datetime::dayofweek_udf(), vec![arg])
}

/// Spark `weekday(date)` — `0`=Monday .. `6`=Sunday (Spark's 0-based-on-Monday indexing).
#[must_use]
pub fn weekday(arg: Expr) -> Expr {
    call(datetime::weekday_udf(), vec![arg])
}

/// Spark `dayofmonth(date)` — the day of month, `1`..=`31`.
#[must_use]
pub fn dayofmonth(arg: Expr) -> Expr {
    call(datetime::dayofmonth_udf(), vec![arg])
}

/// Spark `dayofyear(date)` — the day of year, `1`..=`366`.
#[must_use]
pub fn dayofyear(arg: Expr) -> Expr {
    call(datetime::dayofyear_udf(), vec![arg])
}

/// Spark `add_months(start, num_months)` — end-of-month-preserving month arithmetic → DATE.
#[must_use]
pub fn add_months(start: Expr, num_months: Expr) -> Expr {
    call(datetime::add_months_udf(), vec![start, num_months])
}

/// Spark `date_format(timestamp, format)` — format a date/timestamp with a Java pattern → STRING.
#[must_use]
pub fn date_format(timestamp: Expr, format: Expr) -> Expr {
    call(datetime::date_format_udf(), vec![timestamp, format])
}

/// Spark `trunc(date, format)` — truncate a DATE to `format` (year/month/week/quarter) → DATE.
#[must_use]
pub fn trunc(date: Expr, format: Expr) -> Expr {
    call(datetime::trunc_udf(), vec![date, format])
}

/// Spark `date_trunc(format, timestamp)` — truncate a TIMESTAMP to `format` → TIMESTAMP.
///
/// The Spark argument order (format first) is preserved.
#[must_use]
pub fn date_trunc(format: Expr, timestamp: Expr) -> Expr {
    call(datetime::date_trunc_udf(), vec![format, timestamp])
}

/// Spark `date_add(start, num_days)` — the date `num_days` after `start` → DATE (from `datafusion-spark`).
///
/// `num_days` is cast to `Int32`: Spark's `date_add` day count is a 32-bit integer, and the
/// `datafusion-spark` overload only accepts `Int8`/`Int16`/`Int32`, whereas a bare integer literal
/// (`lit(1)`) is `Int64`. The cast keeps the widened literal (and any integer column) acceptable.
#[must_use]
pub fn date_add(start: Expr, num_days: Expr) -> Expr {
    let num_days = Expr::Cast(Cast::new(Box::new(num_days), DataType::Int32));
    spark_datetime::date_add(start, num_days)
}

/// Spark `unix_date(date)` — days since 1970-01-01 → INT (from `datafusion-spark`).
///
/// The Python facade spelled this `CAST(CAST(x AS DATE) AS INT)` until the G6-3 cast-legality
/// gate landed, which refuses exactly that type pair — as Spark does. The engine's own
/// `unix_date` is the correct builder and always was: `SparkUnixDate::simplify` lowers to the
/// same two casts, but in the OPTIMIZER, one stage after the analyzer gate. That ordering is why
/// the gate lives at analysis (`planning/hardening/G63-DATE-INT-DESIGN.md` §3.4) and why the
/// remedy Spark's own error message names keeps working.
#[must_use]
pub fn unix_date(date: Expr) -> Expr {
    spark_datetime::unix_date(date)
}

/// Spark `last_day(date)` — the last day of the month containing `date` → DATE (from `datafusion-spark`).
#[must_use]
pub fn last_day(date: Expr) -> Expr {
    spark_datetime::last_day(date)
}

/// Spark `next_day(date, dayOfWeek)` → DATE (from `datafusion-spark`).
#[must_use]
pub fn next_day(date: Expr, day_of_week: Expr) -> Expr {
    spark_datetime::next_day(date, day_of_week)
}

/// Spark `hour(timestamp|time)` — repark `DatePartUdf` (Time + Timestamp; overwrites DF-spark).
#[must_use]
pub fn hour(arg: Expr) -> Expr {
    call(datetime::hour_udf(), vec![arg])
}

/// Spark `minute(timestamp|time)` — repark `DatePartUdf` (Time + Timestamp).
#[must_use]
pub fn minute(arg: Expr) -> Expr {
    call(datetime::minute_udf(), vec![arg])
}

/// Spark `second(timestamp|time)` — repark `DatePartUdf` (Time + Timestamp).
#[must_use]
pub fn second(arg: Expr) -> Expr {
    call(datetime::second_udf(), vec![arg])
}

/// Spark `to_date(ts|date|string)` — TZ-8 session-zone date for an LTZ timestamp.
#[must_use]
pub fn to_date(arg: Expr) -> Expr {
    call(crate::timestamp_cast::to_date_udf(), vec![arg])
}

// ===========================================================================================
// FNP-3 — facade-embed builders for `datafusion-spark` kernels the SQL door already resolved.
// ===========================================================================================
//
// Each of these names worked through `spark.sql(...)` and raised
// `UnsupportedOperationException` through the facade, because `register_all` installs the kernel
// by name while the facade's dispatch table had no arm for it. Every builder below embeds the
// SAME singleton the registry installs (`make_udf_function!` hands out one instance), so the two
// doors cannot diverge — charter clause C-012.

/// Spark `crc32(expr)` — CRC-32 checksum as a bigint.
#[must_use]
pub fn crc32(arg: Expr) -> Expr {
    call(datafusion_spark::function::hash::crc32(), vec![arg])
}

/// Spark `sha1(expr)` — SHA-1 as a lowercase hex string. Also serves the `sha` spelling.
#[must_use]
pub fn sha1(arg: Expr) -> Expr {
    call(datafusion_spark::function::hash::sha1(), vec![arg])
}

/// Spark `xxhash64(expr, ...)` — 64-bit xxHash of the arguments.
#[must_use]
pub fn xxhash64(args: Vec<Expr>) -> Expr {
    call(datafusion_spark::function::hash::xxhash64(), args)
}

/// Spark `soundex(expr)` — the four-character Soundex code.
#[must_use]
pub fn soundex(arg: Expr) -> Expr {
    call(datafusion_spark::function::string::soundex(), vec![arg])
}

/// Spark `format_string(fmt, ...)` — printf-style formatting.
#[must_use]
pub fn format_string(args: Vec<Expr>) -> Expr {
    call(datafusion_spark::function::string::format_string(), args)
}

/// Spark `from_utc_timestamp(ts, tz)` — render a UTC instant in `tz`.
#[must_use]
pub fn from_utc_timestamp(timestamp: Expr, timezone: Expr) -> Expr {
    call(
        datafusion_spark::function::datetime::from_utc_timestamp(),
        vec![timestamp, timezone],
    )
}

/// Spark `to_utc_timestamp(ts, tz)` — read a wall clock in `tz` as a UTC instant.
#[must_use]
pub fn to_utc_timestamp(timestamp: Expr, timezone: Expr) -> Expr {
    call(
        datafusion_spark::function::datetime::to_utc_timestamp(),
        vec![timestamp, timezone],
    )
}

/// Spark `map_from_arrays(keys, values)` — a map from two equal-length arrays.
#[must_use]
pub fn map_from_arrays(keys: Expr, values: Expr) -> Expr {
    call(
        datafusion_spark::function::map::map_from_arrays(),
        vec![keys, values],
    )
}

/// Spark `to_timestamp(expr[, format])` — TZ-4 LTZ instant, session-zone localized.
///
/// Variadic to match the kernel's own signature and the facade's arity gate.
#[must_use]
pub fn to_timestamp(args: Vec<Expr>) -> Expr {
    call(crate::instant_ts::to_timestamp_udf(), args)
}

/// Spark `bin(expr)` — binary string of a long (from `datafusion-spark`).
#[must_use]
pub fn bin(arg: Expr) -> Expr {
    spark_math::bin(arg)
}

/// Spark `hex(expr)` — hex string of a number, string, or binary (from `datafusion-spark`).
#[must_use]
pub fn hex(arg: Expr) -> Expr {
    spark_math::hex(arg)
}

/// Spark `unhex(expr)` — hex string to binary (from `datafusion-spark`).
#[must_use]
pub fn unhex(arg: Expr) -> Expr {
    spark_math::unhex(arg)
}

/// Spark `factorial(expr)` — `n!` for `n` in `[0, 20]`, else NULL (from `datafusion-spark`).
///
/// The kernel is `Int32`-exact; a facade `lit(5)` is `Int64`, so we cast like `date_add`.
#[must_use]
pub fn factorial(arg: Expr) -> Expr {
    let arg = Expr::Cast(Cast::new(Box::new(arg), DataType::Int32));
    spark_math::factorial(arg)
}

/// Spark `rint(expr)` — nearest integer as a double (from `datafusion-spark`).
#[must_use]
pub fn rint(arg: Expr) -> Expr {
    spark_math::rint(arg)
}

/// Spark `width_bucket(value, min, max, numBucket)` (from `datafusion-spark`).
#[must_use]
pub fn width_bucket(value: Expr, min: Expr, max: Expr, num_bucket: Expr) -> Expr {
    spark_math::width_bucket(value, min, max, num_bucket)
}

/// Spark `bit_count(expr)` — population count (from `datafusion-spark`).
#[must_use]
pub fn bit_count(arg: Expr) -> Expr {
    spark_bitwise::bit_count(arg)
}

/// Spark `bit_get(expr, pos)` / `getbit` — bit at 0-based position from the right.
#[must_use]
pub fn bit_get(value: Expr, pos: Expr) -> Expr {
    spark_bitwise::bit_get(value, pos)
}

/// Spark `shiftleft(expr, n)` (from `datafusion-spark`).
#[must_use]
pub fn shiftleft(value: Expr, shift: Expr) -> Expr {
    spark_bitwise::shiftleft(value, shift)
}

/// Spark `shiftright(expr, n)` — arithmetic/signed (from `datafusion-spark`).
#[must_use]
pub fn shiftright(value: Expr, shift: Expr) -> Expr {
    spark_bitwise::shiftright(value, shift)
}

/// Spark `shiftrightunsigned(expr, n)` — logical/unsigned (from `datafusion-spark`).
#[must_use]
pub fn shiftrightunsigned(value: Expr, shift: Expr) -> Expr {
    spark_bitwise::shiftrightunsigned(value, shift)
}

/// Spark `split_part(src, delimiter, partNum)` — STRING `partNum` casts (F-6c).
#[must_use]
pub fn split_part(src: Expr, delimiter: Expr, part_num: Expr) -> Expr {
    call(
        crate::spark_split_part::split_part_udf(),
        vec![src, delimiter, part_num],
    )
}

/// Spark `regexp_count(str, regexp)` — NULL-in NULL-out, INT (P1 / A2).
#[must_use]
pub fn regexp_count(str: Expr, regexp: Expr) -> Expr {
    call(crate::spark_regexp::regexp_count_udf(), vec![str, regexp])
}

/// Spark `regexp_instr(str, regexp[, idx])` — ignore idx value; first-match start.
#[must_use]
pub fn regexp_instr(args: Vec<Expr>) -> Expr {
    call(crate::spark_regexp::regexp_instr_udf(), args)
}

/// Spark `bit_length(expr)` — byte-length × 8; stringifies non-binary (G5).
#[must_use]
pub fn bit_length(arg: Expr) -> Expr {
    call(crate::spark_length::bit_length_udf(), vec![arg])
}

/// Spark `octet_length(expr)` — UTF-8 / binary byte length; stringifies non-binary (G5).
#[must_use]
pub fn octet_length(arg: Expr) -> Expr {
    call(crate::spark_length::octet_length_udf(), vec![arg])
}

/// Spark `is_valid_utf8(expr)` (from `datafusion-spark`).
#[must_use]
pub fn is_valid_utf8(arg: Expr) -> Expr {
    spark_string::is_valid_utf8(arg)
}

/// Spark `make_valid_utf8(expr)` — replace invalid UTF-8 with U+FFFD.
#[must_use]
pub fn make_valid_utf8(arg: Expr) -> Expr {
    spark_string::make_valid_utf8(arg)
}

/// Spark `make_date(year, month, day)` — repark registered UDF (not a CAST chain).
#[must_use]
pub fn make_date(year: Expr, month: Expr, day: Expr) -> Expr {
    call(datetime::make_date_udf(), vec![year, month, day])
}

/// Spark `make_interval(...)` — 0..=7 args (years…secs); kernel accepts each arity.
#[must_use]
pub fn make_interval(args: Vec<Expr>) -> Expr {
    datafusion_spark::function::datetime::make_interval().call(args)
}

/// Spark `make_dt_interval(...)` — 0..=4 args (days, hours, mins, secs).
#[must_use]
pub fn make_dt_interval(args: Vec<Expr>) -> Expr {
    datafusion_spark::function::datetime::make_dt_interval().call(args)
}

/// Spark `unix_micros(ts)` — microseconds since epoch (from `datafusion-spark`).
#[must_use]
pub fn unix_micros(arg: Expr) -> Expr {
    spark_datetime::unix_micros(arg)
}

/// Spark `date_diff(end, start)` — days from `start` to `end` (from `datafusion-spark`).
#[must_use]
pub fn date_diff(end: Expr, start: Expr) -> Expr {
    spark_datetime::date_diff(end, start)
}

/// Spark `element_at(container, key)` — 1-based array / map-by-key (repark shim).
#[must_use]
pub fn element_at(container: Expr, key: Expr) -> Expr {
    call(crate::collection::element_at_udf(), vec![container, key])
}

/// Spark `shuffle(array[, seed])` — random permutation (X1 NULL-guarded shim).
///
/// `args` is the array alone, or the array plus the Spark 4.0 `Int64` seed. Both doors resolve
/// the same UDF, so `F.shuffle(col, seed)` and `shuffle(col, seed)` in SQL are one permutation.
#[must_use]
pub fn shuffle(args: Vec<Expr>) -> Expr {
    call(crate::collection::shuffle_udf(), args)
}

/// Spark `map_from_entries(array<struct<key,value>>)` — duplicate keys raise (X7 shim).
#[must_use]
pub fn map_from_entries(arg: Expr) -> Expr {
    call(crate::collection::map_from_entries_udf(), vec![arg])
}

/// Spark `str_to_map(text, pairDelim, keyValueDelim)` — regex delimiters (owned shim).
#[must_use]
pub fn str_to_map(text: Expr, pair_delim: Expr, key_value_delim: Expr) -> Expr {
    call(
        crate::collection::str_to_map_udf(),
        vec![text, pair_delim, key_value_delim],
    )
}

/// Spark `parse_url` — 2 or 3 args, on `java.net.URI` splitting (X8). An unparsable URL
/// raises Spark's `INVALID_URL` on both doors.
#[must_use]
pub fn parse_url(args: Vec<Expr>) -> Expr {
    call(crate::url::parse_url_udf(), args)
}

/// Spark `try_parse_url` — 2 or 3 args; NULL on invalid URL (X8 shim).
#[must_use]
pub fn try_parse_url(args: Vec<Expr>) -> Expr {
    call(crate::url::try_parse_url_udf(), args)
}

/// Spark `url_decode(str)` (from `datafusion-spark`).
#[must_use]
pub fn url_decode(arg: Expr) -> Expr {
    spark_url_udfs::url_decode().call(vec![arg])
}

/// Spark `url_encode(str)` (from `datafusion-spark`).
#[must_use]
pub fn url_encode(arg: Expr) -> Expr {
    spark_url_udfs::url_encode().call(vec![arg])
}

/// Spark `try_url_decode(str)` — NULL on invalid encoding.
#[must_use]
pub fn try_url_decode(arg: Expr) -> Expr {
    spark_url_udfs::try_url_decode().call(vec![arg])
}

/// Spark `bitmap_bit_position(n)` (from `datafusion-spark`).
#[must_use]
pub fn bitmap_bit_position(arg: Expr) -> Expr {
    spark_bitmap::bitmap_bit_position(arg)
}

/// Spark `bitmap_bucket_number(n)` (from `datafusion-spark`).
#[must_use]
pub fn bitmap_bucket_number(arg: Expr) -> Expr {
    spark_bitmap::bitmap_bucket_number(arg)
}

/// Spark `bitmap_count(bitmap)` — popcount of a binary bitmap.
#[must_use]
pub fn bitmap_count(arg: Expr) -> Expr {
    spark_bitmap::bitmap_count(arg)
}
