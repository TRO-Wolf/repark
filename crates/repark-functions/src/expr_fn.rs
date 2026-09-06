//! Logical [`Expr`] builders for Spark functions used by the standalone facade.
//!
//! Each builder embeds the same UDF registered for SQL, preserving Spark argument order.

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
/// Use the engine's `unix_date` builder because Spark rejects DATE-to-integer casts at analysis.
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

#[must_use]
pub fn date(arg: Expr) -> Expr {
    call(crate::timestamp_cast::date_udf(), vec![arg])
}

#[must_use]
pub fn unix_timestamp(args: Vec<Expr>) -> Expr {
    call(crate::timestamp_cast::unix_timestamp_udf(), args)
}
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

#[must_use]
pub fn sha2(arg: Expr, bit_length: Expr) -> Expr {
    call(
        datafusion_spark::function::hash::sha2(),
        vec![arg, bit_length],
    )
}

#[must_use]
pub fn isnan(arg: Expr) -> Expr {
    call(crate::spark_isnan::isnan_udf(), vec![arg])
}

#[must_use]
pub fn initcap(arg: Expr) -> Expr {
    call(crate::spark_initcap::initcap_udf(), vec![arg])
}

#[must_use]
pub fn chr(arg: Expr) -> Expr {
    call(crate::spark_chr::chr_udf(), vec![arg])
}

#[must_use]
pub fn elt(args: Vec<Expr>) -> Expr {
    call(crate::spark_elt::elt_udf(), args)
}

#[must_use]
pub fn regexp_like(str: Expr, regexp: Expr) -> Expr {
    call(
        crate::spark_regexp_match::regexp_like_udf(),
        vec![str, regexp],
    )
}

#[must_use]
pub fn rlike(str: Expr, regexp: Expr) -> Expr {
    call(crate::spark_regexp_match::rlike_udf(), vec![str, regexp])
}

#[must_use]
pub fn regexp_replace(str: Expr, regexp: Expr, replacement: Expr) -> Expr {
    call(
        crate::spark_regexp_match::regexp_replace_udf(),
        vec![str, regexp, replacement],
    )
}

#[must_use]
pub fn array_position(array: Expr, value: Expr) -> Expr {
    call(crate::collection::array_position_udf(), vec![array, value])
}

#[must_use]
pub fn array_sort(args: Vec<Expr>) -> Expr {
    call(crate::collection::array_sort_udf(), args)
}

#[must_use]
pub fn sort_array(args: Vec<Expr>) -> Expr {
    call(crate::collection::sort_array_udf(), args)
}

#[must_use]
pub fn arrays_overlap(left: Expr, right: Expr) -> Expr {
    call(crate::collection::arrays_overlap_udf(), vec![left, right])
}

#[must_use]
pub fn flatten(array: Expr) -> Expr {
    call(crate::collection::flatten_udf(), vec![array])
}

#[must_use]
pub fn percentile_approx(args: Vec<Expr>) -> Expr {
    crate::percentile_approx::percentile_approx_udaf().call(args)
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

/// Spark `validate_utf8(expr)` — the input when it is valid UTF-8, an error otherwise.
#[must_use]
pub fn validate_utf8(arg: Expr) -> Expr {
    call(crate::validate::validate_utf8_udf(), vec![arg])
}

/// Spark `try_validate_utf8(expr)` — the input when it is valid UTF-8, NULL otherwise.
#[must_use]
pub fn try_validate_utf8(arg: Expr) -> Expr {
    call(crate::validate::try_validate_utf8_udf(), vec![arg])
}

/// Spark `assert_true(condition[, message])` — NULL when true, an error otherwise.
#[must_use]
pub fn assert_true(args: Vec<Expr>) -> Expr {
    call(crate::validate::assert_true_udf(), args)
}

/// Spark `randstr(length[, seed])` — a random alphanumeric string.
#[must_use]
pub fn randstr(args: Vec<Expr>) -> Expr {
    call(crate::random::spark_randstr_udf(), args)
}

/// Spark `uniform(min, max[, seed])` — i.i.d. values in `[min, max)`.
#[must_use]
pub fn uniform(args: Vec<Expr>) -> Expr {
    call(crate::random::spark_uniform_udf(), args)
}

/// Spark `regexp_extract_all(str, regexp[, idx])` — every match's `idx`-th group, as an array.
#[must_use]
pub fn regexp_extract_all(args: Vec<Expr>) -> Expr {
    call(crate::spark_regexp::regexp_extract_all_udf(), args)
}

/// Spark `regexp_substr(str, regexp)` — the first match, NULL when there is none.
#[must_use]
pub fn regexp_substr(str: Expr, regexp: Expr) -> Expr {
    call(crate::spark_regexp::regexp_substr_udf(), vec![str, regexp])
}

#[must_use]
pub fn regexp_extract(args: Vec<Expr>) -> Expr {
    call(crate::spark_regexp::regexp_extract_udf(), args)
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

/// Spark `try_divide(left, right)` — NULL on divide-by-zero or overflow.
#[must_use]
pub fn try_divide(left: Expr, right: Expr) -> Expr {
    call(crate::try_invert::try_divide_udf(), vec![left, right])
}

/// Spark `try_mod(left, right)` — NULL on remainder-by-zero.
#[must_use]
pub fn try_mod(left: Expr, right: Expr) -> Expr {
    call(crate::try_invert::try_mod_udf(), vec![left, right])
}

/// Spark `try_add(left, right)` — NULL on overflow.
#[must_use]
pub fn try_add(left: Expr, right: Expr) -> Expr {
    call(crate::try_invert::try_add_udf(), vec![left, right])
}

/// Spark `try_subtract(left, right)` — NULL on overflow.
#[must_use]
pub fn try_subtract(left: Expr, right: Expr) -> Expr {
    call(crate::try_invert::try_subtract_udf(), vec![left, right])
}

/// Spark `try_multiply(left, right)` — NULL on overflow.
#[must_use]
pub fn try_multiply(left: Expr, right: Expr) -> Expr {
    call(crate::try_invert::try_multiply_udf(), vec![left, right])
}

/// Spark `try_element_at(container, key)` — alias of `element_at` (NULL on OOB / missing key).
#[must_use]
pub fn try_element_at(container: Expr, key: Expr) -> Expr {
    element_at(container, key)
}

/// Spark `try_to_date(expr[, format])` — NULL on parse failure.
#[must_use]
pub fn try_to_date(args: Vec<Expr>) -> Expr {
    call(crate::try_invert::try_to_date_udf(), args)
}

/// Spark `try_to_number(expr, format)` — NULL on value/format mismatch.
#[must_use]
pub fn try_to_number(expr: Expr, format: Expr) -> Expr {
    call(crate::try_invert::try_to_number_udf(), vec![expr, format])
}

/// Spark `try_to_binary(expr[, format])` — NULL on decode failure. Default format is hex.
#[must_use]
pub fn try_to_binary(args: Vec<Expr>) -> Expr {
    call(crate::try_invert::try_to_binary_udf(), args)
}

/// Spark `try_to_time` — matches Spark 4.1.2 `UNSUPPORTED_TIME_TYPE`.
#[must_use]
pub fn try_to_time(args: Vec<Expr>) -> Expr {
    call(crate::try_invert::try_to_time_udf(), args)
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

/// Spark `log(expr)` (natural) or `log(base, expr)`.
#[must_use]
pub fn log(args: Vec<Expr>) -> Expr {
    call(crate::spark_log::spark_log_udf(), args)
}

#[must_use]
pub fn log1p(arg: Expr) -> Expr {
    call(crate::spark_log1p::log1p_udf(), vec![arg])
}

#[must_use]
pub fn expm1(arg: Expr) -> Expr {
    call(crate::spark_log1p::expm1_udf(), vec![arg])
}

#[must_use]
pub fn from_unixtime(args: Vec<Expr>) -> Expr {
    call(crate::spark_from_unixtime::from_unixtime_udf(), args)
}

#[must_use]
pub fn get_json_object(json: Expr, path: Expr) -> Expr {
    call(crate::json::get_json_object_udf(), vec![json, path])
}

#[must_use]
pub fn json_array_length(json: Expr) -> Expr {
    call(crate::json::json_array_length_udf(), vec![json])
}

#[must_use]
pub fn json_object_keys(json: Expr) -> Expr {
    call(crate::json::json_object_keys_udf(), vec![json])
}

#[must_use]
pub fn schema_of_json(args: Vec<Expr>) -> Expr {
    call(crate::json::schema_of_json_udf(), args)
}

#[must_use]
pub fn to_json(args: Vec<Expr>) -> Expr {
    call(crate::json::to_json_udf(), args)
}

#[must_use]
pub fn from_json(args: Vec<Expr>) -> Expr {
    call(crate::json::from_json_udf(), args)
}

#[must_use]
pub fn array_insert(array: Expr, position: Expr, value: Expr) -> Expr {
    call(
        crate::collection::array_insert_udf(),
        vec![array, position, value],
    )
}

#[must_use]
pub fn arrays_zip(args: Vec<Expr>) -> Expr {
    call(crate::collection::arrays_zip_udf(), args)
}

#[must_use]
pub fn map_concat(args: Vec<Expr>) -> Expr {
    call(crate::collection::map_concat_udf(), args)
}
