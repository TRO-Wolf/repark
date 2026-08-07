//! Logical-`Expr` builders for the Spark date functions the `DataFrame` facade wires up.
//!
//! The Python facade (`repark.functions`) builds column expressions through `repark-python`, which
//! must produce a self-contained [`Expr`] for each function *without* a `SessionContext` to resolve
//! names against (a `PyColumn` is standalone). These builders return an [`Expr`] that embeds the UDF
//! instance directly, so the resulting expression is valid on its own and is byte-for-byte the same
//! function the SQL path resolves — [`crate::register_all`] installs the same UDFs by name.
//!
//! The calendar extractors and the calendar-math shims come from [`crate::datetime`]; `date_add` and
//! `last_day` come from `datafusion-spark` (which this crate already registers). The Spark argument
//! order is preserved (notably `date_trunc(format, timestamp)` — format first).

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Cast, Expr, ScalarUDF};
use datafusion_spark::function::datetime::expr_fn as spark_datetime;

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
