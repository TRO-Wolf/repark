"""Datetime facade wrappers (FN-D).

Public names are re-exported from ``functions.py``. Helpers stay imported from
``functions`` / ``functions_expr`` (they serve these wrappers). No new
``call_scalar`` arms — those are a ``crates/`` edit, closed this batch.
"""

from __future__ import annotations

import datetime

from repark.spark.column import Column
from repark.spark.functions import (
    _as_column_arg,
    current_timestamp,
    date_add,
    dayofmonth,
    lit,
)
from repark.spark.functions import (
    abs as abs_column,
)
from repark.spark.functions_expr import current_date, date_part, floor, sign, unix_timestamp

# Epoch day 0 as a foldable DATE literal (Spark ``date_from_unix_date(0)``).
_UNIX_EPOCH_DATE = datetime.date(1970, 1, 1)


def day(col: Column | str) -> Column:
    """Day of month (PySpark ``functions.day``; alias of ``dayofmonth``)."""
    return dayofmonth(col)


def curdate() -> Column:
    """Current date (PySpark ``functions.curdate``; alias of ``current_date``)."""
    return current_date()


def now() -> Column:
    """Current timestamp (PySpark ``functions.now``; alias of ``current_timestamp``)."""
    return current_timestamp()


def dateadd(start: Column | str, days: Column | int | str) -> Column:
    """Add days (PySpark ``functions.dateadd``; alias of ``date_add``)."""
    return date_add(start, days)


def datepart(field: str, source: Column | str) -> Column:
    """Extract a calendar field (PySpark ``functions.datepart``; alias of ``date_part``)."""
    return date_part(field, source)


def to_unix_timestamp(
    timestamp: Column | str | None = None,
    format: str | None = None,
) -> Column:
    """Alias of :func:`unix_timestamp` (PySpark ``functions.to_unix_timestamp``).

    The existing ``unix_timestamp`` surface is the R-FN-BATCH1 loud gap; this name
    must not grow a second meaning.
    """
    return unix_timestamp(timestamp, format)


def _truncate_toward_zero(column: Column) -> Column:
    """Integer part of ``column`` toward zero (Spark ``unix_*`` fractional truncate).

    ``CAST(ts AS BIGINT)`` floors (TZ-5: ``-1.5 → -2``). Spark ``unix_seconds`` /
    ``unix_millis`` truncate toward zero (``-1.5 → -1``).
    """
    return (floor(abs_column(column)) * sign(column)).cast("long")


def unix_date(col: Column | str) -> Column:
    """Days since 1970-01-01 (PySpark ``functions.unix_date``).

    Matches datafusion-spark ``SparkUnixDate``: ``CAST(date AS INT)``.
    """
    return _as_column_arg(col, as_lit=False).cast("date").cast("int")


def unix_seconds(col: Column | str) -> Column:
    """Seconds since 1970-01-01 UTC, truncating sub-seconds (PySpark ``unix_seconds``).

    Hazard: ``CAST(ts AS BIGINT)`` floors (TZ-5). Spark ``unix_seconds`` truncates
    toward zero, so negatives use ``floor(abs(seconds)) * sign(seconds)``.
    """
    seconds = _as_column_arg(col, as_lit=False).cast("double")
    return _truncate_toward_zero(seconds)


def unix_millis(col: Column | str) -> Column:
    """Milliseconds since 1970-01-01 UTC, truncating micros (PySpark ``unix_millis``).

    Same toward-zero rule as :func:`unix_seconds` (not TZ-5 floor).
    """
    millis = _as_column_arg(col, as_lit=False).cast("double") * lit(1000)
    return _truncate_toward_zero(millis)


def date_from_unix_date(n: Column | int | str) -> Column:
    """Days-since-epoch → date (PySpark ``functions.date_from_unix_date``).

    ``date_add(DATE '1970-01-01', n)``. SQL ``date_from_unix_date`` is not registered.
    """
    return date_add(lit(_UNIX_EPOCH_DATE), n)


def current_timezone() -> Column:
    """Session ``spark.sql.session.timeZone`` as a foldable string.

    Session-only — never an environment read. Repark defaults to ``UTC`` (not the
    host zone). Runtime ``conf.set`` does not move the engine zone, so baking the
    live session value at construction is honest.
    """
    from repark.spark.session.session_time_zone import active_session_time_zone

    return lit(active_session_time_zone())
