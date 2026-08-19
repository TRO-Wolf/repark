"""Datetime facade wrappers (FN-D + FN-GT2).

Public names are re-exported from ``functions.py``. FN-GT2 wires leftover
``make_date`` / interval constructors / ``unix_micros`` / ``date_diff``.
``datediff`` stays the R-FN-BATCH1 DISPOSED-STUB — do not alias it onto
``date_diff``.
"""

from __future__ import annotations

import datetime

from repark.spark.column import Column
from repark.spark.functions import (
    _as_column_arg,
    _scalar,
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


def datepart(field: Column | str, source: Column | str) -> Column:
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

    Builds the engine's ``unix_date`` (datafusion-spark ``SparkUnixDate``) rather than the
    ``CAST(date AS INT)`` chain it lowers to. Spark refuses ``CAST(DATE AS INT)`` at analysis
    (registry row G6-3, ``DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION``) and so does repark, and
    the error message names ``UNIX_DATE`` as the remedy — so the remedy must not itself be spelled
    as the refused cast. ``SparkUnixDate::simplify`` re-creates the cast in the OPTIMIZER, one
    stage after the gate, which is exactly where it is legal.

    The leading ``.cast("date")`` is unchanged: it is what lets a string / timestamp column reach
    a function whose signature is an exact DATE.
    """
    return _scalar("unix_date", _as_column_arg(col, as_lit=False).cast("date"))


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


def make_date(
    year: Column | str | int,
    month: Column | str | int,
    day: Column | str | int,
) -> Column:
    """Build a date from year/month/day (PySpark ``functions.make_date``).

    **An invalid Y-M-D is NULL here, not an error.** Spark under ANSI raises
    ``DATETIME_FIELD_OUT_OF_BOUNDS`` for e.g. ``make_date(2024, 2, 31)``; this
    engine returns NULL on both doors. repark's ``spark.sql.ansi.enabled``
    defaults to ``true``, but the documented scope of that flag is ``/`` and
    ``%`` by zero — see ``docs/guide/session-and-conf.md``: "Do not read 'ANSI
    on' as 'every arithmetic fault raises'". Invalid-date NULL is a recorded
    divergence (FN-GT2 X9), not silent parity.

    Parameters
    ----------
    year, month, day : Column or str or int
        Calendar parts. Invalid dates yield **NULL**.

    Returns
    -------
    Column
        Date32.

    Examples
    --------
    ``F.make_date(2020, 1, 2)`` is date ``2020-01-02``.
    """
    lit_indices = frozenset(
        index
        for index, part in enumerate((year, month, day))
        if not isinstance(part, (Column, str))
    )
    return _scalar("make_date", year, month, day, lit_indices=lit_indices)


def make_interval(
    years: Column | str | int | None = None,
    months: Column | str | int | None = None,
    weeks: Column | str | int | None = None,
    days: Column | str | int | None = None,
    hours: Column | str | int | None = None,
    mins: Column | str | int | None = None,
    secs: Column | str | float | int | None = None,
) -> Column:
    """Build a year-month-day interval (PySpark ``functions.make_interval``).

    Omitted leading parts default to zero. A no-arg call is a zero interval.
    A Python ``str`` part is a **column name** (``make_date`` mold).

    Parameters
    ----------
    years, months, weeks, days, hours, mins, secs : Column or str or number, optional
        Interval parts. ``str`` names a column; ints/floats are literals.

    Returns
    -------
    Column
        Calendar interval (Arrow ``month_day_nano_interval``).

    Examples
    --------
    ``F.make_interval(days=1)`` is 1 day (string form ``'1 days'``).
    """
    parts: list[Column | str | float | int] = [
        0 if part is None else part for part in (years, months, weeks, days, hours, mins, secs)
    ]
    if all(part is None for part in (years, months, weeks, days, hours, mins, secs)):
        return _scalar("make_interval", foldable=True)
    lit_indices = frozenset(
        index for index, part in enumerate(parts) if not isinstance(part, (Column, str))
    )
    return _scalar("make_interval", *parts, lit_indices=lit_indices)


def make_dt_interval(
    days: Column | str | int | None = None,
    hours: Column | str | int | None = None,
    mins: Column | str | int | None = None,
    secs: Column | str | float | int | None = None,
) -> Column:
    """Build a day-time interval (PySpark ``functions.make_dt_interval``).

    A Python ``str`` part is a **column name** (``make_date`` mold).

    Parameters
    ----------
    days, hours, mins, secs : Column or str or number, optional
        Day-time parts. ``str`` names a column; ints/floats are literals.

    Returns
    -------
    Column
        Day-time interval (Arrow ``duration[us]``).

    Examples
    --------
    ``F.make_dt_interval(1, 0, 0, 0)`` is ``timedelta(days=1)``.
    """
    parts: list[Column | str | float | int] = [
        0 if part is None else part for part in (days, hours, mins, secs)
    ]
    if all(part is None for part in (days, hours, mins, secs)):
        return _scalar("make_dt_interval", foldable=True)
    lit_indices = frozenset(
        index for index, part in enumerate(parts) if not isinstance(part, (Column, str))
    )
    return _scalar("make_dt_interval", *parts, lit_indices=lit_indices)


def unix_micros(col: Column | str) -> Column:
    """Microseconds since 1970-01-01 UTC (PySpark ``functions.unix_micros``).

    Dedicated spark-reg kernel. The leading ``.cast("timestamp")`` is the
    ``unix_date`` mold: Spark-legal string/date inputs coerce and localize
    through the session zone.

    Parameters
    ----------
    col : Column or str
        Timestamp (or string/date coerced to timestamp).

    Returns
    -------
    Column
        Int64 microseconds since the UTC epoch.

    Examples
    --------
    ``F.unix_micros(F.lit('1970-01-01 00:00:00'))`` is ``0`` in a UTC session.
    """
    return _scalar("unix_micros", _as_column_arg(col, as_lit=False).cast("timestamp"))


def date_diff(end: Column | str, start: Column | str) -> Column:
    """Days from ``start`` to ``end`` (PySpark ``functions.date_diff``).

    Distinct from the DISPOSED-STUB ``datediff`` (R-FN-BATCH1). Do not alias.
    Date columns match Spark.

    Parameters
    ----------
    end, start : Column or str
        Date columns (end minus start).

    Returns
    -------
    Column
        Integer day difference.

    Examples
    --------
    ``F.date_diff(F.lit(datetime.date(2020, 1, 3)), F.lit(datetime.date(2020, 1, 1)))``
    is ``2``.
    """
    return _scalar("date_diff", end, start)
