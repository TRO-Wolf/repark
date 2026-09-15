"""FNP-11A temporal constructors, intervals and arithmetic (live Spark 4.1.2 semantics)."""

from typing import Any

from repark.errors import PySparkTypeError
from repark.spark.column import Column
from repark.spark.functions import _scalar

FNP11A_EXPORTS: tuple[str, ...] = (
    "make_timestamp_ltz",
    "make_timestamp_ntz",
    "try_make_timestamp",
    "try_make_timestamp_ltz",
    "try_make_timestamp_ntz",
    "make_ym_interval",
    "try_make_interval",
    "convert_timezone",
    "localtimestamp",
    "timestamp_add",
    "timestamp_diff",
    "to_timestamp_ltz",
    "to_timestamp_ntz",
)


def install_into(namespace: dict[str, Any], all: list[str]) -> None:
    """Expose the temporal names on ``repark.spark.functions`` (see functions_try)."""
    from repark.spark import functions_temporal as module

    for name in FNP11A_EXPORTS:
        namespace[name] = getattr(module, name)
    all.extend(FNP11A_EXPORTS)


def _or_zero(value: Column | str | int | float | None) -> Column | str | int | float:
    """Missing interval parts mean zero (live Spark fills omitted parts with zero)."""
    return 0 if value is None else value


def _given_parts(
    values: list[Column | str | int | float | None],
) -> list[Column | str | int | float]:
    """Drop trailing omitted parts; Spark prints only the parts the call gave."""
    parts = list(values)
    while parts and parts[-1] is None:
        parts.pop()
    return [_or_zero(value) for value in parts]


def _timestamp_parts(
    name: str,
    years: Column | str | int | None,
    months: Column | str | int | None,
    days: Column | str | int | None,
    hours: Column | str | int | None,
    mins: Column | str | int | None,
    secs: Column | str | float | None,
    timezone: Column | str | None,
    date: Column | str | None,
    time: Column | str | None,
) -> Column:
    """Lower one make_timestamp shape: the (date, time[, timezone]) or numeric form."""
    if date is not None or time is not None:
        if timezone is None:
            return _scalar(name, date, time)
        return _scalar(name, date, time, timezone)
    parts: list[Column | str | int | float | None] = [years, months, days, hours, mins, secs]
    if timezone is None:
        return _scalar(name, *parts)
    return _scalar(name, *parts, timezone)


def make_timestamp(
    years: Column | str | int,
    months: Column | str | int,
    days: Column | str | int,
    hours: Column | str | int,
    mins: Column | str | int,
    secs: Column | str | float,
    timezone: Column | str | None = None,
) -> Column:
    """Build a session-zone timestamp from parts under the frozen 1.0 signature."""
    return _timestamp_parts(
        "make_timestamp", years, months, days, hours, mins, secs, timezone, None, None
    )


def try_make_timestamp(
    years: Column | str | int | None = None,
    months: Column | str | int | None = None,
    days: Column | str | int | None = None,
    hours: Column | str | int | None = None,
    mins: Column | str | int | None = None,
    secs: Column | str | float | None = None,
    timezone: Column | str | None = None,
    date: Column | str | None = None,
    time: Column | str | None = None,
) -> Column:
    """Build a timestamp, answering NULL where make_timestamp would raise."""
    return _timestamp_parts(
        "try_make_timestamp", years, months, days, hours, mins, secs, timezone, date, time
    )


def make_timestamp_ltz(
    years: Column | str | int,
    months: Column | str | int,
    days: Column | str | int,
    hours: Column | str | int,
    mins: Column | str | int,
    secs: Column | str | float,
    timezone: Column | str | None = None,
) -> Column:
    """Build a session-zone timestamp from parts, with an optional zone override."""
    if timezone is None:
        return _scalar("make_timestamp_ltz", years, months, days, hours, mins, secs)
    return _scalar("make_timestamp_ltz", years, months, days, hours, mins, secs, timezone)


def try_make_timestamp_ltz(
    years: Column | str | int,
    months: Column | str | int,
    days: Column | str | int,
    hours: Column | str | int,
    mins: Column | str | int,
    secs: Column | str | float,
    timezone: Column | str | None = None,
) -> Column:
    """Build a zone timestamp, answering NULL where make_timestamp_ltz would raise."""
    if timezone is None:
        return _scalar("try_make_timestamp_ltz", years, months, days, hours, mins, secs)
    return _scalar("try_make_timestamp_ltz", years, months, days, hours, mins, secs, timezone)


def make_timestamp_ntz(
    years: Column | str | int | None = None,
    months: Column | str | int | None = None,
    days: Column | str | int | None = None,
    hours: Column | str | int | None = None,
    mins: Column | str | int | None = None,
    secs: Column | str | float | None = None,
    date: Column | str | None = None,
    time: Column | str | None = None,
) -> Column:
    """Build a zone-free timestamp from parts or from a date plus a time."""
    if date is not None or time is not None:
        return _scalar("make_timestamp_ntz", date, time)
    return _scalar("make_timestamp_ntz", years, months, days, hours, mins, secs)


def try_make_timestamp_ntz(
    years: Column | str | int | None = None,
    months: Column | str | int | None = None,
    days: Column | str | int | None = None,
    hours: Column | str | int | None = None,
    mins: Column | str | int | None = None,
    secs: Column | str | float | None = None,
    date: Column | str | None = None,
    time: Column | str | None = None,
) -> Column:
    """Build a zone-free timestamp, answering NULL where make_timestamp_ntz raises."""
    if date is not None or time is not None:
        return _scalar("try_make_timestamp_ntz", date, time)
    return _scalar("try_make_timestamp_ntz", years, months, days, hours, mins, secs)


def make_ym_interval(
    years: Column | str | int | None = None, months: Column | str | int | None = None
) -> Column:
    """Build a year-month interval (missing parts mean zero)."""
    return _scalar("make_ym_interval", *_given_parts([years, months]))


def try_make_interval(
    years: Column | str | int | None = None,
    months: Column | str | int | None = None,
    weeks: Column | str | int | None = None,
    days: Column | str | int | None = None,
    hours: Column | str | int | None = None,
    mins: Column | str | int | None = None,
    secs: Column | str | float | None = None,
) -> Column:
    """Build a calendar interval, answering NULL on overflow (missing parts are zero)."""
    parts = _given_parts([years, months, weeks, days, hours, mins, secs])
    return _scalar("try_make_interval", *(parts or [0]))


def months_between(date1: Column | str, date2: Column | str, roundOff: bool = True) -> Column:  # noqa: N803
    """Month distance between two dates with Spark's last-day and 31-day rules."""
    if roundOff:
        return _scalar("months_between", date1, date2)
    return _scalar("months_between", date1, date2, roundOff, lit_indices=frozenset({2}))


def convert_timezone(
    sourceTz: Column | None,  # noqa: N803
    targetTz: Column,  # noqa: N803
    sourceTs: Column | str,  # noqa: N803
) -> Column:
    """Shift a wall clock from the source zone to the target zone (zones are columns)."""
    for label, zone in (("sourceTz", sourceTz), ("targetTz", targetTz)):
        if zone is not None and not isinstance(zone, Column):
            raise PySparkTypeError(f"convert_timezone {label} must be a Column or None")
    if sourceTz is None:
        return _scalar("convert_timezone", targetTz, sourceTs)
    return _scalar("convert_timezone", sourceTz, targetTz, sourceTs)


def localtimestamp() -> Column:
    """Current session-zone wall clock as a zone-free timestamp."""
    return _scalar("localtimestamp")


def timestamp_add(unit: str | Column, quantity: Column | str | int, ts: Column | str) -> Column:
    """Shift a timestamp by a quantity of the named unit (unit is case-insensitive)."""
    return _scalar("timestampadd", unit, quantity, ts, lit_indices=frozenset({0}))


def timestamp_diff(unit: str | Column, start: Column | str, end: Column | str) -> Column:
    """Whole units between two timestamps (unit is case-insensitive)."""
    return _scalar("timestampdiff", unit, start, end, lit_indices=frozenset({0}))


def to_timestamp_ltz(timestamp: Column | str, format: Column | str | None = None) -> Column:
    """Parse to a session-zone timestamp, with an optional Java datetime pattern."""
    if format is None:
        return _scalar("to_timestamp_ltz", timestamp)
    return _scalar("to_timestamp_ltz", timestamp, format, lit_indices=frozenset({1}))


def to_timestamp_ntz(timestamp: Column | str, format: Column | str | None = None) -> Column:
    """Parse to a zone-free timestamp, with an optional Java datetime pattern."""
    if format is None:
        return _scalar("to_timestamp_ntz", timestamp)
    return _scalar("to_timestamp_ntz", timestamp, format, lit_indices=frozenset({1}))
