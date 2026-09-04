"""Temporal types: date, timestamp, timestamp without time zone, calendar interval.

pins: ex-22-types-writerv2/C-001
"""

from __future__ import annotations

import datetime

from repark.spark import types as T  # noqa: N812

COVERS: list[str] = [
    "types.DateType",
    "types.TimestampType",
    "types.TimestampNTZType",
    "types.CalendarIntervalType",
]


def expect(label: str, got: object, wanted: object) -> None:
    if got != wanted:
        raise SystemExit(f"{label} {got!r} != {wanted!r}")


def main() -> None:
    """Run the measured construction, display, and date-conversion answers."""
    expect("DateType.simpleString", T.DateType().simpleString(), "date")
    expect("TimestampType.simpleString", T.TimestampType().simpleString(), "timestamp")
    expect("TimestampNTZType.simpleString", T.TimestampNTZType().simpleString(), "timestamp_ntz")
    expect("TimestampNTZType.typeName class", T.TimestampNTZType.typeName(), "timestamp_ntz")
    expect("CalendarIntervalType.simpleString", T.CalendarIntervalType().simpleString(), "interval")
    expect("CalendarIntervalType.typeName class", T.CalendarIntervalType.typeName(), "interval")

    date_type = T.DateType()
    day = datetime.date(2024, 3, 15)
    expect("DateType.toInternal", date_type.toInternal(day), 19797)
    expect("DateType.fromInternal", date_type.fromInternal(19797), day)
    expect("DateType.needConversion", date_type.needConversion(), True)
    expect("StringType.needConversion", T.StringType().needConversion(), False)


if __name__ == "__main__":
    main()
