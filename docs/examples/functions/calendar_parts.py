"""Demonstrate the calendar and clock parts of dates and timestamps.

pins: ex-6-functions-datetime-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.year",
    "F.quarter",
    "F.month",
    "F.weekofyear",
    "F.day",
    "F.dayofmonth",
    "F.dayofyear",
    "F.dayofweek",
    "F.weekday",
    "F.hour",
    "F.minute",
    "F.second",
    "F.col",
]


def main() -> None:
    """Check the numeric calendar parts of a date and the clock parts of a timestamp."""
    repark = ReparkSession.builder.appName("ex-calendar-parts").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("2024-01-31", "2024-03-15 13:45:30.123456"),
                ("2024-02-29", "2023-12-31 23:59:59"),
                ("2023-11-15", "2024-02-29 00:15:00"),
                ("2023-12-31", "2023-06-30 06:30:00"),
                (None, None),
            ],
            ["ds", "tss"],
        )
        date_column = F.col("ds").cast("date")
        timestamp_column = F.col("tss").cast("timestamp")
        rows = frame.select(
            date_column.alias("d"),
            timestamp_column.alias("ts"),
            F.year(date_column).alias("year"),
            F.quarter(date_column).alias("quarter"),
            F.month(date_column).alias("month"),
            F.weekofyear(date_column).alias("weekofyear"),
            F.day(date_column).alias("day"),
            F.dayofmonth(date_column).alias("dayofmonth"),
            F.dayofyear(date_column).alias("dayofyear"),
            F.dayofweek(date_column).alias("dayofweek"),
            F.weekday(date_column).alias("weekday"),
            F.hour(timestamp_column).alias("hour"),
            F.minute(timestamp_column).alias("minute"),
            F.second(timestamp_column).alias("second"),
        ).collect()
        checked = (
            ("year", [2024, 2024, 2023, 2023, None]),
            ("quarter", [1, 1, 4, 4, None]),
            ("month", [1, 2, 11, 12, None]),
            ("weekofyear", [5, 9, 46, 52, None]),
            ("day", [31, 29, 15, 31, None]),
            ("dayofmonth", [31, 29, 15, 31, None]),
            ("dayofyear", [31, 60, 319, 365, None]),
            ("dayofweek", [4, 5, 4, 1, None]),
            ("weekday", [2, 3, 2, 6, None]),
            ("hour", [13, 23, 0, 6, None]),
            ("minute", [45, 59, 15, 30, None]),
            ("second", [30, 59, 0, 0, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        if [row["day"] for row in rows] != [row["dayofmonth"] for row in rows]:
            raise SystemExit("F.day is F.dayofmonth and must agree exactly")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
