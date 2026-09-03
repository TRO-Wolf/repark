"""Demonstrate moving a date with day arithmetic, month ends, and the next weekday.

pins: ex-6-functions-datetime-a/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.date_add",
    "F.dateadd",
    "F.date_sub",
    "F.last_day",
    "F.next_day",
    "F.col",
]


def main() -> None:
    """Check day shifts on a date column, the month's last day, and the next weekday."""
    repark = ReparkSession.builder.appName("ex-date-arithmetic").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("2024-01-31", 3),
                ("2024-02-29", -7),
                ("2023-11-15", 25),
                ("2023-12-31", 0),
                (None, None),
            ],
            ["ds", "n"],
        )
        date_column = F.col("ds").cast("date")
        count_column = F.col("n").cast("int")
        rows = frame.select(
            date_column.alias("d"),
            count_column.alias("n"),
            F.date_add(date_column, count_column).alias("date_add"),
            F.dateadd(date_column, count_column).alias("dateadd"),
            F.date_sub(date_column, count_column).alias("date_sub"),
            F.last_day(date_column).alias("last_day"),
            F.next_day(date_column, "Mon").alias("next_mon"),
            F.next_day(date_column, "Sun").alias("next_sun"),
        ).collect()
        checked = (
            (
                "date_add",
                [
                    datetime.date(2024, 2, 3),
                    datetime.date(2024, 2, 22),
                    datetime.date(2023, 12, 10),
                    datetime.date(2023, 12, 31),
                    None,
                ],
            ),
            (
                "date_sub",
                [
                    datetime.date(2024, 1, 28),
                    datetime.date(2024, 3, 7),
                    datetime.date(2023, 10, 21),
                    datetime.date(2023, 12, 31),
                    None,
                ],
            ),
            (
                "last_day",
                [
                    datetime.date(2024, 1, 31),
                    datetime.date(2024, 2, 29),
                    datetime.date(2023, 11, 30),
                    datetime.date(2023, 12, 31),
                    None,
                ],
            ),
            (
                "next_mon",
                [
                    datetime.date(2024, 2, 5),
                    datetime.date(2024, 3, 4),
                    datetime.date(2023, 11, 20),
                    datetime.date(2024, 1, 1),
                    None,
                ],
            ),
            (
                "next_sun",
                [
                    datetime.date(2024, 2, 4),
                    datetime.date(2024, 3, 3),
                    datetime.date(2023, 11, 19),
                    datetime.date(2024, 1, 7),
                    None,
                ],
            ),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
        if [row["date_add"] for row in rows] != [row["dateadd"] for row in rows]:
            raise SystemExit("F.dateadd is F.date_add and must agree exactly")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
