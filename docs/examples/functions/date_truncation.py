"""Demonstrate truncating a timestamp with ``date_trunc`` and a date with ``trunc``.

pins: ex-6-functions-datetime-a/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.date_trunc", "F.trunc", "F.col"]


def main() -> None:
    """Check the year, month and day truncation of a timestamp beside a date's truncation."""
    repark = ReparkSession.builder.appName("ex-date-truncation").master("local[1]").getOrCreate()
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
            F.date_trunc("year", timestamp_column).alias("ts_year"),
            F.date_trunc("month", timestamp_column).alias("ts_month"),
            F.date_trunc("day", timestamp_column).alias("ts_day"),
            F.trunc(date_column, "year").alias("d_year"),
            F.trunc(date_column, "month").alias("d_month"),
            F.trunc(date_column, "quarter").alias("d_quarter"),
        ).collect()
        checked = (
            (
                "ts_year",
                [
                    datetime.datetime(2024, 1, 1, 0, 0),
                    datetime.datetime(2023, 1, 1, 0, 0),
                    datetime.datetime(2024, 1, 1, 0, 0),
                    datetime.datetime(2023, 1, 1, 0, 0),
                    None,
                ],
            ),
            (
                "ts_month",
                [
                    datetime.datetime(2024, 3, 1, 0, 0),
                    datetime.datetime(2023, 12, 1, 0, 0),
                    datetime.datetime(2024, 2, 1, 0, 0),
                    datetime.datetime(2023, 6, 1, 0, 0),
                    None,
                ],
            ),
            (
                "ts_day",
                [
                    datetime.datetime(2024, 3, 15, 0, 0),
                    datetime.datetime(2023, 12, 31, 0, 0),
                    datetime.datetime(2024, 2, 29, 0, 0),
                    datetime.datetime(2023, 6, 30, 0, 0),
                    None,
                ],
            ),
            (
                "d_year",
                [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 1),
                    datetime.date(2023, 1, 1),
                    datetime.date(2023, 1, 1),
                    None,
                ],
            ),
            (
                "d_month",
                [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 2, 1),
                    datetime.date(2023, 11, 1),
                    datetime.date(2023, 12, 1),
                    None,
                ],
            ),
            (
                "d_quarter",
                [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 1),
                    datetime.date(2023, 10, 1),
                    datetime.date(2023, 10, 1),
                    None,
                ],
            ),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
