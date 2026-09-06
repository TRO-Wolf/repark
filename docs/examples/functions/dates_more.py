"""Demonstrate ``F.add_months`` at month ends and ``F.make_interval`` in date arithmetic."""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.add_months",
    "F.make_interval",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Run the measured month-end and interval date-arithmetic arms."""
    repark = ReparkSession.builder.appName("ex-dates-more").master("local[1]").getOrCreate()
    try:
        month_starts = repark.createDataFrame(
            [("2024-01-31",), ("2024-02-29",), ("2023-12-15",), (None,)], "d STRING"
        ).select(F.to_date("d").alias("d"))
        rows = month_starts.select(
            F.add_months("d", 1).alias("plus_one"),
            F.add_months("d", -2).alias("minus_two"),
        ).collect()
        checked = (
            (
                "plus_one",
                [
                    datetime.date(2024, 2, 29),
                    datetime.date(2024, 3, 29),
                    datetime.date(2024, 1, 15),
                    None,
                ],
            ),
            (
                "minus_two",
                [
                    datetime.date(2023, 11, 30),
                    datetime.date(2023, 12, 29),
                    datetime.date(2023, 10, 15),
                    None,
                ],
            ),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")

        day = repark.createDataFrame([("2024-01-15",)], "d STRING").select(
            F.to_date("d").alias("d")
        )
        rows = day.select(
            (
                F.col("d")
                + F.make_interval(
                    F.lit(1), F.lit(2), F.lit(0), F.lit(3), F.lit(0), F.lit(0), F.lit(0)
                )
            ).alias("shifted")
        ).collect()
        values = [row["shifted"] for row in rows]
        print(f"F.make_interval date shift: {values!r}")
        if values != [datetime.date(2025, 3, 18)]:
            raise SystemExit(f"F.make_interval date shift {values!r} != [2025-03-18]")

        stamp = repark.createDataFrame([("2024-01-15 10:30:05",)], "t STRING").select(
            F.to_timestamp("t").alias("t")
        )
        rows = stamp.select(
            (
                F.col("t")
                + F.make_interval(
                    F.lit(0), F.lit(0), F.lit(0), F.lit(0), F.lit(4), F.lit(5), F.lit(6)
                )
            ).alias("shifted")
        ).collect()
        values = [row["shifted"] for row in rows]
        print(f"F.make_interval timestamp shift: {values!r}")
        if values != [datetime.datetime(2024, 1, 15, 14, 35, 11)]:
            raise SystemExit(f"F.make_interval timestamp shift {values!r} != [2024-01-15 14:35:11]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
