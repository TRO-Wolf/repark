"""Demonstrate ``F.make_date`` and ``F.make_dt_interval``, built from calendar parts.

pins: ex-7-functions-datetime-b/C-001
"""

from __future__ import annotations

import datetime

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.make_date", "F.make_dt_interval", "F.col", "F.lit"]


def main() -> None:
    """Check the built date, its literal form, and the built day-time interval."""
    repark = ReparkSession.builder.appName("ex-make-calendar").master("local[1]").getOrCreate()
    try:
        parts = repark.createDataFrame(
            [(2020, 1, 2), (2024, 2, 29), (1999, 12, 31), (None, 1, 2)], ["y", "m", "d"]
        )
        rows = parts.select(
            F.make_date(F.col("y"), F.col("m"), F.col("d")).alias("v"),
            F.make_date(F.lit(2020), F.lit(1), F.lit(2)).alias("literal"),
        ).collect()
        values = [row["v"] for row in rows]
        if values != [
            datetime.date(2020, 1, 2),
            datetime.date(2024, 2, 29),
            datetime.date(1999, 12, 31),
            None,
        ]:
            raise SystemExit(f"F.make_date values {values!r} != the built date list")
        values = [row["literal"] for row in rows]
        if values != [datetime.date(2020, 1, 2)] * 4:
            raise SystemExit(f"F.make_date literal values {values!r} != [2020-01-02] * 4")

        spans = repark.createDataFrame(
            [(1, 2, 3, 4.5), (0, 0, 0, 0.0), (-1, 0, 0, 0.25), (None, None, None, None)],
            ["d", "h", "mi", "s"],
        )
        rows = spans.select(
            F.make_dt_interval(F.col("d"), F.col("h"), F.col("mi"), F.col("s")).alias("v")
        ).collect()
        values = [row["v"] for row in rows]
        if values != [
            datetime.timedelta(days=1, seconds=7384, microseconds=500000),
            datetime.timedelta(0),
            datetime.timedelta(days=-1, microseconds=250000),
            None,
        ]:
            raise SystemExit(f"F.make_dt_interval values {values!r} != the built durations")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
