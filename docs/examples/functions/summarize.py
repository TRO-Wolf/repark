"""Demonstrate the classic summary aggregates of one grouped frame, median included.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.count",
    "F.sum",
    "F.avg",
    "F.mean",
    "F.median",
    "F.min",
    "F.max",
    "F.col",
]


def main() -> None:
    """Summarize one grouped frame — totals, extremes and the middle — with NULLs skipped."""
    repark = ReparkSession.builder.appName("ex-summarize").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, "x"),
                ("a", 2, "y"),
                ("a", 3, "x"),
                ("a", None, None),
                ("b", 4, "z"),
                ("b", 6, "z"),
            ],
            ["k", "v", "s"],
        )
        aggregated = frame.groupBy("k").agg(
            F.count("v").alias("count_v"),
            F.count("*").alias("count_rows"),
            F.sum(F.col("v")).alias("total"),
            F.avg("v").alias("mean_value"),
            F.mean("v").alias("mean_alias"),
            F.median("v").alias("middle"),
            F.min("v").alias("low"),
            F.max("v").alias("high"),
        )
        rows = sorted(aggregated.collect(), key=lambda row: row["k"])
        counted = [row["count_v"] for row in rows]
        if counted != [3, 2]:
            raise SystemExit(f"F.count values {counted!r} != [3, 2]; NULLs are not counted")
        row_counts = [row["count_rows"] for row in rows]
        if row_counts != [4, 2]:
            raise SystemExit(f'F.count("*") values {row_counts!r} != [4, 2]')
        totals = [row["total"] for row in rows]
        if totals != [6, 10]:
            raise SystemExit(f"F.sum values {totals!r} != [6, 10]")
        means = [row["mean_value"] for row in rows]
        for value, expected in zip(means, [2.0, 5.0], strict=True):
            if not math.isclose(value, expected, rel_tol=1e-12):
                raise SystemExit(f"F.avg value {value!r} != {expected!r}")
        aliases = [row["mean_alias"] for row in rows]
        for value, alias_value in zip(means, aliases, strict=True):
            if not math.isclose(value, alias_value, rel_tol=1e-12):
                raise SystemExit(f"F.mean value {alias_value!r} != F.avg value {value!r}")
        middles = [row["middle"] for row in rows]
        for value, expected in zip(middles, [2.0, 5.0], strict=True):
            if not math.isclose(value, expected, rel_tol=1e-12):
                raise SystemExit(f"F.median value {value!r} != {expected!r}")
        lows = [row["low"] for row in rows]
        if lows != [1, 4]:
            raise SystemExit(f"F.min values {lows!r} != [1, 4]")
        highs = [row["high"] for row in rows]
        if highs != [3, 6]:
            raise SystemExit(f"F.max values {highs!r} != [3, 6]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
