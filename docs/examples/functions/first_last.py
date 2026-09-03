"""Demonstrate ``F.first``, ``F.last`` and aliases ``F.first_value``/``F.last_value``.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.first", "F.last", "F.first_value", "F.last_value", "F.col"]


def main() -> None:
    """Take the first and last group value, and skip the trailing NULL on demand."""
    repark = ReparkSession.builder.appName("ex-first-last").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1),
                ("a", 2),
                ("a", 3),
                ("a", None),
                ("b", 4),
                ("b", 6),
            ],
            ["k", "v"],
        )
        aggregated = frame.groupBy("k").agg(
            F.first(F.col("v")).alias("first_value"),
            F.last("v").alias("last_value"),
            F.last("v", True).alias("last_value_ignoring_nulls"),
            F.first_value("v").alias("first_alias"),
            F.last_value("v").alias("last_alias"),
        )
        rows = sorted(aggregated.collect(), key=lambda row: row["k"])
        firsts = [row["first_value"] for row in rows]
        if firsts != [1, 4]:
            raise SystemExit(f"F.first values {firsts!r} != [1, 4]")
        lasts = [row["last_value"] for row in rows]
        if lasts != [None, 6]:
            raise SystemExit(f"F.last values {lasts!r} != [None, 6]")
        ignoring = [row["last_value_ignoring_nulls"] for row in rows]
        if ignoring != [3, 6]:
            raise SystemExit(f"F.last ignorenulls values {ignoring!r} != [3, 6]")
        first_aliases = [row["first_alias"] for row in rows]
        if first_aliases != firsts:
            raise SystemExit(f"F.first_value values {first_aliases!r} != F.first {firsts!r}")
        last_aliases = [row["last_alias"] for row in rows]
        if last_aliases != lasts:
            raise SystemExit(f"F.last_value values {last_aliases!r} != F.last {lasts!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
