"""Demonstrate the counting variants: conditions, distinct values, and the approximate count.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.count_if",
    "F.countDistinct",
    "F.count_distinct",
    "F.approx_count_distinct",
    "F.col",
]


def main() -> None:
    """Count true rows, distinct values, distinct tuples, and the approximate distinct count."""
    repark = ReparkSession.builder.appName("ex-counting").master("local[1]").getOrCreate()
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
        counted_if = frame.select(F.count_if(F.col("v") > 2)).collect()[0][0]
        if counted_if != 3:
            raise SystemExit(f"F.count_if value {counted_if!r} != 3; NULL conditions are not true")
        distinct = frame.select(F.countDistinct("s")).collect()[0][0]
        if distinct != 3:
            raise SystemExit(f"F.countDistinct value {distinct!r} != 3; NULL is not a value")
        tuples = frame.select(F.count_distinct("k", "v")).collect()[0][0]
        if tuples != 5:
            raise SystemExit(f"F.count_distinct value {tuples!r} != 5; NULL drops the tuple")
        approx = frame.select(F.approx_count_distinct("s")).collect()[0][0]
        if approx != 3:
            raise SystemExit(f"F.approx_count_distinct value {approx!r} != 3")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
