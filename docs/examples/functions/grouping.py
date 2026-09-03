"""Demonstrate ``F.grouping`` inside a cube: 1 for the grand-total row, 0 for the rest.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.grouping", "F.sum", "F.col"]


def main() -> None:
    """Flag the cube's grand-total row with 1 and every member row with 0."""
    repark = ReparkSession.builder.appName("ex-grouping").master("local[1]").getOrCreate()
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
        cubed = frame.cube("k").agg(
            F.sum(F.col("v")).alias("total"),
            F.grouping(F.col("k")).alias("grouped"),
        )
        rows = sorted(cubed.collect(), key=lambda row: (row["k"] is not None, str(row["k"])))
        keys = [row["k"] for row in rows]
        if keys != [None, "a", "b"]:
            raise SystemExit(f"cube keys {keys!r} != [None, 'a', 'b']")
        totals = [row["total"] for row in rows]
        if totals != [16, 6, 10]:
            raise SystemExit(f"cube totals {totals!r} != [16, 6, 10]")
        grouped_flags = [row["grouped"] for row in rows]
        if grouped_flags != [1, 0, 0]:
            raise SystemExit(f"F.grouping values {grouped_flags!r} != [1, 0, 0]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
