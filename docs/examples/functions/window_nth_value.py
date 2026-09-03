"""Demonstrate ``F.nth_value``: the nth value seen so far in the ordered frame.

pins: ex-14-functions-window/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = ["F.nth_value", "F.col"]


def main() -> None:
    """Check the second value of each frame, NULL rows and short frames included."""
    repark = ReparkSession.builder.appName("ex-window-nth-value").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("a", 1, 10), ("a", 2, 20), ("a", 2, 30), ("a", 3, 40), ("b", 1, 50), ("b", 2, None)],
            ["g", "k", "v"],
        )
        ordered = (
            Window.partitionBy("g")
            .orderBy("k", "v")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        rows = (
            frame.select(
                F.col("g"),
                F.col("k"),
                F.col("v"),
                F.nth_value("v", 2).over(ordered).alias("nth_value"),
            )
            .orderBy("g", "k", "v")
            .collect()
        )
        values = [row["nth_value"] for row in rows]
        expected = [None, 20, 20, 20, None, None]
        print(f"F.nth_value: {values!r}")
        if values != expected:
            raise SystemExit(f"F.nth_value values {values!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
