"""Demonstrate the ``F.*`` offset family: the previous and next row's value.

pins: ex-14-functions-window/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = ["F.lag", "F.lead", "F.col"]


def main() -> None:
    """Check lag and lead at two offsets, with and without a fill default."""
    repark = ReparkSession.builder.appName("ex-window-offset").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("a", 1, 10), ("a", 2, 20), ("a", 2, 30), ("a", 3, 40), ("b", 1, 50), ("b", 2, None)],
            ["g", "k", "v"],
        )
        ordered = Window.partitionBy("g").orderBy("k", "v")
        rows = (
            frame.select(
                F.col("g"),
                F.col("k"),
                F.col("v"),
                F.lag("v", 1).over(ordered).alias("lag_1"),
                F.lag("v", 2, -1).over(ordered).alias("lag_2"),
                F.lead("v", 1).over(ordered).alias("lead_1"),
                F.lead("v", 1, 0).over(ordered).alias("lead_2"),
            )
            .orderBy("g", "k", "v")
            .collect()
        )
        checked = (
            ("lag_1", [None, 10, 20, 30, None, 50]),
            ("lag_2", [-1, -1, 10, 20, -1, -1]),
            ("lead_1", [20, 30, 40, None, None, None]),
            ("lead_2", [20, 30, 40, 0, None, 0]),
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
