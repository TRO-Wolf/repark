"""Demonstrate where a row sits in its partition: relative rank, share, tile.

pins: ex-14-functions-window/C-001
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = ["F.percent_rank", "F.cume_dist", "F.ntile", "F.col"]


def main() -> None:
    """Check the relative position of each row against the live-matched values."""
    repark = ReparkSession.builder.appName("ex-window-position").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("a", 1, 10), ("a", 2, 20), ("a", 2, 30), ("a", 3, 40), ("b", 1, 50), ("b", 2, None)],
            ["g", "k", "v"],
        )
        ordered = Window.partitionBy("g").orderBy("k", "v")
        peers = Window.partitionBy("g").orderBy("k")
        rows = (
            frame.select(
                F.col("g"),
                F.col("k"),
                F.col("v"),
                F.percent_rank().over(peers).alias("percent_rank"),
                F.cume_dist().over(peers).alias("cume_dist"),
                F.ntile(2).over(ordered).alias("ntile"),
            )
            .orderBy("g", "k", "v")
            .collect()
        )
        checked = (
            ("percent_rank", [0.0, 0.3333333333333333, 0.3333333333333333, 1.0, 0.0, 1.0]),
            ("cume_dist", [0.25, 0.75, 0.75, 1.0, 0.5, 1.0]),
            ("ntile", [1, 1, 2, 2, 1, 2]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            for value, want in zip(values, expected, strict=True):
                if not math.isclose(value, want, rel_tol=1e-12):
                    raise SystemExit(f"F.{name} gave {value!r}, expected {want!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
