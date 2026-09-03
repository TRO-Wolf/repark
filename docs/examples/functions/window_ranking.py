"""Demonstrate the ``F.*`` ranking family: how ties are counted three ways.

pins: ex-14-functions-window/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = ["F.row_number", "F.rank", "F.dense_rank", "F.col"]


def main() -> None:
    """Number rows in order and rank peers with and without gaps."""
    repark = ReparkSession.builder.appName("ex-window-ranking").master("local[1]").getOrCreate()
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
                F.row_number().over(ordered).alias("row_number"),
                F.rank().over(peers).alias("rank"),
                F.dense_rank().over(peers).alias("dense_rank"),
            )
            .orderBy("g", "k", "v")
            .collect()
        )
        checked = (
            ("row_number", [1, 2, 3, 4, 1, 2]),
            ("rank", [1, 2, 2, 4, 1, 2]),
            ("dense_rank", [1, 2, 2, 3, 1, 2]),
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
