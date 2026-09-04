"""Rank and total rows over a window with ``Column.over``.

pins: ex-17-column-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession, Window

COVERS: list[str] = ["Column.over"]


def main() -> None:
    """Run the measured window ranking and partition-total answers on one frame."""
    repark = ReparkSession.builder.appName("ex-col-over").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 1, 10.0),
                ("a", 2, 20.0),
                ("a", 2, 30.0),
                ("a", 3, 40.0),
                ("b", 1, 50.0),
                ("b", 2, None),
            ],
            ["g", "k", "v"],
        )
        rank_col = F.row_number()
        spec = Window.partitionBy("g").orderBy(F.col("k").asc())
        ranked = frame.withColumn("rn", rank_col.over(spec)).select("g", "k", "rn")
        rank_rows = set(ranked.collect())
        rank_expected = {
            ("a", 1, 1),
            ("a", 2, 2),
            ("a", 2, 3),
            ("a", 3, 4),
            ("b", 1, 1),
            ("b", 2, 2),
        }
        if rank_rows != rank_expected:
            raise SystemExit(f"Column.over rank rows {rank_rows!r} != {rank_expected!r}")

        total_col = F.sum("v")
        totals = frame.withColumn("tv", total_col.over(Window.partitionBy("g"))).select("g", "tv")
        total_rows = sorted(totals.collect(), key=tuple)
        total_expected = [
            ("a", 100.0),
            ("a", 100.0),
            ("a", 100.0),
            ("a", 100.0),
            ("b", 50.0),
            ("b", 50.0),
        ]
        if total_rows != total_expected:
            raise SystemExit(f"Column.over sum rows {total_rows!r} != {total_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
