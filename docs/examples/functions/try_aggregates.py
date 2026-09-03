"""Demonstrate ``F.try_sum`` and ``F.try_avg``, which answer NULL where the sum overflows.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import math

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.try_avg", "F.try_sum", "F.col"]


def main() -> None:
    """Overflow one group's sum to NULL while its average stays a finite double."""
    repark = ReparkSession.builder.appName("ex-try-aggregates").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                ("a", 9223372036854775807),
                ("a", 1),
                ("b", 2),
                ("b", 3),
            ],
            ["k", "x"],
        )
        aggregated = frame.groupBy("k").agg(
            F.try_sum(F.col("x")).alias("tried_sum"),
            F.try_avg("x").alias("tried_avg"),
        )
        rows = sorted(aggregated.collect(), key=lambda row: row["k"])
        sums = [row["tried_sum"] for row in rows]
        if sums != [None, 5]:
            raise SystemExit(f"F.try_sum values {sums!r} != [None, 5]; overflow answers NULL")
        averages = [row["tried_avg"] for row in rows]
        for value, expected in zip(averages, [4.611686018427388e18, 2.5], strict=True):
            if not math.isclose(value, expected, rel_tol=1e-12):
                raise SystemExit(f"F.try_avg value {value!r} != {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
