"""Demonstrate ``F.abs`` on a small local frame.

pins: ex-0-example-drift-gate/C-002, C-008, C-010
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import SparkSession

COVERS: list[str] = ["F.abs", "F.col", "F.lit"]


def main() -> None:
    """Run ``F.abs`` on one positive, one negative, and one NULL input."""
    spark = SparkSession.builder.appName("ex-abs").master("local[1]").getOrCreate()
    try:
        frame = spark.createDataFrame([(1,), (-2,), (None,)], ["x"])
        rows = frame.select(F.abs(F.col("x")).alias("a"), F.lit(1).alias("one")).collect()
        values = [row["a"] for row in rows]
        if values != [1, 2, None]:
            raise SystemExit(f"F.abs values {values!r} != [1, 2, None]")
        ones = [row["one"] for row in rows]
        if ones != [1, 1, 1]:
            raise SystemExit(f"F.lit values {ones!r} != [1, 1, 1]")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
