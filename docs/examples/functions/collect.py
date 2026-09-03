"""Demonstrate the row-collecting aggregates and their NULL and de-duplication rules.

pins: ex-12-functions-aggregates-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["F.collect_list", "F.collect_set", "F.array_agg", "F.col"]


def main() -> None:
    """Collect each group's values into arrays, compared order-insensitively."""
    repark = ReparkSession.builder.appName("ex-collect").master("local[1]").getOrCreate()
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
        aggregated = frame.groupBy("k").agg(
            F.collect_list("v").alias("collected"),
            F.array_agg(F.col("v")).alias("arrayed"),
            F.collect_set("s").alias("set_values"),
        )
        rows = sorted(aggregated.collect(), key=lambda row: row["k"])
        listed = [sorted(row["collected"]) for row in rows]
        if listed != [[1, 2, 3], [4, 6]]:
            raise SystemExit(f"F.collect_list values {listed!r} != [[1, 2, 3], [4, 6]]")
        arrayed = [sorted(row["arrayed"]) for row in rows]
        if arrayed != listed:
            raise SystemExit(f"F.array_agg values {arrayed!r} != F.collect_list {listed!r}")
        sets = [sorted(row["set_values"]) for row in rows]
        if sets != [["x", "y"], ["z"]]:
            raise SystemExit(f"F.collect_set values {sets!r} != [['x', 'y'], ['z']]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
