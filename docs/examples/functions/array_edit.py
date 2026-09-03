"""Demonstrate the ``F.*`` names that grow, shrink, and clean an array.

pins: ex-8-functions-arrays/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.array_append",
    "F.array_prepend",
    "F.array_remove",
    "F.array_compact",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Add and drop elements at either end, and strip the NULLs out."""
    repark = ReparkSession.builder.appName("ex-array-edit").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame([([10, 20, 30],), ([None, 5],), (None,)], ["a"])
        rows = frame.select(
            F.col("a"),
            F.array_append(F.col("a"), F.lit(4)).alias("appended"),
            F.array_append(F.col("a"), F.lit(None)).alias("appended_null"),
            F.array_prepend(F.col("a"), F.lit(0)).alias("prepended"),
            F.array_remove(F.col("a"), F.lit(20)).alias("removed"),
            F.array_compact(F.col("a")).alias("compacted"),
        ).collect()
        checked = (
            ("appended", [[10, 20, 30, 4], [None, 5, 4], None]),
            ("appended_null", [[10, 20, 30, None], [None, 5, None], None]),
            ("prepended", [[0, 10, 20, 30], [0, None, 5], None]),
            ("removed", [[10, 30], [None, 5], None]),
            ("compacted", [[10, 20, 30], [5], None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} gave {values!r}, expected {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
