"""Demonstrate the ``F.*`` array set algebra on two small local array columns.

pins: ex-8-functions-arrays/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.array_distinct",
    "F.array_union",
    "F.array_intersect",
    "F.array_except",
    "F.col",
]


def main() -> None:
    """Deduplicate one array and combine two, with NULL elements and NULL arrays."""
    repark = ReparkSession.builder.appName("ex-array-setops").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [([1, 2, 3], [2, 3, 4]), ([1, 1, 2], [2, 3]), ([None], [1]), (None, [1])],
            ["a", "b"],
        )
        rows = frame.select(
            F.col("a"),
            F.col("b"),
            F.array_distinct(F.col("a")).alias("distinct"),
            F.array_union(F.col("a"), F.col("b")).alias("unioned"),
            F.array_intersect(F.col("a"), F.col("b")).alias("intersected"),
            F.array_except(F.col("a"), F.col("b")).alias("excepted"),
        ).collect()
        checked = (
            ("distinct", [[1, 2, 3], [1, 2], [None], None]),
            ("unioned", [[1, 2, 3, 4], [1, 2, 3], [None, 1], None]),
            ("intersected", [[2, 3], [2], [], None]),
            ("excepted", [[1], [1], [None], None]),
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
