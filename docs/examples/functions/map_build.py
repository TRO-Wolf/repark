"""Demonstrate building and joining maps, and inserting into an array at a position.

pins: fnp-9-collections-json/C-006
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.create_map",
    "F.map_concat",
    "F.array_insert",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Build a one-entry map per row, union a constant map onto it, then edit an array."""
    repark = ReparkSession.builder.appName("ex-map-build").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("a", 1, [1, 2]), ("b", 2, [3]), ("c", None, [])],
            "k STRING, v INT, ai ARRAY<INT>",
        )
        rows = frame.select(
            F.create_map(F.col("k"), F.col("v")).alias("as_map"),
            F.map_concat(
                F.create_map(F.col("k"), F.col("v")), F.create_map(F.lit("z"), F.lit(9))
            ).alias("joined"),
            F.array_insert("ai", 1, F.lit(0)).alias("front"),
            F.array_insert("ai", -1, F.lit(99)).alias("back"),
            F.array_insert("ai", 5, F.lit(7)).alias("padded"),
        ).collect()
        as_map = [sorted(dict(row["as_map"]).items()) for row in rows]
        print(f"F.create_map: {as_map!r}")
        if as_map != [[("a", 1)], [("b", 2)], [("c", None)]]:
            raise SystemExit(f"F.create_map gave {as_map!r}")
        joined = [sorted(dict(row["joined"]).items()) for row in rows]
        print(f"F.map_concat: {joined!r}")
        if joined != [[("a", 1), ("z", 9)], [("b", 2), ("z", 9)], [("c", None), ("z", 9)]]:
            raise SystemExit(f"F.map_concat gave {joined!r}")
        checked = (
            ("front", [[0, 1, 2], [0, 3], [0]]),
            ("back", [[1, 2, 99], [3, 99], [99]]),
            (
                "padded",
                [[1, 2, None, None, 7], [3, None, None, None, 7], [None, None, None, None, 7]],
            ),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.array_insert {name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.array_insert {name} gave {values!r}, expected {expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
