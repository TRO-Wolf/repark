"""Demonstrate the ``F.*`` array search, order, overlap, flatten and map-merge names."""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.array_position",
    "F.array_sort",
    "F.arrays_overlap",
    "F.flatten",
    "F.map_zip_with",
    "F.lit",
]


def main() -> None:
    repark = ReparkSession.builder.appName("ex-array-more").master("local[1]").getOrCreate()
    try:
        arrays = repark.createDataFrame([([10, 20, 30],), ([5],), ([],), (None,)], "a ARRAY<INT>")
        rows = arrays.select(
            F.array_position("a", 20).alias("found"),
            F.array_position("a", 99).alias("missing"),
            F.array_position("a", F.lit(None).cast("int")).alias("null_element"),
        ).collect()
        checked = (
            ("found", [2, 0, 0, None]),
            ("missing", [0, 0, 0, None]),
            ("null_element", [None, None, None, None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} values {values!r} != {expected!r}")

        sort_frame = repark.createDataFrame(
            [([3, 1, 2],), ([2, None, 1],), ([],), (None,)], "a ARRAY<INT>"
        )
        rows = sort_frame.select(F.array_sort("a").alias("sorted")).collect()
        values = [row["sorted"] for row in rows]
        print(f"F.array_sort: {values!r}")
        if values != [[1, 2, 3], [1, 2, None], [], None]:
            raise SystemExit(f"F.array_sort values {values!r} != the ascending sort")

        overlap = repark.createDataFrame(
            [([1, 2], [2, 3]), ([1], [2]), ([None], [1]), (None, [1]), ([1], None)],
            "a ARRAY<INT>, b ARRAY<INT>",
        )
        rows = overlap.select(F.arrays_overlap("a", "b").alias("overlap")).collect()
        values = [row["overlap"] for row in rows]
        print(f"F.arrays_overlap: {values!r}")
        if values != [True, False, None, None, None]:
            raise SystemExit(f"F.arrays_overlap values {values!r} != the overlap triple")

        nested = repark.createDataFrame(
            [([[1, 2], [3], []],), ([[None], [4]],), ([None],), (None,)], "a ARRAY<ARRAY<INT>>"
        )
        rows = nested.select(F.flatten("a").alias("flat")).collect()
        values = [row["flat"] for row in rows]
        print(f"F.flatten: {values!r}")
        if values != [[1, 2, 3], [None, 4], None, None]:
            raise SystemExit(f"F.flatten values {values!r} != the flattened arrays")

        maps = repark.createDataFrame(
            [({1: "a", 2: "b"}, {1: "x", 3: "y"})], "m1 MAP<INT, STRING>, m2 MAP<INT, STRING>"
        )
        rows = maps.select(
            F.map_zip_with(
                "m1",
                "m2",
                lambda key, first, second: F.concat(
                    F.coalesce(first, F.lit("")), F.coalesce(second, F.lit(""))
                ),
            ).alias("merged")
        ).collect()
        values = [row["merged"] for row in rows]
        print(f"F.map_zip_with: {values!r}")
        if values != [{1: "ax", 2: "b", 3: "y"}]:
            raise SystemExit(f"F.map_zip_with values {values!r} != [{{1: 'ax', 2: 'b', 3: 'y'}}]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
