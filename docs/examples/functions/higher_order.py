"""Demonstrate the ``F.*`` higher-order names, lambdas over array elements.

pins: ex-8-functions-arrays/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "F.exists",
    "F.forall",
    "F.filter",
    "F.transform",
    "F.aggregate",
    "F.reduce",
    "F.zip_with",
    "F.col",
    "F.lit",
]


def main() -> None:
    """Test, rewrite, and fold arrays with lambdas, NULL elements included."""
    repark = ReparkSession.builder.appName("ex-higher-order").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [([1, 3, 5], [10, 20, 30]), ([2, 4], [10, 20]), ([None, 1], [1]), (None, [7])],
            ["a", "b"],
        )
        rows = frame.select(
            F.col("a"),
            F.col("b"),
            F.exists(F.col("a"), lambda x: x > 2).alias("exists"),
            F.forall(F.col("a"), lambda x: x > 0).alias("forall"),
            F.filter(F.col("a"), lambda x: x >= 2).alias("filtered"),
            F.transform(F.col("a"), lambda x: x * 2).alias("doubled"),
            F.transform(F.col("a"), lambda x, i: x + i).alias("indexed"),
            F.zip_with(F.col("a"), F.col("b"), lambda x, y: x + y).alias("zipped"),
        ).collect()
        checked = (
            ("exists", [True, True, None, None]),
            ("forall", [True, True, None, None]),
            ("filtered", [[3, 5], [2, 4], [], None]),
            ("doubled", [[2, 6, 10], [4, 8], [None, 2], None]),
            ("indexed", [[1, 4, 7], [2, 5], [None, 2], None]),
            ("zipped", [[11, 23, 35], [12, 24], [None, None], None]),
        )
        for name, expected in checked:
            values = [row[name] for row in rows]
            print(f"F.{name}: {values!r}")
            if values != expected:
                raise SystemExit(f"F.{name} gave {values!r}, expected {expected!r}")
        initial = F.lit(0).cast("bigint")
        summed = [
            row["summed"]
            for row in frame.select(
                F.aggregate(F.col("a"), initial, lambda acc, x: acc + x).alias("summed")
            ).collect()
        ]
        print(f"F.aggregate: {summed!r}")
        if summed != [9, 6, None, None]:
            raise SystemExit(f"F.aggregate gave {summed!r}, expected [9, 6, None, None]")
        finished = [
            row["finished"]
            for row in frame.select(
                F.aggregate(
                    F.col("a"),
                    initial,
                    lambda acc, x: acc + x,
                    lambda total: total * 10,
                ).alias("finished")
            ).collect()
        ]
        print(f"F.aggregate with finish: {finished!r}")
        if finished != [90, 60, None, None]:
            raise SystemExit(f"F.aggregate with finish gave {finished!r}")
        reduced = [
            row["reduced"]
            for row in frame.select(
                F.reduce(F.col("a"), initial, lambda acc, x: acc + x).alias("reduced")
            ).collect()
        ]
        print(f"F.reduce: {reduced!r}")
        if reduced != summed:
            raise SystemExit("F.reduce is F.aggregate's alias and must agree exactly")
        emptied = [
            row["emptied"]
            for row in frame.select(
                F.aggregate(F.slice(F.col("a"), 1, 0), initial, lambda acc, x: acc + x).alias(
                    "emptied"
                )
            ).collect()
        ]
        print(f"F.aggregate over an empty array: {emptied!r}")
        if emptied != [0, 0, 0, None]:
            raise SystemExit(f"F.aggregate over an empty array gave {emptied!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
