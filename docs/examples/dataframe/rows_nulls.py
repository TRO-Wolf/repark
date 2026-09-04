"""Shape rows: slice, order, unpivot, and repair nulls through the na surface.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.limit",
    "DataFrame.offset",
    "DataFrame.orderBy",
    "DataFrame.order_by",
    "DataFrame.melt",
    "DataFrame.na",
]


def main() -> None:
    """Run the measured shaping answers: limit, offset, ordering, melt, and na arms."""
    repark = ReparkSession.builder.appName("ex-df-b-rows-nulls").master("local[1]").getOrCreate()
    try:
        ordered = repark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c"), (4, "d")],
            ["k", "name"],
        )
        assert [tuple(row) for row in ordered.orderBy("k").limit(3).collect()] == [
            (1, "a"),
            (2, "b"),
            (3, "c"),
        ]
        assert ordered.limit(0).collect() == []

        skips = repark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c")],
            ["k", "name"],
        )
        assert [tuple(row) for row in skips.offset(2).collect()] == [(3, "c")]
        assert [tuple(row) for row in skips.offset(0).collect()] == [
            (1, "a"),
            (2, "b"),
            (3, "c"),
        ]

        nulls = repark.createDataFrame(
            [("a", None), ("a", 2), ("b", None), ("b", 1)],
            ["g", "k"],
        )
        assert [tuple(row) for row in nulls.orderBy("k").collect()] == [
            ("a", None),
            ("b", None),
            ("b", 1),
            ("a", 2),
        ]
        assert [tuple(row) for row in nulls.orderBy("k", ascending=False).collect()] == [
            ("a", 2),
            ("b", 1),
            ("a", None),
            ("b", None),
        ]
        assert [tuple(row) for row in nulls.order_by(F.col("k").desc()).collect()] == [
            ("a", 2),
            ("b", 1),
            ("a", None),
            ("b", None),
        ]

        wide = repark.createDataFrame(
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
        melted = wide.melt("g", ["k", "v"], "var", "val")
        assert melted.columns == ["g", "var", "val"]
        assert melted.dtypes == [("g", "string"), ("var", "string"), ("val", "double")]
        assert set(melted.collect()) == {
            ("a", "k", 1.0),
            ("a", "k", 2.0),
            ("a", "k", 2.0),
            ("a", "k", 3.0),
            ("a", "v", 10.0),
            ("a", "v", 20.0),
            ("a", "v", 30.0),
            ("a", "v", 40.0),
            ("b", "k", 1.0),
            ("b", "k", 2.0),
            ("b", "v", 50.0),
            ("b", "v", None),
        }

        sparse = repark.createDataFrame(
            [("a", 1, 10.0), ("a", None, 20.0), ("a", 2, None), ("b", 3, 30.0)],
            ["g", "k", "v"],
        )
        assert set(sparse.na.fill(0.0).collect()) == {
            ("a", 0, 20.0),
            ("a", 1, 10.0),
            ("a", 2, 0.0),
            ("b", 3, 30.0),
        }
        assert set(sparse.na.fill({"v": -1.0, "k": -2}).collect()) == {
            ("a", -2, 20.0),
            ("a", 1, 10.0),
            ("a", 2, -1.0),
            ("b", 3, 30.0),
        }
        assert set(sparse.na.drop().collect()) == {("a", 1, 10.0), ("b", 3, 30.0)}
        assert set(sparse.na.drop(subset=["v"]).collect()) == {
            ("a", 1, 10.0),
            ("a", None, 20.0),
            ("b", 3, 30.0),
        }
        assert set(sparse.na.drop(how="all").collect()) == {
            ("a", 1, 10.0),
            ("a", 2, None),
            ("a", None, 20.0),
            ("b", 3, 30.0),
        }
        assert set(sparse.na.drop(thresh=2).collect()) == {
            ("a", 1, 10.0),
            ("a", 2, None),
            ("a", None, 20.0),
            ("b", 3, 30.0),
        }
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
