"""Multiply one frame with every row of another and read the cartesian product back.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.crossJoin", "DataFrame.cross_join"]


def main() -> None:
    """Run the measured cross-join answers: row count and the full product set."""
    repark = ReparkSession.builder.appName("ex-df-cross").master("local[1]").getOrCreate()
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
        right = repark.createDataFrame([(1, "x"), (2, "y"), (3, "z")], ["n", "label"])
        product = frame.select("k").crossJoin(right)
        assert product.count() == 18
        assert product.columns == ["k", "n", "label"]
        assert sorted(product.collect(), key=tuple) == [
            (1, 1, "x"),
            (1, 1, "x"),
            (1, 2, "y"),
            (1, 2, "y"),
            (1, 3, "z"),
            (1, 3, "z"),
            (2, 1, "x"),
            (2, 1, "x"),
            (2, 1, "x"),
            (2, 2, "y"),
            (2, 2, "y"),
            (2, 2, "y"),
            (2, 3, "z"),
            (2, 3, "z"),
            (2, 3, "z"),
            (3, 1, "x"),
            (3, 2, "y"),
            (3, 3, "z"),
        ]
        same = frame.select("k").cross_join(right)
        assert same.count() == 18
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
