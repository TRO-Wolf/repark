"""Deduplicate rows, drop null-bearing rows, fill nulls, and drop columns by name.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.distinct",
    "DataFrame.dropDuplicates",
    "DataFrame.drop_duplicates",
    "DataFrame.dropna",
    "DataFrame.fillna",
    "DataFrame.drop",
]


def main() -> None:
    """Run the measured deduplication, null-handling, and column-drop answers."""
    repark = ReparkSession.builder.appName("ex-df-dedup-nulls").master("local[1]").getOrCreate()
    try:
        dupes = repark.createDataFrame(
            [(1, "x"), (1, "x"), (2, "y"), (3, "z"), (2, "y")],
            ["n", "label"],
        )
        assert set(dupes.distinct().collect()) == {(1, "x"), (2, "y"), (3, "z")}
        assert set(dupes.dropDuplicates().collect()) == {(1, "x"), (2, "y"), (3, "z")}
        assert set(dupes.dropDuplicates(["label"]).collect()) == {(1, "x"), (2, "y"), (3, "z")}
        assert set(dupes.drop_duplicates(("n",)).collect()) == {(1, "x"), (2, "y"), (3, "z")}

        sparse = repark.createDataFrame(
            [("a", 1, 10.0), ("a", None, 20.0), ("a", 2, None), ("b", 3, 30.0)],
            ["g", "k", "v"],
        )
        assert set(sparse.dropna().collect()) == {("a", 1, 10.0), ("b", 3, 30.0)}
        assert set(sparse.dropna(how="all").collect()) == {
            ("a", 1, 10.0),
            ("a", 2, None),
            ("a", None, 20.0),
            ("b", 3, 30.0),
        }
        assert set(sparse.dropna(subset=["v"]).collect()) == {
            ("a", 1, 10.0),
            ("a", None, 20.0),
            ("b", 3, 30.0),
        }
        assert set(sparse.dropna(thresh=2).collect()) == {
            ("a", 1, 10.0),
            ("a", 2, None),
            ("a", None, 20.0),
            ("b", 3, 30.0),
        }

        assert set(sparse.fillna(0.0).collect()) == {
            ("a", 0, 20.0),
            ("a", 1, 10.0),
            ("a", 2, 0.0),
            ("b", 3, 30.0),
        }
        assert set(sparse.fillna({"v": -1.0, "k": -2}).collect()) == {
            ("a", -2, 20.0),
            ("a", 1, 10.0),
            ("a", 2, -1.0),
            ("b", 3, 30.0),
        }
        assert set(sparse.fillna(0.0, subset=["v"]).collect()) == {
            ("a", 1, 10.0),
            ("a", 2, 0.0),
            ("a", None, 20.0),
            ("b", 3, 30.0),
        }

        frame = repark.createDataFrame(
            [("a", 1, 10.0), ("b", 2, 20.0)],
            ["g", "k", "v"],
        )
        assert frame.drop("v").columns == ["g", "k"]
        assert frame.drop("nope").columns == ["g", "k", "v"]
        assert frame.drop("v", "k").columns == ["g"]
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
