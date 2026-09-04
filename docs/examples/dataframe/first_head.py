"""Row-first access: the first row and the first n rows of a frame.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.first",
    "DataFrame.head",
]


def main() -> None:
    """Run the measured first-row answers, including the empty-frame arms."""
    repark = ReparkSession.builder.appName("ex-df-b-first-head").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 3)],
            ["g", "k"],
        )
        assert tuple(frame.first()) == ("a", 1)
        assert tuple(frame.head()) == ("a", 1)
        assert [tuple(row) for row in frame.head(2)] == [("a", 1), ("a", 2)]
        assert frame.head(0) == []

        empty = repark.createDataFrame([], "g string, k long")
        assert empty.first() is None
        assert empty.head() is None
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
