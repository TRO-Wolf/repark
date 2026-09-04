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
        first_row = tuple(frame.first())
        if first_row != ("a", 1):
            raise SystemExit(f"DataFrame.first row {first_row!r} != ('a', 1)")
        head_row = tuple(frame.head())
        if head_row != ("a", 1):
            raise SystemExit(f"DataFrame.head row {head_row!r} != ('a', 1)")
        head_rows = [tuple(row) for row in frame.head(2)]
        if head_rows != [("a", 1), ("a", 2)]:
            raise SystemExit(f"DataFrame.head rows {head_rows!r} != [('a', 1), ('a', 2)]")
        zero_rows = frame.head(0)
        if zero_rows != []:
            raise SystemExit(f"DataFrame.head rows {zero_rows!r} != []")

        empty = repark.createDataFrame([], "g string, k long")
        first_empty = empty.first()
        if first_empty is not None:
            raise SystemExit(f"DataFrame.first row {first_empty!r} != None")
        head_empty = empty.head()
        if head_empty is not None:
            raise SystemExit(f"DataFrame.head row {head_empty!r} != None")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
