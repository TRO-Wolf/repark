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
        first_expected = ("a", 1)
        if first_row != first_expected:
            raise SystemExit(f"DataFrame.first row {first_row!r} != {first_expected!r}")
        head_row = tuple(frame.head())
        head_expected = ("a", 1)
        if head_row != head_expected:
            raise SystemExit(f"DataFrame.head row {head_row!r} != {head_expected!r}")
        head_rows = [tuple(row) for row in frame.head(2)]
        head_rows_expected = [("a", 1), ("a", 2)]
        if head_rows != head_rows_expected:
            raise SystemExit(f"DataFrame.head rows {head_rows!r} != {head_rows_expected!r}")
        zero_rows = frame.head(0)
        zero_expected: list[tuple] = []
        if zero_rows != zero_expected:
            raise SystemExit(f"DataFrame.head rows {zero_rows!r} != {zero_expected!r}")

        empty = repark.createDataFrame([], "g string, k long")
        first_empty_expected = None
        first_empty = empty.first()
        if first_empty != first_empty_expected:
            raise SystemExit(f"DataFrame.first row {first_empty!r} != {first_empty_expected!r}")
        head_empty_expected = None
        head_empty = empty.head()
        if head_empty != head_empty_expected:
            raise SystemExit(f"DataFrame.head row {head_empty!r} != {head_empty_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
