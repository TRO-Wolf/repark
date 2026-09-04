"""Subtract one frame's rows from another and summarize a numeric frame.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.subtract", "DataFrame.summary"]


def main() -> None:
    """Run the measured subtract differences and the single-stat summary row."""
    repark = (
        ReparkSession.builder.appName("ex-df-subtract-summary").master("local[1]").getOrCreate()
    )
    try:
        left = repark.createDataFrame([(1,), (1,), (2,)], ["n"])
        right = repark.createDataFrame([(1,)], ["n"])
        remaining = left.subtract(right).collect()
        remaining_expected = [(2,)]
        if remaining != remaining_expected:
            raise SystemExit(f"DataFrame.subtract rows {remaining!r} != {remaining_expected!r}")

        dups = repark.createDataFrame(
            [(1, "x"), (1, "x"), (2, "y"), (3, "z"), (2, "y")],
            ["n", "s"],
        )
        other = repark.createDataFrame([(1, "x"), (3, "z")], ["n", "s"])
        string_rows = set(dups.subtract(other).collect())
        string_expected = {(2, "y")}
        if string_rows != string_expected:
            raise SystemExit(f"DataFrame.subtract rows {string_rows!r} != {string_expected!r}")

        stats = repark.createDataFrame(
            [(1, 10.0), (2, 20.0), (2, 30.0), (3, 40.0), (1, 50.0)],
            ["k", "v"],
        )
        counted = stats.summary("count").collect()
        counted_expected = [("count", "5", "5")]
        if counted != counted_expected:
            raise SystemExit(f"DataFrame.summary rows {counted!r} != {counted_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
