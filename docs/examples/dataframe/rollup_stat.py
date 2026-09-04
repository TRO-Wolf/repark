"""Roll one frame up its key hierarchy and read the stat accessor's pair-frequency table.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.rollup", "DataFrame.stat"]


def main() -> None:
    """Run the measured rollup grouping sets and the stat crosstab cells."""
    repark = ReparkSession.builder.appName("ex-df-rollup-stat").master("local[1]").getOrCreate()
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
        rolled = frame.rollup("g", "k").agg(F.sum("v"))
        if rolled.columns != ["g", "k", "sum(v)"]:
            raise SystemExit(f"DataFrame.rollup columns {rolled.columns!r} != ['g', 'k', 'sum(v)']")
        rolled_rows = set(rolled.collect())
        rolled_expected = {
            (None, None, 150.0),
            ("a", 1, 10.0),
            ("a", 2, 50.0),
            ("a", 3, 40.0),
            ("a", None, 100.0),
            ("b", 1, 50.0),
            ("b", 2, None),
            ("b", None, 50.0),
        }
        if rolled_rows != rolled_expected:
            raise SystemExit(f"DataFrame.rollup rows {rolled_rows!r} != {rolled_expected!r}")

        table = frame.stat.crosstab("g", "k")
        if table.columns != ["g_k", "1", "2", "3"]:
            raise SystemExit(f"DataFrame.stat columns {table.columns!r} != ['g_k', '1', '2', '3']")
        table_rows = set(table.collect())
        table_expected = {("a", 1, 2, 1), ("b", 1, 1, 0)}
        if table_rows != table_expected:
            raise SystemExit(f"DataFrame.stat rows {table_rows!r} != {table_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
