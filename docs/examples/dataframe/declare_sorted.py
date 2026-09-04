"""Declare a frame already sorted, and watch the engine verify the claim row by row.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

import contextlib

from repark.errors import AnalysisException
from repark.spark import ReparkSession

COVERS: list[str] = ["DataFrame.declareSorted", "DataFrame.declare_sorted"]


def main() -> None:
    """Run the measured declareSorted answers: verified sorted input and refused unsorted input."""
    repark = ReparkSession.builder.appName("ex-df-declare-sorted").master("local[1]").getOrCreate()
    try:
        ordered = repark.createDataFrame(
            [(1, 5.0), (2, 6.0), (2, 7.0), (3, 8.0)],
            ["k", "x"],
        )
        declared_rows = ordered.declareSorted("k").collect()
        declared_expected = [(1, 5.0), (2, 6.0), (2, 7.0), (3, 8.0)]
        if declared_rows != declared_expected:
            raise SystemExit(
                f"DataFrame.declareSorted rows {declared_rows!r} != {declared_expected!r}"
            )
        multi = ordered.declare_sorted("k", "x")
        multi_count = multi.count()
        if multi_count != 4:
            raise SystemExit(f"DataFrame.declare_sorted count {multi_count!r} != 4")

        scrambled = repark.createDataFrame([(2, 5.0), (1, 6.0)], ["k", "x"])
        completed = False
        with contextlib.suppress(AnalysisException):
            scrambled.declareSorted("k")
            completed = True
        if completed:
            raise SystemExit("DataFrame.declareSorted accepted unsorted input")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
