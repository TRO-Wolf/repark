"""Inspect one frame's shape, materialize it with cache, and print its plan.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

import contextlib
import io

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.columns",
    "DataFrame.count",
    "DataFrame.dtypes",
    "DataFrame.cache",
    "DataFrame.coalesce",
    "DataFrame.explain",
]


def main() -> None:
    """Run the measured inspection answers and prove cache, coalesce, and explain run."""
    repark = ReparkSession.builder.appName("ex-df-inspect").master("local[1]").getOrCreate()
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
        assert frame.columns == ["g", "k", "v"]
        assert frame.dtypes == [("g", "string"), ("k", "bigint"), ("v", "double")]
        assert frame.count() == 6

        cached = frame.cache()
        assert cached.count() == 6
        assert cached.columns == ["g", "k", "v"]
        assert frame.coalesce(1).count() == 6

        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            frame.explain()
        assert printed.getvalue().strip() != ""
        costed = io.StringIO()
        with contextlib.redirect_stdout(costed):
            frame.explain(mode="cost")
        assert costed.getvalue().strip() != ""
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
