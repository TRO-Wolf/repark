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
        names = frame.columns
        if names != ["g", "k", "v"]:
            raise SystemExit(f"DataFrame.columns {names!r} != ['g', 'k', 'v']")
        schema = frame.dtypes
        schema_expected = [("g", "string"), ("k", "bigint"), ("v", "double")]
        if schema != schema_expected:
            raise SystemExit(f"DataFrame.dtypes {schema!r} != {schema_expected!r}")
        total = frame.count()
        if total != 6:
            raise SystemExit(f"DataFrame.count {total!r} != 6")

        cached = frame.cache()
        cached_total = cached.count()
        if cached_total != 6:
            raise SystemExit(f"DataFrame.cache count {cached_total!r} != 6")
        cached_names = cached.columns
        if cached_names != ["g", "k", "v"]:
            raise SystemExit(f"DataFrame.cache columns {cached_names!r} != ['g', 'k', 'v']")
        coalesced_total = frame.coalesce(1).count()
        if coalesced_total != 6:
            raise SystemExit(f"DataFrame.coalesce count {coalesced_total!r} != 6")

        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            frame.explain()
        if printed.getvalue().strip() == "":
            raise SystemExit("DataFrame.explain printed an empty plan")
        costed = io.StringIO()
        with contextlib.redirect_stdout(costed):
            frame.explain(mode="cost")
        if costed.getvalue().strip() == "":
            raise SystemExit("DataFrame.explain printed an empty cost plan")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
