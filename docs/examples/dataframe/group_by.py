"""Group rows by key and aggregate: the three groupBy spellings.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.groupBy",
    "DataFrame.group_by",
    "DataFrame.groupby",
]


def main() -> None:
    """Run the measured grouping answers: count, expression agg, and dict agg."""
    repark = ReparkSession.builder.appName("ex-df-b-group-by").master("local[1]").getOrCreate()
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
        counted = frame.groupBy("g").count()
        counted_names = counted.columns
        if counted_names != ["g", "count"]:
            raise SystemExit(f"DataFrame.groupBy columns {counted_names!r} != ['g', 'count']")
        counted_rows = set(counted.collect())
        counted_expected = {("a", 4), ("b", 2)}
        if counted_rows != counted_expected:
            raise SystemExit(f"DataFrame.groupBy rows {counted_rows!r} != {counted_expected!r}")

        totaled = frame.group_by("g").agg(F.sum("v"), F.count(F.lit(1)))
        totaled_names = totaled.columns
        if totaled_names != ["g", "sum(v)", "count(1)"]:
            raise SystemExit(
                f"DataFrame.group_by columns {totaled_names!r} != ['g', 'sum(v)', 'count(1)']"
            )
        totaled_rows = set(totaled.collect())
        totaled_expected = {("a", 100.0, 4), ("b", 50.0, 2)}
        if totaled_rows != totaled_expected:
            raise SystemExit(f"DataFrame.group_by rows {totaled_rows!r} != {totaled_expected!r}")

        peak = frame.groupby("g").agg({"v": "max"})
        peak_names = peak.columns
        if peak_names != ["g", "max(v)"]:
            raise SystemExit(f"DataFrame.groupby columns {peak_names!r} != ['g', 'max(v)']")
        peak_rows = set(peak.collect())
        peak_expected = {("a", 40.0), ("b", 50.0)}
        if peak_rows != peak_expected:
            raise SystemExit(f"DataFrame.groupby rows {peak_rows!r} != {peak_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
