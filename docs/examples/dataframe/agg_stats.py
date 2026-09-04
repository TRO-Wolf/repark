"""Aggregate one frame and measure column statistics, summary tables, and quantiles.

pins: ex-15-dataframe-a/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.agg",
    "DataFrame.corr",
    "DataFrame.cov",
    "DataFrame.approxQuantile",
    "DataFrame.crosstab",
]


def main() -> None:
    """Run the measured aggregation and statistics answers on one local frame."""
    repark = ReparkSession.builder.appName("ex-df-agg-stats").master("local[1]").getOrCreate()
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
        totals = frame.agg(F.max("v"), F.count(F.lit(1)))
        if totals.columns != ["max(v)", "count(1)"]:
            raise SystemExit(f"DataFrame.agg columns {totals.columns!r} != ['max(v)', 'count(1)']")
        total_rows = totals.collect()
        if total_rows != [(50.0, 6)]:
            raise SystemExit(f"DataFrame.agg rows {total_rows!r} != [(50.0, 6)]")
        via_dict = frame.agg({"v": "max"})
        if via_dict.columns != ["max(v)"]:
            raise SystemExit(f"DataFrame.agg columns {via_dict.columns!r} != ['max(v)']")
        dict_rows = via_dict.collect()
        if dict_rows != [(50.0,)]:
            raise SystemExit(f"DataFrame.agg rows {dict_rows!r} != [(50.0,)]")

        stats = repark.createDataFrame(
            [(1, 10.0), (2, 20.0), (2, 30.0), (3, 40.0), (1, 50.0)],
            ["k", "v"],
        )
        pearson = stats.corr("k", "v")
        if pearson != 0.18898223650461363:
            raise SystemExit(f"DataFrame.corr {pearson!r} != 0.18898223650461363")
        pearson_named = stats.corr("k", "v", "pearson")
        if pearson_named != 0.18898223650461363:
            raise SystemExit(f"DataFrame.corr {pearson_named!r} != 0.18898223650461363")
        sample_cov = stats.cov("k", "v")
        if sample_cov != 2.5:
            raise SystemExit(f"DataFrame.cov {sample_cov!r} != 2.5")

        median = frame.approxQuantile("v", [0.5], 0.0)
        if median != [30.0]:
            raise SystemExit(f"DataFrame.approxQuantile {median!r} != [30.0]")
        quartiles = frame.approxQuantile(["k", "v"], [0.25, 0.5], 0.0)
        quartiles_expected = [[1.0, 2.0], [20.0, 30.0]]
        if quartiles != quartiles_expected:
            raise SystemExit(f"DataFrame.approxQuantile {quartiles!r} != {quartiles_expected!r}")

        strata = repark.createDataFrame(
            [("a", 1), ("a", 10), ("a", 2), ("b", 1), ("b", 2)],
            ["g", "k"],
        )
        table = strata.crosstab("g", "k")
        if table.columns != ["g_k", "1", "10", "2"]:
            raise SystemExit(
                f"DataFrame.crosstab columns {table.columns!r} != ['g_k', '1', '10', '2']"
            )
        strata_rows = set(table.collect())
        strata_expected = {("a", 1, 1, 1), ("b", 1, 0, 1)}
        if strata_rows != strata_expected:
            raise SystemExit(f"DataFrame.crosstab rows {strata_rows!r} != {strata_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
