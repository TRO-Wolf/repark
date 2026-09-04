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
        assert totals.columns == ["max(v)", "count(1)"]
        assert totals.collect() == [(50.0, 6)]
        via_dict = frame.agg({"v": "max"})
        assert via_dict.columns == ["max(v)"]
        assert via_dict.collect() == [(50.0,)]

        stats = repark.createDataFrame(
            [(1, 10.0), (2, 20.0), (2, 30.0), (3, 40.0), (1, 50.0)],
            ["k", "v"],
        )
        assert stats.corr("k", "v") == 0.18898223650461363
        assert stats.corr("k", "v", "pearson") == 0.18898223650461363
        assert stats.cov("k", "v") == 2.5

        assert frame.approxQuantile("v", [0.5], 0.0) == [30.0]
        assert frame.approxQuantile(["k", "v"], [0.25, 0.5], 0.0) == [[1.0, 2.0], [20.0, 30.0]]

        strata = repark.createDataFrame(
            [("a", 1), ("a", 10), ("a", 2), ("b", 1), ("b", 10), ("b", 2)],
            ["g", "k"],
        )
        table = strata.crosstab("g", "k")
        assert table.columns == ["g_k", "1", "10", "2"]
        assert set(table.collect()) == {("a", 1, 1, 1), ("b", 1, 1, 1)}
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
