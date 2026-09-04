"""Measure column statistics through the stat helper surface.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameStatFunctions.approxQuantile",
    "DataFrameStatFunctions.corr",
    "DataFrameStatFunctions.cov",
    "DataFrameStatFunctions.crosstab",
    "DataFrameStatFunctions.sampleBy",
]


def main() -> None:
    """Run the measured stat answers: quantiles, correlation, covariance, table, sample."""
    repark = ReparkSession.builder.appName("ex-df-d-stat-helpers").master("local[1]").getOrCreate()
    try:
        stats = repark.createDataFrame(
            [(1, 10.0), (2, 20.0), (2, 30.0), (3, 40.0), (1, 50.0)],
            ["k", "v"],
        )
        median = stats.stat.approxQuantile("v", [0.5], 0.0)
        median_expected = [30.0]
        if median != median_expected:
            raise SystemExit(
                f"DataFrameStatFunctions.approxQuantile {median!r} != {median_expected!r}"
            )
        pearson = stats.stat.corr("k", "v")
        pearson_expected = 0.18898223650461363
        if pearson != pearson_expected:
            raise SystemExit(f"DataFrameStatFunctions.corr {pearson!r} != {pearson_expected!r}")
        sample_cov = stats.stat.cov("k", "v")
        sample_cov_expected = 2.5
        if sample_cov != sample_cov_expected:
            raise SystemExit(
                f"DataFrameStatFunctions.cov {sample_cov!r} != {sample_cov_expected!r}"
            )

        strata = repark.createDataFrame(
            [("a", 1), ("a", 10), ("a", 2), ("b", 1), ("b", 2)],
            ["g", "k"],
        )
        table = strata.stat.crosstab("g", "k")
        table_columns = table.columns
        table_columns_expected = ["g_k", "1", "10", "2"]
        if table_columns != table_columns_expected:
            raise SystemExit(
                f"DataFrameStatFunctions.crosstab columns {table_columns!r}"
                f" != {table_columns_expected!r}"
            )
        table_rows = sorted(tuple(row) for row in table.collect())
        table_rows_expected = [("a", 1, 1, 1), ("b", 1, 0, 1)]
        if table_rows != table_rows_expected:
            raise SystemExit(
                f"DataFrameStatFunctions.crosstab rows {table_rows!r} != {table_rows_expected!r}"
            )

        sampleby = repark.createDataFrame(
            [(1, 10.0), (1, 11.0), (2, 20.0), (3, 30.0)],
            ["k", "v"],
        )
        certain = sampleby.stat.sampleBy("k", {1: 1.0, 2: 0.0, 3: 0.0}, 42)
        certain_rows = sorted(tuple(row) for row in certain.collect())
        certain_rows_expected = [(1, 10.0), (1, 11.0)]
        if certain_rows != certain_rows_expected:
            raise SystemExit(
                f"DataFrameStatFunctions.sampleBy rows {certain_rows!r}"
                f" != {certain_rows_expected!r}"
            )
        flipped = sampleby.stat.sampleBy("k", {1: 0.0, 2: 0.0, 3: 1.0}, 7)
        flipped_rows = sorted(tuple(row) for row in flipped.collect())
        flipped_rows_expected = [(3, 30.0)]
        if flipped_rows != flipped_rows_expected:
            raise SystemExit(
                f"DataFrameStatFunctions.sampleBy rows {flipped_rows!r}"
                f" != {flipped_rows_expected!r}"
            )
        probabilistic = sampleby.stat.sampleBy("k", {1: 1.0, 2: 0.5, 3: 0.5}, 42)
        sampled = {tuple(row) for row in probabilistic.collect()}
        universe = {tuple(row) for row in sampleby.collect()}
        kept = {(1, 10.0), (1, 11.0)}
        if not kept <= sampled:
            raise SystemExit(
                f"DataFrameStatFunctions.sampleBy rows {sorted(sampled)!r}"
                f" misses {sorted(kept - sampled)!r}"
            )
        if not sampled <= universe:
            raise SystemExit(
                f"DataFrameStatFunctions.sampleBy rows {sorted(sampled - universe)!r}"
                f" outside the frame"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
