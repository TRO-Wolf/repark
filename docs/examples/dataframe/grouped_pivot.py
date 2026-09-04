"""Pivot grouped rows and run a pandas function per group.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

import pandas as pd

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "GroupedData.pivot",
    "GroupedData.applyInPandas",
    "GroupedData.apply_in_pandas",
]


def append_w(pdf: pd.DataFrame) -> pd.DataFrame:
    out = pdf.copy()
    out["w"] = out["v"] + 1.0
    return out


def main() -> None:
    """Run the measured pivot and per-group pandas answers."""
    repark = ReparkSession.builder.appName("ex-df-d-grouped-pivot").master("local[1]").getOrCreate()
    try:
        base = repark.createDataFrame(
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
        grouped = base.groupBy("g")

        pivoted = grouped.pivot("k", [1, 2]).sum("v")
        pivoted_columns = pivoted.columns
        pivoted_columns_expected = ["g", "1", "2"]
        if pivoted_columns != pivoted_columns_expected:
            raise SystemExit(
                f"GroupedData.pivot columns {pivoted_columns!r} != {pivoted_columns_expected!r}"
            )
        pivoted_rows = sorted(tuple(row) for row in pivoted.collect())
        pivoted_rows_expected = [("a", 10.0, 50.0), ("b", 50.0, None)]
        if pivoted_rows != pivoted_rows_expected:
            raise SystemExit(
                f"GroupedData.pivot rows {pivoted_rows!r} != {pivoted_rows_expected!r}"
            )

        discovered = grouped.pivot("k").sum("v")
        discovered_columns = discovered.columns
        discovered_columns_expected = ["g", "1", "2", "3"]
        if discovered_columns != discovered_columns_expected:
            raise SystemExit(
                f"GroupedData.pivot columns {discovered_columns!r}"
                f" != {discovered_columns_expected!r}"
            )
        discovered_rows = sorted(tuple(row) for row in discovered.collect())
        discovered_rows_expected = [("a", 10.0, 50.0, 40.0), ("b", 50.0, None, None)]
        if discovered_rows != discovered_rows_expected:
            raise SystemExit(
                f"GroupedData.pivot rows {discovered_rows!r} != {discovered_rows_expected!r}"
            )

        multi = grouped.pivot("k", [1, 2]).agg(F.sum("v"), F.count(F.lit(1)))
        multi_columns = multi.columns
        multi_columns_expected = ["g", "1_sum(v)", "1_count(1)", "2_sum(v)", "2_count(1)"]
        if multi_columns != multi_columns_expected:
            raise SystemExit(
                f"GroupedData.pivot columns {multi_columns!r} != {multi_columns_expected!r}"
            )
        multi_rows = sorted(tuple(row) for row in multi.collect())
        multi_rows_expected = [("a", 10.0, 1, 50.0, 2), ("b", 50.0, 1, None, 1)]
        if multi_rows != multi_rows_expected:
            raise SystemExit(f"GroupedData.pivot rows {multi_rows!r} != {multi_rows_expected!r}")

        per_group = repark.createDataFrame(
            [("a", 1, 10.0), ("a", 2, 20.0), ("b", 1, 50.0)],
            ["g", "k", "v"],
        )
        bridged = per_group.groupBy("g").applyInPandas(
            append_w,
            "g string, k bigint, v double, w double",
        )
        bridged_rows = sorted(tuple(row) for row in bridged.collect())
        bridged_rows_expected = [
            ("a", 1, 10.0, 11.0),
            ("a", 2, 20.0, 21.0),
            ("b", 1, 50.0, 51.0),
        ]
        if bridged_rows != bridged_rows_expected:
            raise SystemExit(
                f"GroupedData.applyInPandas rows {bridged_rows!r} != {bridged_rows_expected!r}"
            )

        snake_bridged = per_group.groupBy("g").apply_in_pandas(
            append_w,
            "g string, k bigint, v double, w double",
        )
        snake_bridged_rows = sorted(tuple(row) for row in snake_bridged.collect())
        if snake_bridged_rows != bridged_rows_expected:
            raise SystemExit(
                f"GroupedData.apply_in_pandas rows {snake_bridged_rows!r}"
                f" != {bridged_rows_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
