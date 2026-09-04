"""Aggregate grouped rows: the expression and dict forms plus every shortcut.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "GroupedData.agg",
    "GroupedData.count",
    "GroupedData.sum",
    "GroupedData.avg",
    "GroupedData.mean",
    "GroupedData.min",
    "GroupedData.max",
]


def main() -> None:
    """Run the measured grouped-aggregate answers on one two-group frame."""
    repark = ReparkSession.builder.appName("ex-df-d-grouped-agg").master("local[1]").getOrCreate()
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

        totals = grouped.agg(F.sum("v"), F.count(F.lit(1)))
        totals_columns = totals.columns
        totals_columns_expected = ["g", "sum(v)", "count(1)"]
        if totals_columns != totals_columns_expected:
            raise SystemExit(
                f"GroupedData.agg columns {totals_columns!r} != {totals_columns_expected!r}"
            )
        totals_rows = sorted(tuple(row) for row in totals.collect())
        totals_rows_expected = [("a", 100.0, 4), ("b", 50.0, 2)]
        if totals_rows != totals_rows_expected:
            raise SystemExit(f"GroupedData.agg rows {totals_rows!r} != {totals_rows_expected!r}")

        via_dict = grouped.agg({"v": "max"})
        via_dict_columns = via_dict.columns
        via_dict_columns_expected = ["g", "max(v)"]
        if via_dict_columns != via_dict_columns_expected:
            raise SystemExit(
                f"GroupedData.agg columns {via_dict_columns!r} != {via_dict_columns_expected!r}"
            )
        via_dict_rows = sorted(tuple(row) for row in via_dict.collect())
        via_dict_rows_expected = [("a", 40.0), ("b", 50.0)]
        if via_dict_rows != via_dict_rows_expected:
            raise SystemExit(
                f"GroupedData.agg rows {via_dict_rows!r} != {via_dict_rows_expected!r}"
            )

        counted = grouped.count()
        counted_columns = counted.columns
        counted_columns_expected = ["g", "count"]
        if counted_columns != counted_columns_expected:
            raise SystemExit(
                f"GroupedData.count columns {counted_columns!r} != {counted_columns_expected!r}"
            )
        counted_rows = sorted(tuple(row) for row in counted.collect())
        counted_rows_expected = [("a", 4), ("b", 2)]
        if counted_rows != counted_rows_expected:
            raise SystemExit(
                f"GroupedData.count rows {counted_rows!r} != {counted_rows_expected!r}"
            )

        summed = grouped.sum("v")
        summed_rows = sorted(tuple(row) for row in summed.collect())
        summed_rows_expected = [("a", 100.0), ("b", 50.0)]
        if summed_rows != summed_rows_expected:
            raise SystemExit(f"GroupedData.sum rows {summed_rows!r} != {summed_rows_expected!r}")

        averaged = grouped.avg("v")
        averaged_rows = sorted(tuple(row) for row in averaged.collect())
        averaged_rows_expected = [("a", 25.0), ("b", 50.0)]
        if averaged_rows != averaged_rows_expected:
            raise SystemExit(
                f"GroupedData.avg rows {averaged_rows!r} != {averaged_rows_expected!r}"
            )

        mean_frame = grouped.mean("v")
        mean_frame_rows = sorted(tuple(row) for row in mean_frame.collect())
        if mean_frame_rows != averaged_rows_expected:
            raise SystemExit(
                f"GroupedData.mean rows {mean_frame_rows!r} != {averaged_rows_expected!r}"
            )

        minimum = grouped.min("v")
        minimum_rows = sorted(tuple(row) for row in minimum.collect())
        minimum_rows_expected = [("a", 10.0), ("b", 50.0)]
        if minimum_rows != minimum_rows_expected:
            raise SystemExit(f"GroupedData.min rows {minimum_rows!r} != {minimum_rows_expected!r}")

        maximum = grouped.max("v")
        maximum_rows = sorted(tuple(row) for row in maximum.collect())
        maximum_rows_expected = [("a", 40.0), ("b", 50.0)]
        if maximum_rows != maximum_rows_expected:
            raise SystemExit(f"GroupedData.max rows {maximum_rows!r} != {maximum_rows_expected!r}")

        all_summed = grouped.sum()
        all_summed_columns = all_summed.columns
        all_summed_columns_expected = ["g", "sum(k)", "sum(v)"]
        if all_summed_columns != all_summed_columns_expected:
            raise SystemExit(
                f"GroupedData.sum columns {all_summed_columns!r} != {all_summed_columns_expected!r}"
            )
        all_summed_rows = sorted(tuple(row) for row in all_summed.collect())
        all_summed_rows_expected = [("a", 8, 100.0), ("b", 3, 50.0)]
        if all_summed_rows != all_summed_rows_expected:
            raise SystemExit(
                f"GroupedData.sum rows {all_summed_rows!r} != {all_summed_rows_expected!r}"
            )

        all_averaged = grouped.avg()
        all_averaged_columns = all_averaged.columns
        all_averaged_columns_expected = ["g", "avg(k)", "avg(v)"]
        if all_averaged_columns != all_averaged_columns_expected:
            raise SystemExit(
                f"GroupedData.avg columns {all_averaged_columns!r}"
                f" != {all_averaged_columns_expected!r}"
            )
        all_averaged_rows = sorted(tuple(row) for row in all_averaged.collect())
        all_averaged_rows_expected = [("a", 2.0, 25.0), ("b", 1.5, 50.0)]
        if all_averaged_rows != all_averaged_rows_expected:
            raise SystemExit(
                f"GroupedData.avg rows {all_averaged_rows!r} != {all_averaged_rows_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
