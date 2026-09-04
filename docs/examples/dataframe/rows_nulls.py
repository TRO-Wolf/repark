"""Shape rows: slice, order, unpivot, and repair nulls through the na surface.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.limit",
    "DataFrame.offset",
    "DataFrame.orderBy",
    "DataFrame.order_by",
    "DataFrame.melt",
    "DataFrame.na",
]


def main() -> None:
    """Run the measured shaping answers: limit, offset, ordering, melt, and na arms."""
    repark = ReparkSession.builder.appName("ex-df-b-rows-nulls").master("local[1]").getOrCreate()
    try:
        ordered = repark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c"), (4, "d")],
            ["k", "name"],
        )
        limited_rows = [tuple(row) for row in ordered.orderBy("k").limit(3).collect()]
        limited_expected = [(1, "a"), (2, "b"), (3, "c")]
        if limited_rows != limited_expected:
            raise SystemExit(f"DataFrame.limit rows {limited_rows!r} != {limited_expected!r}")
        zero_rows = ordered.limit(0).collect()
        zero_expected: list[tuple] = []
        if zero_rows != zero_expected:
            raise SystemExit(f"DataFrame.limit rows {zero_rows!r} != {zero_expected!r}")

        skips = repark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c")],
            ["k", "name"],
        )
        offset_rows = [tuple(row) for row in skips.offset(2).collect()]
        offset_expected = [(3, "c")]
        if offset_rows != offset_expected:
            raise SystemExit(f"DataFrame.offset rows {offset_rows!r} != {offset_expected!r}")
        kept_rows = [tuple(row) for row in skips.offset(0).collect()]
        kept_expected = [(1, "a"), (2, "b"), (3, "c")]
        if kept_rows != kept_expected:
            raise SystemExit(f"DataFrame.offset rows {kept_rows!r} != {kept_expected!r}")

        nulls = repark.createDataFrame(
            [("a", None), ("a", 2), ("b", None), ("b", 1)],
            ["g", "k"],
        )
        asc_rows = [tuple(row) for row in nulls.orderBy("k").collect()]
        asc_expected = [("a", None), ("b", None), ("b", 1), ("a", 2)]
        if asc_rows != asc_expected:
            raise SystemExit(f"DataFrame.orderBy rows {asc_rows!r} != {asc_expected!r}")
        desc_rows = [tuple(row) for row in nulls.orderBy("k", ascending=False).collect()]
        desc_expected = [("a", 2), ("b", 1), ("a", None), ("b", None)]
        if desc_rows != desc_expected:
            raise SystemExit(f"DataFrame.orderBy rows {desc_rows!r} != {desc_expected!r}")
        desc_col_rows = [tuple(row) for row in nulls.order_by(F.col("k").desc()).collect()]
        if desc_col_rows != desc_expected:
            raise SystemExit(f"DataFrame.order_by rows {desc_col_rows!r} != {desc_expected!r}")

        wide = repark.createDataFrame(
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
        melted = wide.melt("g", ["k", "v"], "var", "val")
        melted_names = melted.columns
        if melted_names != ["g", "var", "val"]:
            raise SystemExit(f"DataFrame.melt columns {melted_names!r} != ['g', 'var', 'val']")
        melted_types = melted.dtypes
        melted_types_expected = [("g", "string"), ("var", "string"), ("val", "double")]
        if melted_types != melted_types_expected:
            raise SystemExit(f"DataFrame.melt dtypes {melted_types!r} != {melted_types_expected!r}")
        melted_rows = sorted(
            melted.collect(),
            key=lambda row: (
                row["g"],
                row["var"],
                row["val"] is None,
                row["val"] if row["val"] is not None else 0.0,
            ),
        )
        melted_expected = [
            ("a", "k", 1.0),
            ("a", "k", 2.0),
            ("a", "k", 2.0),
            ("a", "k", 3.0),
            ("a", "v", 10.0),
            ("a", "v", 20.0),
            ("a", "v", 30.0),
            ("a", "v", 40.0),
            ("b", "k", 1.0),
            ("b", "k", 2.0),
            ("b", "v", 50.0),
            ("b", "v", None),
        ]
        if melted_rows != melted_expected:
            raise SystemExit(f"DataFrame.melt rows {melted_rows!r} != {melted_expected!r}")

        sparse = repark.createDataFrame(
            [("a", 1, 10.0), ("a", None, 20.0), ("a", 2, None), ("b", 3, 30.0)],
            ["g", "k", "v"],
        )
        filled_rows = set(sparse.na.fill(0.0).collect())
        filled_expected = {("a", 0, 20.0), ("a", 1, 10.0), ("a", 2, 0.0), ("b", 3, 30.0)}
        if filled_rows != filled_expected:
            raise SystemExit(f"DataFrame.na rows {filled_rows!r} != {filled_expected!r}")
        dict_filled_rows = set(sparse.na.fill({"v": -1.0, "k": -2}).collect())
        dict_filled_expected = {("a", -2, 20.0), ("a", 1, 10.0), ("a", 2, -1.0), ("b", 3, 30.0)}
        if dict_filled_rows != dict_filled_expected:
            raise SystemExit(f"DataFrame.na rows {dict_filled_rows!r} != {dict_filled_expected!r}")
        dropped_rows = set(sparse.na.drop().collect())
        dropped_expected = {("a", 1, 10.0), ("b", 3, 30.0)}
        if dropped_rows != dropped_expected:
            raise SystemExit(f"DataFrame.na rows {dropped_rows!r} != {dropped_expected!r}")
        subset_rows = set(sparse.na.drop(subset=["v"]).collect())
        subset_expected = {("a", 1, 10.0), ("a", None, 20.0), ("b", 3, 30.0)}
        if subset_rows != subset_expected:
            raise SystemExit(f"DataFrame.na rows {subset_rows!r} != {subset_expected!r}")
        all_rows = set(sparse.na.drop(how="all").collect())
        all_expected = {("a", 1, 10.0), ("a", 2, None), ("a", None, 20.0), ("b", 3, 30.0)}
        if all_rows != all_expected:
            raise SystemExit(f"DataFrame.na rows {all_rows!r} != {all_expected!r}")
        thresh_rows = set(sparse.na.drop(thresh=2).collect())
        if thresh_rows != all_expected:
            raise SystemExit(f"DataFrame.na rows {thresh_rows!r} != {all_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
