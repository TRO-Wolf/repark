"""Repair nulls through the na surface: fill arms and drop arms.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrameNaFunctions.fill",
    "DataFrameNaFunctions.drop",
]


def row_key(row: tuple) -> tuple:
    return tuple((value is None, value) for value in row)


def main() -> None:
    """Run the measured fill and drop answers on sparse numeric and string frames."""
    repark = ReparkSession.builder.appName("ex-df-d-na-surface").master("local[1]").getOrCreate()
    try:
        sparse = repark.createDataFrame(
            [("a", 1, 10.0), ("a", None, 20.0), ("a", 2, None), ("b", 3, 30.0)],
            ["g", "k", "v"],
        )
        filled = sparse.na.fill(0.0)
        filled_rows = sorted(tuple(row) for row in filled.collect())
        filled_rows_expected = [
            ("a", 0, 20.0),
            ("a", 1, 10.0),
            ("a", 2, 0.0),
            ("b", 3, 30.0),
        ]
        if filled_rows != filled_rows_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.fill rows {filled_rows!r} != {filled_rows_expected!r}"
            )

        string_frame = repark.createDataFrame(
            [("a", 1, None), (None, 2, "y")],
            ["g", "k", "s"],
        )
        string_filled = string_frame.na.fill("zz")
        string_filled_columns = string_filled.columns
        string_filled_columns_expected = ["g", "k", "s"]
        if string_filled_columns != string_filled_columns_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.fill columns {string_filled_columns!r}"
                f" != {string_filled_columns_expected!r}"
            )
        string_filled_rows = sorted(tuple(row) for row in string_filled.collect())
        string_filled_rows_expected = [("a", 1, "zz"), ("zz", 2, "y")]
        if string_filled_rows != string_filled_rows_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.fill rows {string_filled_rows!r}"
                f" != {string_filled_rows_expected!r}"
            )

        string_subset = string_frame.na.fill("zz", subset=["g"])
        string_subset_rows = sorted(tuple(row) for row in string_subset.collect())
        string_subset_rows_expected = [("a", 1, None), ("zz", 2, "y")]
        if string_subset_rows != string_subset_rows_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.fill rows {string_subset_rows!r}"
                f" != {string_subset_rows_expected!r}"
            )

        dict_filled = sparse.na.fill({"v": -1.0, "k": -2})
        dict_filled_rows = sorted(tuple(row) for row in dict_filled.collect())
        dict_filled_rows_expected = [
            ("a", -2, 20.0),
            ("a", 1, 10.0),
            ("a", 2, -1.0),
            ("b", 3, 30.0),
        ]
        if dict_filled_rows != dict_filled_rows_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.fill rows {dict_filled_rows!r}"
                f" != {dict_filled_rows_expected!r}"
            )

        subset_filled = sparse.na.fill(0.0, subset=["k"])
        subset_filled_rows = sorted((tuple(row) for row in subset_filled.collect()), key=row_key)
        subset_filled_rows_expected = [
            ("a", 0, 20.0),
            ("a", 1, 10.0),
            ("a", 2, None),
            ("b", 3, 30.0),
        ]
        if subset_filled_rows != subset_filled_rows_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.fill rows {subset_filled_rows!r}"
                f" != {subset_filled_rows_expected!r}"
            )

        dropped = sparse.na.drop()
        dropped_rows = sorted(tuple(row) for row in dropped.collect())
        dropped_rows_expected = [("a", 1, 10.0), ("b", 3, 30.0)]
        if dropped_rows != dropped_rows_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.drop rows {dropped_rows!r} != {dropped_rows_expected!r}"
            )

        loosened = sparse.na.drop(thresh=1)
        loosened_rows = sorted((tuple(row) for row in loosened.collect()), key=row_key)
        loosened_rows_expected = [
            ("a", 1, 10.0),
            ("a", 2, None),
            ("a", None, 20.0),
            ("b", 3, 30.0),
        ]
        if loosened_rows != loosened_rows_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.drop rows {loosened_rows!r} != {loosened_rows_expected!r}"
            )

        tightened = sparse.na.drop(thresh=2, subset=["k", "v"])
        tightened_rows = sorted(tuple(row) for row in tightened.collect())
        if tightened_rows != dropped_rows_expected:
            raise SystemExit(
                f"DataFrameNaFunctions.drop rows {tightened_rows!r} != {dropped_rows_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
