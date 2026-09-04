"""Unpivot wide value columns into long variable and value rows.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.unpivot",
]


def row_key(row: tuple) -> tuple:
    return tuple((value is None, value) for value in row)


def main() -> None:
    """Run the measured unpivot answers for list and string ids and values, NULLs included."""
    repark = ReparkSession.builder.appName("ex-df-d-unpivot").master("local[1]").getOrCreate()
    try:
        wide = repark.createDataFrame(
            [("a", 1, 10.0, 100.0), ("b", 2, 20.0, 200.0)],
            ["g", "k", "x", "y"],
        )
        melted = wide.unpivot("g", ["x", "y"], "var", "val")
        melted_columns = melted.columns
        melted_columns_expected = ["g", "var", "val"]
        if melted_columns != melted_columns_expected:
            raise SystemExit(
                f"DataFrame.unpivot columns {melted_columns!r} != {melted_columns_expected!r}"
            )
        melted_rows = sorted(tuple(row) for row in melted.collect())
        melted_rows_expected = [
            ("a", "x", 10.0),
            ("a", "y", 100.0),
            ("b", "x", 20.0),
            ("b", "y", 200.0),
        ]
        if melted_rows != melted_rows_expected:
            raise SystemExit(f"DataFrame.unpivot rows {melted_rows!r} != {melted_rows_expected!r}")

        null_wide = repark.createDataFrame(
            [("a", 1, 10.0, None), ("b", 2, None, 200.0)],
            ["g", "k", "x", "y"],
        )
        null_melted = null_wide.unpivot("g", ["x", "y"], "var", "val")
        null_melted_rows = sorted((tuple(row) for row in null_melted.collect()), key=row_key)
        null_melted_rows_expected = [
            ("a", "x", 10.0),
            ("a", "y", None),
            ("b", "x", None),
            ("b", "y", 200.0),
        ]
        if null_melted_rows != null_melted_rows_expected:
            raise SystemExit(
                f"DataFrame.unpivot rows {null_melted_rows!r} != {null_melted_rows_expected!r}"
            )

        single = wide.unpivot("g", "x", "var", "val")
        single_rows = sorted(tuple(row) for row in single.collect())
        single_rows_expected = [("a", "x", 10.0), ("b", "x", 20.0)]
        if single_rows != single_rows_expected:
            raise SystemExit(f"DataFrame.unpivot rows {single_rows!r} != {single_rows_expected!r}")

        two_ids = wide.unpivot(["g", "k"], ["x", "y"], "var", "val")
        two_ids_rows = sorted(tuple(row) for row in two_ids.collect())
        two_ids_rows_expected = [
            ("a", 1, "x", 10.0),
            ("a", 1, "y", 100.0),
            ("b", 2, "x", 20.0),
            ("b", 2, "y", 200.0),
        ]
        if two_ids_rows != two_ids_rows_expected:
            raise SystemExit(
                f"DataFrame.unpivot rows {two_ids_rows!r} != {two_ids_rows_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
