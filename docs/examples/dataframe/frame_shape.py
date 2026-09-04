"""Reshape one frame: a callable transform and the column add and replace arms.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

import repark.functions as F  # noqa: N812
from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.transform",
    "DataFrame.withColumn",
    "DataFrame.with_column",
    "DataFrame.withColumns",
    "DataFrame.with_columns",
]


def row_key(row: tuple) -> tuple:
    return tuple((value is None, value) for value in row)


def main() -> None:
    """Run the measured transform and column add, replace, and swap answers."""
    repark = ReparkSession.builder.appName("ex-df-d-frame-shape").master("local[1]").getOrCreate()
    try:
        left = repark.createDataFrame([("a", 1), ("b", 2)], ["g", "k"])
        transformed = left.transform(lambda frame, n: frame.filter(frame["k"] < n), 2)
        transformed_rows = sorted(tuple(row) for row in transformed.collect())
        transformed_expected = [("a", 1)]
        if transformed_rows != transformed_expected:
            raise SystemExit(
                f"DataFrame.transform rows {transformed_rows!r} != {transformed_expected!r}"
            )

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
        added = base.withColumn("w", F.col("v") + F.lit(1.0))
        added_columns = added.columns
        added_columns_expected = ["g", "k", "v", "w"]
        if added_columns != added_columns_expected:
            raise SystemExit(
                f"DataFrame.withColumn columns {added_columns!r} != {added_columns_expected!r}"
            )
        added_rows = sorted((tuple(row) for row in added.collect()), key=row_key)
        added_rows_expected = [
            ("a", 1, 10.0, 11.0),
            ("a", 2, 20.0, 21.0),
            ("a", 2, 30.0, 31.0),
            ("a", 3, 40.0, 41.0),
            ("b", 1, 50.0, 51.0),
            ("b", 2, None, None),
        ]
        if added_rows != added_rows_expected:
            raise SystemExit(f"DataFrame.withColumn rows {added_rows!r} != {added_rows_expected!r}")

        replaced = base.withColumn("k", F.col("k") * F.lit(10))
        replaced_rows = sorted((tuple(row) for row in replaced.collect()), key=row_key)
        replaced_rows_expected = [
            ("a", 10, 10.0),
            ("a", 20, 20.0),
            ("a", 20, 30.0),
            ("a", 30, 40.0),
            ("b", 10, 50.0),
            ("b", 20, None),
        ]
        if replaced_rows != replaced_rows_expected:
            raise SystemExit(
                f"DataFrame.withColumn rows {replaced_rows!r} != {replaced_rows_expected!r}"
            )

        snake_added = base.with_column("w", F.col("v") + F.lit(1.0))
        snake_added_rows = sorted((tuple(row) for row in snake_added.collect()), key=row_key)
        if snake_added_rows != added_rows_expected:
            raise SystemExit(
                f"DataFrame.with_column rows {snake_added_rows!r} != {added_rows_expected!r}"
            )

        swap = repark.createDataFrame([(1, 10)], ["a", "b"])
        swapped = swap.withColumns({"a": F.col("b"), "b": F.col("a")})
        swapped_rows = sorted(tuple(row) for row in swapped.collect())
        swapped_rows_expected = [(10, 1)]
        if swapped_rows != swapped_rows_expected:
            raise SystemExit(
                f"DataFrame.withColumns rows {swapped_rows!r} != {swapped_rows_expected!r}"
            )
        newcol = swap.withColumns({"c": F.col("a") + F.col("b")})
        newcol_columns = newcol.columns
        newcol_columns_expected = ["a", "b", "c"]
        if newcol_columns != newcol_columns_expected:
            raise SystemExit(
                f"DataFrame.withColumns columns {newcol_columns!r} != {newcol_columns_expected!r}"
            )
        newcol_rows = sorted(tuple(row) for row in newcol.collect())
        newcol_rows_expected = [(1, 10, 11)]
        if newcol_rows != newcol_rows_expected:
            raise SystemExit(
                f"DataFrame.withColumns rows {newcol_rows!r} != {newcol_rows_expected!r}"
            )

        snake_swapped = swap.with_columns({"a": F.col("b"), "b": F.col("a")})
        snake_swapped_rows = sorted(tuple(row) for row in snake_swapped.collect())
        if snake_swapped_rows != swapped_rows_expected:
            raise SystemExit(
                f"DataFrame.with_columns rows {snake_swapped_rows!r} != {swapped_rows_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
