"""Rename frame columns one at a time and by map, both spellings.

pins: ex-19-dataframe-d-window/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.withColumnRenamed",
    "DataFrame.with_column_renamed",
    "DataFrame.withColumnsRenamed",
    "DataFrame.with_columns_renamed",
]


def main() -> None:
    """Run the measured rename answers, including the absent-name no-op."""
    repark = ReparkSession.builder.appName("ex-df-d-rename").master("local[1]").getOrCreate()
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
        renamed = base.withColumnRenamed("v", "u")
        renamed_columns = renamed.columns
        renamed_columns_expected = ["g", "k", "u"]
        if renamed_columns != renamed_columns_expected:
            raise SystemExit(
                f"DataFrame.withColumnRenamed columns {renamed_columns!r}"
                f" != {renamed_columns_expected!r}"
            )
        noop = base.withColumnRenamed("nope", "x")
        noop_columns = noop.columns
        noop_columns_expected = ["g", "k", "v"]
        if noop_columns != noop_columns_expected:
            raise SystemExit(
                f"DataFrame.withColumnRenamed columns {noop_columns!r} != {noop_columns_expected!r}"
            )

        renamed_rows = sorted(tuple(row) for row in renamed.collect())
        renamed_rows_expected = [
            ("a", 1, 10.0),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 3, 40.0),
            ("b", 1, 50.0),
            ("b", 2, None),
        ]
        if renamed_rows != renamed_rows_expected:
            raise SystemExit(
                f"DataFrame.withColumnRenamed rows {renamed_rows!r} != {renamed_rows_expected!r}"
            )

        snake_renamed = base.with_column_renamed("v", "u")
        snake_renamed_columns = snake_renamed.columns
        if snake_renamed_columns != renamed_columns_expected:
            raise SystemExit(
                f"DataFrame.with_column_renamed columns {snake_renamed_columns!r}"
                f" != {renamed_columns_expected!r}"
            )
        snake_renamed_rows = sorted(tuple(row) for row in snake_renamed.collect())
        if snake_renamed_rows != renamed_rows_expected:
            raise SystemExit(
                f"DataFrame.with_column_renamed rows {snake_renamed_rows!r}"
                f" != {renamed_rows_expected!r}"
            )

        mapped = base.withColumnsRenamed({"g": "gg", "k": "kk"})
        mapped_columns = mapped.columns
        mapped_columns_expected = ["gg", "kk", "v"]
        if mapped_columns != mapped_columns_expected:
            raise SystemExit(
                f"DataFrame.withColumnsRenamed columns {mapped_columns!r}"
                f" != {mapped_columns_expected!r}"
            )
        mapped_rows = sorted(tuple(row) for row in mapped.collect())
        if mapped_rows != renamed_rows_expected:
            raise SystemExit(
                f"DataFrame.withColumnsRenamed rows {mapped_rows!r} != {renamed_rows_expected!r}"
            )
        chained = base.withColumnsRenamed({"g": "gg", "k": "g"})
        chained_columns = chained.columns
        chained_columns_expected = ["gg", "g", "v"]
        if chained_columns != chained_columns_expected:
            raise SystemExit(
                f"DataFrame.withColumnsRenamed columns {chained_columns!r}"
                f" != {chained_columns_expected!r}"
            )
        chained_rows = sorted(tuple(row) for row in chained.collect())
        if chained_rows != renamed_rows_expected:
            raise SystemExit(
                f"DataFrame.withColumnsRenamed rows {chained_rows!r} != {renamed_rows_expected!r}"
            )

        snake_mapped = base.with_columns_renamed({"g": "gg", "k": "kk"})
        snake_mapped_columns = snake_mapped.columns
        if snake_mapped_columns != mapped_columns_expected:
            raise SystemExit(
                f"DataFrame.with_columns_renamed columns {snake_mapped_columns!r}"
                f" != {mapped_columns_expected!r}"
            )
        snake_mapped_rows = sorted(tuple(row) for row in snake_mapped.collect())
        if snake_mapped_rows != renamed_rows_expected:
            raise SystemExit(
                f"DataFrame.with_columns_renamed rows {snake_mapped_rows!r}"
                f" != {renamed_rows_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
