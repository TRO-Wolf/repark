"""Export one frame to Arrow tables, Arrow batches, and renamed columns.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.toArrow",
    "DataFrame.to_arrow",
    "DataFrame.toArrowBatches",
    "DataFrame.to_arrow_batches",
    "DataFrame.toDF",
    "DataFrame.to_df",
]


def main() -> None:
    """Run the measured Arrow, Arrow-batch, and toDF export answers."""
    repark = ReparkSession.builder.appName("ex-df-export-arrow").master("local[1]").getOrCreate()
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
        table = frame.toArrow()
        if table.column_names != ["g", "k", "v"]:
            raise SystemExit(f"DataFrame.toArrow columns {table.column_names!r} != ['g', 'k', 'v']")
        v_values = table.column("v").to_pylist()
        v_expected = [10.0, 20.0, 30.0, 40.0, 50.0, None]
        if v_values != v_expected:
            raise SystemExit(f"DataFrame.toArrow values {v_values!r} != {v_expected!r}")
        snake_table = frame.to_arrow()
        g_values = snake_table.column("g").to_pylist()
        g_expected = ["a", "a", "a", "a", "b", "b"]
        if g_values != g_expected:
            raise SystemExit(f"DataFrame.to_arrow values {g_values!r} != {g_expected!r}")

        batches = list(frame.toArrowBatches())
        batch_rows = sum(batch.num_rows for batch in batches)
        if batch_rows != 6:
            raise SystemExit(f"DataFrame.toArrowBatches rows {batch_rows!r} != 6")
        batch_names = batches[0].column_names
        if batch_names != ["g", "k", "v"]:
            raise SystemExit(f"DataFrame.toArrowBatches columns {batch_names!r} != ['g', 'k', 'v']")
        snake_batches = list(frame.to_arrow_batches())
        snake_rows = sum(batch.num_rows for batch in snake_batches)
        if snake_rows != 6:
            raise SystemExit(f"DataFrame.to_arrow_batches rows {snake_rows!r} != 6")

        renamed = frame.toDF("x", "y", "z")
        if renamed.columns != ["x", "y", "z"]:
            raise SystemExit(f"DataFrame.toDF columns {renamed.columns!r} != ['x', 'y', 'z']")
        renamed_rows = set(renamed.collect())
        renamed_expected = {
            ("a", 1, 10.0),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 3, 40.0),
            ("b", 1, 50.0),
            ("b", 2, None),
        }
        if renamed_rows != renamed_expected:
            raise SystemExit(f"DataFrame.toDF rows {renamed_rows!r} != {renamed_expected!r}")
        snake_columns = frame.to_df("x", "y", "z").columns
        if snake_columns != ["x", "y", "z"]:
            raise SystemExit(f"DataFrame.to_df columns {snake_columns!r} != ['x', 'y', 'z']")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
