"""Export one frame to local Python, pandas, numpy, and polars containers.

pins: ex-18-dataframe-c/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.toLocalIterator",
    "DataFrame.to_local_iterator",
    "DataFrame.toPandas",
    "DataFrame.to_pandas",
    "DataFrame.to_numpy",
    "DataFrame.to_polars",
]


def main() -> None:
    """Run the measured local-iterator, pandas, numpy, and polars export answers."""
    repark = ReparkSession.builder.appName("ex-df-export-local").master("local[1]").getOrCreate()
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
        ordered = frame.sort("k", "v")
        iterated = [tuple(row) for row in ordered.toLocalIterator()]
        iterated_expected = [
            ("a", 1, 10.0),
            ("b", 1, 50.0),
            ("b", 2, None),
            ("a", 2, 20.0),
            ("a", 2, 30.0),
            ("a", 3, 40.0),
        ]
        if iterated != iterated_expected:
            raise SystemExit(
                f"DataFrame.toLocalIterator rows {iterated!r} != {iterated_expected!r}"
            )
        snake_iterated = [tuple(row) for row in ordered.to_local_iterator()]
        if snake_iterated != iterated_expected:
            raise SystemExit(
                f"DataFrame.to_local_iterator rows {snake_iterated!r} != {iterated_expected!r}"
            )

        stats = repark.createDataFrame(
            [(1, 10.0), (2, 20.0), (2, 30.0), (3, 40.0), (1, 50.0)],
            ["k", "v"],
        )
        pdf = stats.toPandas()
        if list(pdf.columns) != ["k", "v"]:
            raise SystemExit(f"DataFrame.toPandas columns {list(pdf.columns)!r} != ['k', 'v']")
        pdf_values = pdf["v"].tolist()
        pdf_expected = [10.0, 20.0, 30.0, 40.0, 50.0]
        if pdf_values != pdf_expected:
            raise SystemExit(f"DataFrame.toPandas values {pdf_values!r} != {pdf_expected!r}")
        dtypes = {name: str(dtype) for name, dtype in pdf.dtypes.items()}
        dtypes_expected = {"k": "int64", "v": "float64"}
        if dtypes != dtypes_expected:
            raise SystemExit(f"DataFrame.toPandas dtypes {dtypes!r} != {dtypes_expected!r}")
        snake_pdf = stats.to_pandas()
        snake_k = snake_pdf["k"].tolist()
        snake_k_expected = [1, 2, 2, 3, 1]
        if snake_k != snake_k_expected:
            raise SystemExit(f"DataFrame.to_pandas values {snake_k!r} != {snake_k_expected!r}")

        matrix = stats.to_numpy()
        if matrix.shape != (5, 2):
            raise SystemExit(f"DataFrame.to_numpy shape {matrix.shape!r} != (5, 2)")
        matrix_values = matrix.tolist()
        matrix_expected = [
            [1.0, 10.0],
            [2.0, 20.0],
            [2.0, 30.0],
            [3.0, 40.0],
            [1.0, 50.0],
        ]
        if matrix_values != matrix_expected:
            raise SystemExit(f"DataFrame.to_numpy values {matrix_values!r} != {matrix_expected!r}")

        pl_frame = stats.to_polars()
        if pl_frame.columns != ["k", "v"]:
            raise SystemExit(f"DataFrame.to_polars columns {pl_frame.columns!r} != ['k', 'v']")
        pl_rows = pl_frame.rows()
        pl_rows_expected = [(1, 10.0), (2, 20.0), (2, 30.0), (3, 40.0), (1, 50.0)]
        if pl_rows != pl_rows_expected:
            raise SystemExit(f"DataFrame.to_polars rows {pl_rows!r} != {pl_rows_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
