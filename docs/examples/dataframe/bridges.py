"""Bridge a frame to per-batch functions and to pandas, Arrow, and polars runtimes.

pins: ex-16-dataframe-b/C-001
"""

from __future__ import annotations

from collections.abc import Iterator

import pandas as pd
import pyarrow as pa

from repark.spark import ReparkSession

COVERS: list[str] = [
    "DataFrame.mapInArrow",
    "DataFrame.map_in_arrow",
    "DataFrame.mapInPandas",
    "DataFrame.map_in_pandas",
    "DataFrame.pl",
]

OUT_SCHEMA = pa.schema([pa.field("k", pa.int64()), pa.field("v", pa.float64())])


def double_v_batches(batches: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    for batch in batches:
        table = pa.Table.from_batches([batch])
        out = pa.Table.from_pylist(
            [
                {"k": row["k"], "v": row["v"] * 2.0 if row["v"] is not None else None}
                for row in table.to_pylist()
            ],
            schema=OUT_SCHEMA,
        )
        yield from out.to_batches()


def double_v_pdfs(pdfs: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in pdfs:
        out = pdf[["k", "v"]].copy()
        out["v"] = out["v"] * 2.0
        yield out


def main() -> None:
    """Run the measured bridge answers, including the NULL v arm on both bridges."""
    repark = ReparkSession.builder.appName("ex-df-b-bridges").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("a", 1, 10.0), ("b", 2, 20.0), ("c", 3, None)],
            ["g", "k", "v"],
        )
        bridged_expected = [(1, 20.0), (2, 40.0), (3, None)]

        arrowed = frame.mapInArrow(double_v_batches, "k long, v double")
        arrowed_names = arrowed.columns
        arrowed_names_expected = ["k", "v"]
        if arrowed_names != arrowed_names_expected:
            raise SystemExit(
                f"DataFrame.mapInArrow columns {arrowed_names!r} != {arrowed_names_expected!r}"
            )
        arrowed_rows = sorted(arrowed.collect(), key=lambda row: row["k"])
        if arrowed_rows != bridged_expected:
            raise SystemExit(f"DataFrame.mapInArrow rows {arrowed_rows!r} != {bridged_expected!r}")
        arrowed_snake_rows = sorted(
            frame.map_in_arrow(double_v_batches, "k long, v double").collect(),
            key=lambda row: row["k"],
        )
        if arrowed_snake_rows != bridged_expected:
            raise SystemExit(
                f"DataFrame.map_in_arrow rows {arrowed_snake_rows!r} != {bridged_expected!r}"
            )

        pandas_out = frame.mapInPandas(double_v_pdfs, "k long, v double")
        pandas_names = pandas_out.columns
        if pandas_names != arrowed_names_expected:
            raise SystemExit(
                f"DataFrame.mapInPandas columns {pandas_names!r} != {arrowed_names_expected!r}"
            )
        pandas_rows = sorted(pandas_out.collect(), key=lambda row: row["k"])
        if pandas_rows != bridged_expected:
            raise SystemExit(f"DataFrame.mapInPandas rows {pandas_rows!r} != {bridged_expected!r}")
        pandas_snake_rows = sorted(
            frame.map_in_pandas(double_v_pdfs, "k long, v double").collect(),
            key=lambda row: row["k"],
        )
        if pandas_snake_rows != bridged_expected:
            raise SystemExit(
                f"DataFrame.map_in_pandas rows {pandas_snake_rows!r} != {bridged_expected!r}"
            )

        grouped = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 1)],
            ["g", "k"],
        )
        polars_frame = grouped.pl
        wrapper_type = type(polars_frame).__name__
        wrapper_type_expected = "PolarsFrame"
        if wrapper_type != wrapper_type_expected:
            raise SystemExit(f"DataFrame.pl type {wrapper_type!r} != {wrapper_type_expected!r}")
        collected = polars_frame.select("k").collect()
        collected_names = collected.columns
        collected_names_expected = ["k"]
        if collected_names != collected_names_expected:
            raise SystemExit(
                f"DataFrame.pl columns {collected_names!r} != {collected_names_expected!r}"
            )
        polars_values = collected["k"].to_list()
        polars_values_expected = [1, 2, 1]
        if polars_values != polars_values_expected:
            raise SystemExit(f"DataFrame.pl values {polars_values!r} != {polars_values_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
