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
            [{"k": row["k"], "v": row["v"] * 2.0} for row in table.to_pylist()],
            schema=OUT_SCHEMA,
        )
        yield from out.to_batches()


def double_v_pdfs(pdfs: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in pdfs:
        out = pdf[["k", "v"]].copy()
        out["v"] = out["v"] * 2.0
        yield out


def main() -> None:
    """Run the measured bridge answers on one two-row frame."""
    repark = ReparkSession.builder.appName("ex-df-b-bridges").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [("a", 1, 10.0), ("b", 2, 20.0)],
            ["g", "k", "v"],
        )
        arrowed = frame.mapInArrow(double_v_batches, "k long, v double")
        arrowed_names = arrowed.columns
        if arrowed_names != ["k", "v"]:
            raise SystemExit(f"DataFrame.mapInArrow columns {arrowed_names!r} != ['k', 'v']")
        arrowed_rows = set(arrowed.collect())
        arrowed_expected = {(1, 20.0), (2, 40.0)}
        if arrowed_rows != arrowed_expected:
            raise SystemExit(f"DataFrame.mapInArrow rows {arrowed_rows!r} != {arrowed_expected!r}")
        arrowed_snake_rows = set(frame.map_in_arrow(double_v_batches, "k long, v double").collect())
        if arrowed_snake_rows != arrowed_expected:
            raise SystemExit(
                f"DataFrame.map_in_arrow rows {arrowed_snake_rows!r} != {arrowed_expected!r}"
            )

        pandas_out = frame.mapInPandas(double_v_pdfs, "k long, v double")
        pandas_names = pandas_out.columns
        if pandas_names != ["k", "v"]:
            raise SystemExit(f"DataFrame.mapInPandas columns {pandas_names!r} != ['k', 'v']")
        pandas_rows = set(pandas_out.collect())
        if pandas_rows != arrowed_expected:
            raise SystemExit(f"DataFrame.mapInPandas rows {pandas_rows!r} != {arrowed_expected!r}")
        pandas_snake_rows = set(frame.map_in_pandas(double_v_pdfs, "k long, v double").collect())
        if pandas_snake_rows != arrowed_expected:
            raise SystemExit(
                f"DataFrame.map_in_pandas rows {pandas_snake_rows!r} != {arrowed_expected!r}"
            )

        grouped = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 1)],
            ["g", "k"],
        )
        polars_frame = grouped.pl
        wrapper_type = type(polars_frame).__name__
        if wrapper_type != "PolarsFrame":
            raise SystemExit(f"DataFrame.pl type {wrapper_type!r} != 'PolarsFrame'")
        collected = polars_frame.select("k").collect()
        collected_names = collected.columns
        if collected_names != ["k"]:
            raise SystemExit(f"DataFrame.pl columns {collected_names!r} != ['k']")
        polars_values = collected["k"].to_list()
        if polars_values != [1, 2, 1]:
            raise SystemExit(f"DataFrame.pl values {polars_values!r} != [1, 2, 1]")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
