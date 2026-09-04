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
        assert arrowed.columns == ["k", "v"]
        assert set(arrowed.collect()) == {(1, 20.0), (2, 40.0)}
        arrowed_snake = frame.map_in_arrow(double_v_batches, "k long, v double")
        assert set(arrowed_snake.collect()) == {(1, 20.0), (2, 40.0)}

        pandas_out = frame.mapInPandas(double_v_pdfs, "k long, v double")
        assert pandas_out.columns == ["k", "v"]
        assert set(pandas_out.collect()) == {(1, 20.0), (2, 40.0)}
        pandas_snake = frame.map_in_pandas(double_v_pdfs, "k long, v double")
        assert set(pandas_snake.collect()) == {(1, 20.0), (2, 40.0)}

        grouped = repark.createDataFrame(
            [("a", 1), ("a", 2), ("b", 1)],
            ["g", "k"],
        )
        polars_frame = grouped.pl
        assert type(polars_frame).__name__ == "PolarsFrame"
        collected = polars_frame.select("k").collect()
        assert collected.columns == ["k"]
        assert collected["k"].to_list() == [1, 2, 1]
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
