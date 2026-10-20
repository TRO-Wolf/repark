from __future__ import annotations

import json
import time
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa

import repark
from repark import ReparkSession

ROWS = 1_000_000
GROUPS = 1000
SMALL_ROWS = 200_000
IDS = np.arange(ROWS, dtype=np.int64)
KEYS = pa.array(IDS % GROUPS)
MICROS = pa.array(1767323045000000 + IDS, pa.timestamp("us"))
NANOS = pa.array(1767323045000000000 + IDS * 1001, pa.timestamp("ns"))
MICRO_LISTS = pa.ListArray.from_arrays(pa.array(np.arange(ROWS + 1, dtype=np.int32)), MICROS)
LIST_SCHEMA = "id long, v array<timestamp_ntz>"
WIDE_NAMES = [f"t{index}" for index in range(4)]
WIDE_SCHEMA = (
    "id long, "
    + ", ".join(f"{name} timestamp_ntz" for name in WIDE_NAMES)
    + ", x0 double, x1 double, x2 double, x3 double"
)
WIDE = pa.table(
    {
        "id": KEYS,
        **dict.fromkeys(WIDE_NAMES, MICROS),
        **{f"x{index}": pa.array(IDS.astype("float64")) for index in range(4)},
    }
)


def identity(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return the group frame unchanged."""
    return pdf


def python_lists(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return every array cell as a Python list of datetimes."""
    return pdf.assign(v=[cell.tolist() for cell in pdf["v"]])


def empty_lists(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return an empty Python list in every cell."""
    return pdf.assign(v=[[] for _ in range(len(pdf))])


def floor_divide(pdf: pd.DataFrame) -> pd.DataFrame:
    """Floor-divide every struct tick by 1000."""
    return pdf.assign(v=[{"t": cell["t"] // 1000} for cell in pdf["v"]])


def keep_key(pdf: pd.DataFrame) -> pd.DataFrame:
    """Return only the key column."""
    return pdf[["id"]]


def floor_column(pdf: pd.DataFrame) -> pd.DataFrame:
    """Floor a nanosecond column to microseconds with pandas."""
    return pdf.assign(v=pdf["v"].dt.floor("us"))


SHAPES: dict[str, tuple[pa.Table, Any, str, int]] = {
    "no_list_column": (
        pa.table({"id": KEYS, "x": pa.array(IDS.astype("float64"))}),
        identity,
        "id long, x double",
        ROWS,
    ),
    "numpy_microsecond_cells": (
        pa.table({"id": KEYS, "v": MICRO_LISTS}),
        identity,
        LIST_SCHEMA,
        ROWS,
    ),
    "python_datetime_lists": (
        pa.table({"id": KEYS, "v": MICRO_LISTS}),
        python_lists,
        LIST_SCHEMA,
        ROWS,
    ),
    "empty_lists": (pa.table({"id": KEYS, "v": MICRO_LISTS}), empty_lists, LIST_SCHEMA, ROWS),
    "struct_of_microsecond_lists": (
        pa.table({"id": KEYS, "v": pa.StructArray.from_arrays([MICRO_LISTS], names=["a"])}),
        identity,
        "id long, v struct<a:array<timestamp_ntz>>",
        ROWS,
    ),
    "nanosecond_struct_floor_divided": (
        pa.table({"id": KEYS, "v": pa.StructArray.from_arrays([NANOS], names=["t"])}),
        floor_divide,
        "id long, v struct<t:timestamp_ntz>",
        ROWS,
    ),
    "nanosecond_struct_dropped": (
        pa.table({"id": KEYS, "v": pa.StructArray.from_arrays([NANOS], names=["t"])}),
        keep_key,
        "id long",
        ROWS,
    ),
    "nanosecond_column_floored": (
        pa.table({"id": KEYS, "v": NANOS}),
        floor_column,
        "id long, v timestamp_ntz",
        ROWS,
    ),
    "top_level_timestamps": (WIDE, identity, WIDE_SCHEMA, ROWS),
    "groups_of_four_rows": (
        pa.table({"id": pa.array(IDS[:SMALL_ROWS] // 4), "t": MICROS.slice(0, SMALL_ROWS)}),
        identity,
        "id long, t timestamp_ntz",
        SMALL_ROWS,
    ),
}


def time_shape(spark: Any, name: str, view: str) -> float:
    """Return the seconds one grouped applyInPandas over a shape takes to its Arrow result."""
    table, function, schema, rows = SHAPES[name]
    spark._ensure_alive().register_arrow_stream_as_temp_view(view, table)
    start = time.perf_counter()
    result = spark.table(view).groupBy("id").applyInPandas(function, schema).toArrow()
    elapsed = time.perf_counter() - start
    if result.num_rows != rows:
        raise SystemExit(f"{name}: {result.num_rows} rows, expected {rows}")
    return elapsed


def main() -> None:
    """Print one JSON object: the seconds of every shape on the installed build."""
    spark = ReparkSession.builder.appName("ns-array-apply-in-pandas-refuse-1").getOrCreate()
    time_shape(spark, "empty_lists", "warm")
    seconds = {name: round(time_shape(spark, name, name), 4) for name in SHAPES}
    print(json.dumps({"package": repark.__file__, "seconds": seconds}))
    spark.stop()


if __name__ == "__main__":
    main()
