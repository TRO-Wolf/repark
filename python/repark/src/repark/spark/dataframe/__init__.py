"""repark.spark.dataframe package (r26 T1; re-homed Q1 2026-08-14)."""

from __future__ import annotations

import repark.spark.dataframe.core as _core
from repark.spark.dataframe.core import (
    DataFrame,
    DataFrameNaFunctions,
    DataFrameStatFunctions,
    DataFrameWriter,
    DataFrameWriterV2,
    GroupedData,
)

# Star-import skips leading-underscore names; bind them for pre-split parity
# (`from repark.spark.dataframe import _reset_…`, udtf helpers, etc.).
for _name in dir(_core):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_core, _name)
del _name, _core

__all__ = [
    "DataFrame",
    "DataFrameNaFunctions",
    "DataFrameStatFunctions",
    "DataFrameWriter",
    "DataFrameWriterV2",
    "GroupedData",
]
