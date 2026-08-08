"""repark.dataframe package (r26 T1; public import paths frozen)."""

from __future__ import annotations

import repark.dataframe.core as _core
from repark.dataframe.core import (
    DataFrame,
    DataFrameNaFunctions,
    DataFrameStatFunctions,
    DataFrameWriter,
    DataFrameWriterV2,
    GroupedData,
)

# Star-import skips leading-underscore names; bind them for pre-split parity
# (`from repark.dataframe import _reset_…`, udtf helpers, etc.).
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
