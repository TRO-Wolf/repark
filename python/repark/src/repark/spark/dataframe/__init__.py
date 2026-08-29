"""repark.spark.dataframe package."""

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

# Star imports omit private names. Bind them for compatibility imports.
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
