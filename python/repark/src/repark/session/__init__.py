"""repark.session package (r26 T1; public import paths frozen)."""

from __future__ import annotations

from repark.dataframe import (
    DataFrame,
    DataFrameNaFunctions,
    DataFrameStatFunctions,
    DataFrameWriter,
    DataFrameWriterV2,
    GroupedData,
)
from repark.session import _funcs as _session_funcs
from repark.session import builder_conf as _builder_conf
from repark.session import reader as _reader
from repark.session import session_core as _session_core
from repark.session import sql_udf as _sql_udf
from repark.session.builder_conf import RuntimeConfig, SparkContext
from repark.session.create_dataframe import ReParkSession, SparkSession
from repark.session.reader import DataFrameReader
from repark.session.session_core import ReparkSession
from repark.session.sql_udf import UDFRegistration


def _wire(module: object) -> None:
    """Install peer class names used by free functions / methods (pre-split globals)."""
    module.RuntimeConfig = RuntimeConfig
    module.SparkContext = SparkContext
    module.ReparkSession = ReparkSession
    module.DataFrameReader = DataFrameReader
    module.UDFRegistration = UDFRegistration
    module.SparkSession = SparkSession
    module.ReParkSession = ReParkSession
    module.DataFrame = DataFrame
    module.DataFrameWriter = DataFrameWriter
    module.DataFrameWriterV2 = DataFrameWriterV2
    module.DataFrameNaFunctions = DataFrameNaFunctions
    module.DataFrameStatFunctions = DataFrameStatFunctions
    module.GroupedData = GroupedData


for _mod in (_session_funcs, _builder_conf, _session_core, _reader, _sql_udf):
    _wire(_mod)

# Re-export free functions + private helpers for `from repark.session import _foo`.
for _name in dir(_session_funcs):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_session_funcs, _name)

__all__ = [
    "DataFrameReader",
    "ReParkSession",
    "ReparkSession",
    "RuntimeConfig",
    "SparkContext",
    "SparkSession",
    "UDFRegistration",
]
