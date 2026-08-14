"""repark.spark.session package (r26 T1; re-homed Q1 2026-08-14)."""

from __future__ import annotations

from repark.spark.dataframe import (
    DataFrame,
    DataFrameNaFunctions,
    DataFrameStatFunctions,
    DataFrameWriter,
    DataFrameWriterV2,
    GroupedData,
)
from repark.spark.session import _funcs as _session_funcs
from repark.spark.session import builder_conf as _builder_conf
from repark.spark.session import reader as _reader
from repark.spark.session import session_core as _session_core
from repark.spark.session import sql_udf as _sql_udf
from repark.spark.session.builder_conf import RuntimeConfig, SparkContext
from repark.spark.session.create_dataframe import ReParkSession, SparkSession
from repark.spark.session.reader import DataFrameReader
from repark.spark.session.session_core import ReparkSession
from repark.spark.session.sql_udf import UDFRegistration


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

# Re-export free functions + private helpers for `from repark.spark.session import _foo`.
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
