"""repark.spark.session package (r26 T1; re-homed Q1 2026-08-14)."""

from __future__ import annotations

import sys as _sys

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
from repark.spark.session import catalog_resolution as _catalog_resolution
from repark.spark.session import create_dataframe_arrow as _create_dataframe_arrow
from repark.spark.session import create_dataframe_inference as _create_dataframe_inference
from repark.spark.session import create_dataframe_rows as _create_dataframe_rows
from repark.spark.session import create_dataframe_schema as _create_dataframe_schema
from repark.spark.session import create_dataframe_tuples as _create_dataframe_tuples
from repark.spark.session import create_dataframe_values as _create_dataframe_values
from repark.spark.session import reader as _reader
from repark.spark.session import reader_support as _reader_support
from repark.spark.session import session_configuration as _session_configuration
from repark.spark.session import session_core as _session_core
from repark.spark.session import session_state as _session_state
from repark.spark.session import sql_relations as _sql_relations
from repark.spark.session import sql_udf as _sql_udf
from repark.spark.session import sql_udf_discovery as _sql_udf_discovery
from repark.spark.session import sql_udf_materialization as _sql_udf_materialization
from repark.spark.session import sql_udf_parsing as _sql_udf_parsing
from repark.spark.session import sql_udf_residual as _sql_udf_residual
from repark.spark.session import sql_udf_rewrite as _sql_udf_rewrite
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


for _mod in (
    _session_funcs,
    _builder_conf,
    _catalog_resolution,
    _create_dataframe_arrow,
    _create_dataframe_inference,
    _create_dataframe_rows,
    _create_dataframe_schema,
    _create_dataframe_tuples,
    _create_dataframe_values,
    _reader,
    _reader_support,
    _session_configuration,
    _session_core,
    _session_state,
    _sql_relations,
    _sql_udf,
    _sql_udf_discovery,
    _sql_udf_materialization,
    _sql_udf_parsing,
    _sql_udf_residual,
    _sql_udf_rewrite,
):
    _wire(_mod)

# Re-export free functions + private helpers for `from repark.spark.session import _foo`.
for _name in dir(_session_funcs):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_session_funcs, _name)

_session_state._install_state_proxy(_sys.modules[__name__])

__all__ = [
    "DataFrameReader",
    "ReParkSession",
    "ReparkSession",
    "RuntimeConfig",
    "SparkContext",
    "SparkSession",
    "UDFRegistration",
]
