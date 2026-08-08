"""r27 T0 — Q7 import freeze pins (package + core + private helper re-exports).

Mutation: drop a core re-export of GroupedData / _resolve_writer_table → RED.
"""

from __future__ import annotations

import repark.dataframe as dataframe_package
import repark.dataframe.actions_export as actions_export
import repark.dataframe.core as dataframe_core
import repark.dataframe.joins_columns as joins_columns
import repark.dataframe.writer_readwriter as writer_readwriter
from repark.dataframe import (
    DataFrame,
    DataFrameNaFunctions,
    DataFrameStatFunctions,
    DataFrameWriter,
    DataFrameWriterV2,
    GroupedData,
    _resolve_writer_table,
)
from repark.dataframe.core import (
    DataFrame as CoreDataFrame,
)
from repark.dataframe.core import (
    DataFrameNaFunctions as CoreNa,
)
from repark.dataframe.core import (
    DataFrameStatFunctions as CoreStat,
)
from repark.dataframe.core import (
    DataFrameWriter as CoreWriter,
)
from repark.dataframe.core import (
    DataFrameWriterV2 as CoreWriterV2,
)
from repark.dataframe.core import (
    GroupedData as CoreGrouped,
)
from repark.dataframe.core import (
    _pivot_max_values,
)
from repark.dataframe.core import (
    _resolve_writer_table as core_resolve_writer_table,
)


def test_public_package_and_core_export_same_dataframe_class() -> None:
    assert DataFrame is CoreDataFrame is dataframe_core.DataFrame
    assert DataFrame is dataframe_package.DataFrame


def test_nested_classes_identity_across_package_core_and_region_modules() -> None:
    """Region modules own the class body; package and core must re-export the same object."""
    assert GroupedData is CoreGrouped is joins_columns.GroupedData is dataframe_package.GroupedData
    assert (
        DataFrameWriter
        is CoreWriter
        is writer_readwriter.DataFrameWriter
        is dataframe_package.DataFrameWriter
    )
    assert (
        DataFrameWriterV2
        is CoreWriterV2
        is writer_readwriter.DataFrameWriterV2
        is dataframe_package.DataFrameWriterV2
    )
    assert (
        DataFrameNaFunctions
        is CoreNa
        is actions_export.DataFrameNaFunctions
        is dataframe_package.DataFrameNaFunctions
    )
    assert (
        DataFrameStatFunctions
        is CoreStat
        is writer_readwriter.DataFrameStatFunctions
        is dataframe_package.DataFrameStatFunctions
    )


def test_moved_private_helpers_reexported_on_core_and_package() -> None:
    """Callers (merge.py, tests) import helpers from package/core — not region paths."""
    assert _resolve_writer_table is core_resolve_writer_table
    assert _resolve_writer_table is dataframe_package._resolve_writer_table
    assert _resolve_writer_table is writer_readwriter._resolve_writer_table
    assert _pivot_max_values is dataframe_core._pivot_max_values
    assert _pivot_max_values is joins_columns._pivot_max_values
    assert callable(_resolve_writer_table)
    assert callable(_pivot_max_values)


def test_dataframe_class_module_remains_core() -> None:
    """DataFrame skeleton stays in core; nested types may live in region modules."""
    assert DataFrame.__module__ == "repark.dataframe.core"
    assert GroupedData.__module__ == "repark.dataframe.joins_columns"
    assert DataFrameWriter.__module__ == "repark.dataframe.writer_readwriter"
    assert DataFrameNaFunctions.__module__ == "repark.dataframe.actions_export"
