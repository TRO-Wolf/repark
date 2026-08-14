"""Q1 re-home pins — ``import repark.sql`` fails; ``repark.sql()`` is the ANSI door.

The pyspark-alias package now lives at ``repark.spark.sql`` so
``sed 's/pyspark/repark.spark/'`` still works. Identity of alias names vs
canonical ``repark.spark.*`` is pinned here; the old ``repark.sql`` *module*
must not exist.
"""

from __future__ import annotations

import importlib
import textwrap

import pytest


def test_import_repark_sql_module_fails() -> None:
    """Acceptance: ``import repark.sql`` exits non-zero / raises ModuleNotFoundError."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("repark.sql")


def test_repark_sql_callable_select_one_arrow_path() -> None:
    """``repark.sql("SELECT 1")`` runs through the ANSI door (value AND Arrow type)."""
    import pyarrow as pa

    import repark

    assert callable(repark.sql)
    frame = repark.sql("SELECT 1 AS one")
    table = frame.to_arrow()
    assert table.num_rows == 1
    assert table.num_columns == 1
    assert table.column_names == ["one"]
    # Stock DataFusion / ANSI integer literal is Int64 (not Spark-door collapse).
    assert pa.types.is_integer(table.schema.field("one").type)
    assert table.column("one").to_pylist() == [1]


def test_repark_sql_callable_integer_division_is_ansi_not_spark() -> None:
    """Honesty pin: INT/INT truncates on the ANSI door; Spark would yield 2.5 float."""
    import pyarrow as pa

    import repark

    table = repark.sql("SELECT CAST(5 AS INT) / CAST(2 AS INT) AS q").to_arrow()
    field_type = table.schema.field("q").type
    assert pa.types.is_integer(field_type), f"ANSI INT/INT must stay integer, got {field_type}"
    assert table.column("q").to_pylist() == [2]


def test_spark_sql_package_core_names_are_canonical_identity() -> None:
    import repark.spark as spark
    import repark.spark.sql as sql
    from repark.spark.sql import (
        Catalog,
        Column,
        DataFrame,
        DataFrameReader,
        GroupedData,
        Row,
        SparkSession,
        Window,
        WindowSpec,
    )

    assert SparkSession is spark.SparkSession
    assert SparkSession is spark.ReparkSession
    assert DataFrame is spark.DataFrame
    assert Row is spark.Row
    assert Column is spark.Column
    assert Window is spark.Window
    assert WindowSpec is spark.WindowSpec
    assert Catalog is spark.Catalog
    assert GroupedData is spark.dataframe.GroupedData
    assert DataFrameReader is spark.session.DataFrameReader
    assert sql.functions is importlib.import_module("repark.spark.sql.functions")
    assert sql.types is importlib.import_module("repark.spark.sql.types")
    assert sql.window is importlib.import_module("repark.spark.sql.window")


def test_spark_sql_functions_types_window_names_are_identity() -> None:
    import repark.spark.functions as canonical_functions
    import repark.spark.types as canonical_types
    import repark.spark.window as canonical_window
    from repark.spark.sql.functions import col, lit
    from repark.spark.sql.functions import sum as spark_sum
    from repark.spark.sql.types import IntegerType, StringType, StructType
    from repark.spark.sql.window import Window, WindowSpec

    assert col is canonical_functions.col
    assert lit is canonical_functions.lit
    assert spark_sum is canonical_functions.sum
    assert StringType is canonical_types.StringType
    assert IntegerType is canonical_types.IntegerType
    assert StructType is canonical_types.StructType
    assert Window is canonical_window.Window
    assert WindowSpec is canonical_window.WindowSpec

    import repark.spark.sql.functions as sql_functions
    import repark.spark.sql.types as sql_types

    for name in canonical_functions.__all__:
        assert getattr(sql_functions, name) is getattr(canonical_functions, name), name
    for name in canonical_types.__all__:
        assert getattr(sql_types, name) is getattr(canonical_types, name), name


def test_absent_pyspark_sql_names_raise_loud() -> None:
    import repark.spark.sql as sql

    with pytest.raises(ImportError):
        exec("from repark.spark.sql import UDFRegistration", {})

    with pytest.raises(AttributeError, match="not implemented"):
        _ = sql.UDFRegistration  # type: ignore[attr-defined]

    with pytest.raises(AttributeError, match="not implemented"):
        _ = sql.SQLContext  # type: ignore[attr-defined]

    with pytest.raises(AttributeError, match="not implemented"):
        from repark.spark.sql import functions as sql_functions

        _ = sql_functions.no_such_function_xyz  # type: ignore[attr-defined]


def test_sed_swap_pyspark_to_repark_spark_execs() -> None:
    """Mechanical ``pyspark`` → ``repark.spark`` still works (design Q1; nested ``.sql``)."""
    pyspark_block = textwrap.dedent(
        """\
        from pyspark.sql import SparkSession, Window
        from pyspark.sql import functions as sfunctions
        from pyspark.sql import types as stypes
        """
    )
    repark_block = pyspark_block.replace("pyspark", "repark.spark")
    assert "pyspark" not in repark_block
    assert "repark.spark.sql" in repark_block
    namespace: dict[str, object] = {}
    exec(repark_block, namespace)
    spark = __import__("repark.spark", fromlist=["SparkSession"])
    assert namespace["SparkSession"] is spark.SparkSession
    assert namespace["Window"] is spark.Window
    assert namespace["sfunctions"].col is __import__("repark.spark.functions", fromlist=["col"]).col
    assert (
        namespace["stypes"].StringType
        is __import__("repark.spark.types", fromlist=["StringType"]).StringType
    )


def test_sed_swap_process_silver_style_imports() -> None:
    """Publish-job-style multi-line imports survive sed pyspark→repark.spark."""
    block = textwrap.dedent(
        """\
        from pyspark.sql import SparkSession, DataFrame, Row, Column, Window
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        from pyspark.sql.window import Window as Window2
        """
    ).replace("pyspark", "repark.spark")
    namespace: dict[str, object] = {}
    exec(block, namespace)
    import repark.spark as spark
    import repark.spark.functions as functions
    import repark.spark.types as types
    import repark.spark.window as window

    assert namespace["SparkSession"] is spark.SparkSession
    assert namespace["DataFrame"] is spark.DataFrame
    assert namespace["Row"] is spark.Row
    assert namespace["Column"] is spark.Column
    assert namespace["Window"] is spark.Window
    assert namespace["Window2"] is window.Window
    assert namespace["F"].col is functions.col
    assert namespace["StructType"] is types.StructType
    assert namespace["StringType"] is types.StringType


def test_top_level_shim_repark_session_identity() -> None:
    """A5: ``from repark import ReparkSession`` keeps resolving (re-export, not a module shim)."""
    import repark
    from repark import ReparkSession, SparkSession

    assert ReparkSession is repark.ReparkSession
    assert SparkSession is ReparkSession
    assert ReparkSession is repark.spark.ReparkSession
