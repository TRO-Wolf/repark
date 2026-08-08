"""R-SQLALIAS — ``repark.sql`` package so ``sed 's/pyspark/repark/'`` works verbatim.

Pins:

* every aliased name ``is`` its canonical ``repark.*`` object
* absent pyspark.sql names raise ImportError / AttributeError naming the gap (never stubs)
* sed-swap smoke: the live-parity harness import block with pyspark→repark exec's cleanly
"""

from __future__ import annotations

import importlib
import textwrap

import pytest


def test_sql_package_core_names_are_canonical_identity() -> None:
    import repark
    import repark.sql as sql
    from repark.sql import (
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

    assert SparkSession is repark.SparkSession
    assert SparkSession is repark.ReparkSession
    assert DataFrame is repark.DataFrame
    assert Row is repark.Row
    assert Column is repark.Column
    assert Window is repark.Window
    assert WindowSpec is repark.WindowSpec
    assert Catalog is repark.Catalog
    assert GroupedData is repark.dataframe.GroupedData
    assert DataFrameReader is repark.session.DataFrameReader
    assert sql.functions is importlib.import_module("repark.sql.functions")
    assert sql.types is importlib.import_module("repark.sql.types")
    assert sql.window is importlib.import_module("repark.sql.window")


def test_sql_functions_types_window_names_are_identity() -> None:
    import repark.functions as canonical_functions
    import repark.types as canonical_types
    import repark.window as canonical_window
    from repark.sql.functions import col, lit
    from repark.sql.functions import sum as spark_sum
    from repark.sql.types import IntegerType, StringType, StructType
    from repark.sql.window import Window, WindowSpec

    assert col is canonical_functions.col
    assert lit is canonical_functions.lit
    assert spark_sum is canonical_functions.sum
    assert StringType is canonical_types.StringType
    assert IntegerType is canonical_types.IntegerType
    assert StructType is canonical_types.StructType
    assert Window is canonical_window.Window
    assert WindowSpec is canonical_window.WindowSpec

    # Every canonical __all__ name is the same object on the sql alias modules.
    import repark.sql.functions as sql_functions
    import repark.sql.types as sql_types

    for name in canonical_functions.__all__:
        assert getattr(sql_functions, name) is getattr(canonical_functions, name), name
    for name in canonical_types.__all__:
        assert getattr(sql_types, name) is getattr(canonical_types, name), name


def test_absent_pyspark_sql_names_raise_loud() -> None:
    import repark.sql as sql

    with pytest.raises(ImportError):
        # from-import of a missing name
        importlib.import_module("repark.sql")
        exec("from repark.sql import UDFRegistration", {})

    with pytest.raises(AttributeError, match="not implemented"):
        _ = sql.UDFRegistration  # type: ignore[attr-defined]

    with pytest.raises(AttributeError, match="not implemented"):
        _ = sql.SQLContext  # type: ignore[attr-defined]

    with pytest.raises(AttributeError, match="not implemented"):
        from repark.sql import functions as sql_functions

        _ = sql_functions.no_such_function_xyz  # type: ignore[attr-defined]


def test_sed_swap_harness_import_block_execs() -> None:
    """Consumer-test finding: multi-import scripts use pyspark.sql / types / functions / window.

    Take the live-parity harness import shape, string-replace pyspark→repark, exec it.
    """
    # Verbatim shape from python/repark/tests/_live_parity.py (oracle harness imports).
    pyspark_block = textwrap.dedent(
        """\
        from pyspark.sql import SparkSession, Window
        from pyspark.sql import functions as sfunctions
        from pyspark.sql import types as stypes
        """
    )
    repark_block = pyspark_block.replace("pyspark", "repark")
    assert "pyspark" not in repark_block
    assert "repark.sql" in repark_block
    namespace: dict[str, object] = {}
    exec(repark_block, namespace)
    assert namespace["SparkSession"] is __import__("repark").SparkSession
    assert namespace["Window"] is __import__("repark").Window
    assert namespace["sfunctions"].col is __import__("repark.functions", fromlist=["col"]).col
    assert (
        namespace["stypes"].StringType
        is __import__("repark.types", fromlist=["StringType"]).StringType
    )


def test_sed_swap_process_silver_style_imports() -> None:
    """process_silver-style multi-line imports survive sed pyspark→repark."""
    block = textwrap.dedent(
        """\
        from pyspark.sql import SparkSession, DataFrame, Row, Column, Window
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        from pyspark.sql.window import Window as Window2
        """
    ).replace("pyspark", "repark")
    namespace: dict[str, object] = {}
    exec(block, namespace)
    import repark
    import repark.functions as functions
    import repark.types as types
    import repark.window as window

    assert namespace["SparkSession"] is repark.SparkSession
    assert namespace["DataFrame"] is repark.DataFrame
    assert namespace["Row"] is repark.Row
    assert namespace["Column"] is repark.Column
    assert namespace["Window"] is repark.Window
    assert namespace["Window2"] is window.Window
    assert namespace["F"].col is functions.col
    assert namespace["StructType"] is types.StructType
    assert namespace["StringType"] is types.StringType
