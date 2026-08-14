"""repark — native ANSI ``sql()`` door plus a deprecation shim for the PySpark facade.

The facade lives at :mod:`repark.spark`. Migrating a script is still one line::

    from repark import ReparkSession   # was: from pyspark.sql import SparkSession

    spark = ReparkSession.builder.appName("etl").getOrCreate()
    spark.sql("SELECT 1 AS a, 'x' AS b").show()

``repark.sql("SELECT 1")`` is the ANSI-door *callable* (not a package).
``import repark.sql`` fails — the old pyspark-alias package moved to
``repark.spark.sql`` so ``sed 's/pyspark/repark.spark/'`` still works.

``SparkSession`` remains an alias of :class:`ReparkSession`. All compute happens
in Rust; data crosses as Apache Arrow via the Arrow PyCapsule interface.
"""

from __future__ import annotations

import sys
import types as _types_mod
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version
from typing import Any

# Session construction reads ``repark.__version__`` while this package is still
# loading — bind it before any facade import.
try:
    __version__ = _distribution_version("repark")
except PackageNotFoundError:  # running from a source tree without an installed distribution
    __version__ = "0.0.0"

from repark import errors
from repark.spark import (
    catalog,
    column,
    dataframe,
    functions,
    merge,
    ml,
    polars,
    session,
    ta,
    types,
)
from repark.spark.catalog import Catalog
from repark.spark.column import Column
from repark.spark.dataframe import DataFrame
from repark.spark.row import Row
from repark.spark.session import ReParkSession, ReparkSession, SparkSession
from repark.spark.storage import StorageLevel
from repark.spark.window import Window, WindowSpec

__all__ = [
    "Catalog",
    "Column",
    "DataFrame",
    "ReParkSession",
    "ReparkSession",
    "Row",
    "SparkSession",
    "StorageLevel",
    "Window",
    "WindowSpec",
    "catalog",
    "column",
    "dataframe",
    "errors",
    "functions",
    "merge",
    "ml",
    "polars",
    "session",
    "sql",
    "ta",
    "types",
]

_ANSI_NATIVE: Any = None
_ANSI_ALIVE: dict[str, bool] = {"alive": True}


def sql(query: str) -> DataFrame:
    """Run ``query`` through the native ANSI SQL door.

    Uses a process-wide native engine session (stock DataFusion dialect, no
    Spark extension) distinct from :meth:`ReparkSession.builder.getOrCreate`.
    Results are a :class:`DataFrame`; pin value AND Arrow type via ``to_arrow``
    / ``collect`` — never ``show`` alone.

    Parameters
    ----------
    query:
        A SQL string. Must be ``str`` (no bytes / Column).

    Returns
    -------
    DataFrame
        The planned native-session frame.
    """
    if not isinstance(query, str):
        raise TypeError(f"repark.sql() query must be str, got {type(query).__name__}")
    from repark import _native

    global _ANSI_NATIVE
    if _ANSI_NATIVE is None:
        _ANSI_NATIVE = _native.PyReparkSession.native()
    return DataFrame(_ANSI_NATIVE.sql(query), _ANSI_NATIVE, _ANSI_ALIVE)


# === r21 T3: ux-polish ===
class _ReparkModule(_types_mod.ModuleType):
    """Module type that refuses silent ``repark.display_style = …`` absorption.

    Display style is a **session** attribute (or ``repark.display.style`` conf key). Assigning
    it on the package object used to succeed as a dead module attribute while show() kept
    the spark grid — refuse loud instead.
    """

    def __setattr__(self, name: str, value: object) -> None:
        if name == "display_style":
            raise AttributeError(
                "repark.display_style is not a module attribute; set "
                "session.display_style or session.conf.set('repark.display.style', …) "
                "on a live ReparkSession (silent module assignment does not change show())"
            )
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _ReparkModule
