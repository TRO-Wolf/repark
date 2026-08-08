"""repark — a near-drop-in PySpark API on a pure-Rust, no-JVM Apache Iceberg engine.

Migrating an existing script is a one-line change::

    from repark import ReparkSession   # was: from pyspark.sql import SparkSession

    spark = ReparkSession.builder.appName("etl").getOrCreate()
    spark.sql("SELECT 1 AS a, 'x' AS b").show()

``SparkSession`` remains available as an alias of :class:`ReparkSession` for byte-identical drop-in
(``from repark import SparkSession``); new code may also use ``import repark as rp`` then
``rp.ReparkSession``. All compute happens in Rust; data crosses the boundary as Apache Arrow
(zero-copy, no serialization) via the Arrow PyCapsule interface.
"""

from __future__ import annotations

import sys
import types as _types_mod
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version

from repark import errors, functions, ml, polars, ta, types
from repark.catalog import Catalog
from repark.column import Column
from repark.dataframe import DataFrame
from repark.row import Row
from repark.session import ReParkSession, ReparkSession, SparkSession
from repark.storage import StorageLevel
from repark.window import Window, WindowSpec

try:
    __version__ = _distribution_version("repark")
except PackageNotFoundError:  # running from a source tree without an installed distribution
    __version__ = "0.0.0"

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
    "errors",
    "functions",
    "ml",
    "polars",
    "ta",
    "types",
]


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
