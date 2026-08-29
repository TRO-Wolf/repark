"""Native ANSI SQL entry point and compatibility exports for the Spark facade.

The facade lives in :mod:`repark.spark`. ``repark.sql`` is a callable, not a
package, and ``SparkSession`` remains an alias of :class:`ReparkSession`.
Compute runs in Rust and crosses the boundary through Apache Arrow capsules.
"""

from __future__ import annotations

import sys
import types as _types_mod
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _distribution_version
from typing import Any

# Bind the version before facade imports because session construction reads it during package load.

try:
    __version__ = _distribution_version("repark")
except PackageNotFoundError:  # source-tree execution has no installed distribution
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
    """Run a query through the process-wide native ANSI SQL session.

    Parameters
    ----------
    query:
        SQL text as a string.

    Returns
    -------
    DataFrame
        The planned frame from the native session.

    Raises
    ------
    TypeError
        If ``query`` is not a string.
    """
    if not isinstance(query, str):
        raise TypeError(f"repark.sql() query must be str, got {type(query).__name__}")
    from repark import _native

    global _ANSI_NATIVE
    if _ANSI_NATIVE is None:
        _ANSI_NATIVE = _native.PyReparkSession.native()
    return DataFrame(_ANSI_NATIVE.sql(query), _ANSI_NATIVE, _ANSI_ALIVE)


class _ReparkModule(_types_mod.ModuleType):
    """Reject unsupported module-level display-style assignment."""

    def __setattr__(self, name: str, value: object) -> None:
        if name == "display_style":
            raise AttributeError(
                "repark.display_style is not a module attribute; set "
                "session.display_style or session.conf.set('repark.display.style', …) "
                "on a live ReparkSession (silent module assignment does not change show())"
            )
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _ReparkModule
