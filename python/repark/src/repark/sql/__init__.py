"""``repark.sql`` — alias package so ``sed 's/pyspark/repark/'`` works on multi-import scripts.

Re-exports the subset of ``pyspark.sql`` that repark actually implements. Every name here is
the **same object** as its canonical ``repark.*`` binding (``is`` identity). Names that live on
live PySpark's ``pyspark.sql`` but are **not** yet on repark raise :class:`ImportError` /
:class:`AttributeError` naming the gap — never a silent stub.

Canonical home stays ``repark`` / ``repark.functions`` / ``repark.types`` / ``repark.window``;
this package is aliases only (R-SQLALIAS).
"""

from __future__ import annotations

from repark.catalog import Catalog
from repark.column import Column
from repark.dataframe import DataFrame, GroupedData
from repark.row import Row
from repark.session import DataFrameReader, ReparkSession, SparkSession
from repark.sql import functions, types, window
from repark.window import Window, WindowSpec

# Names that are real repark surfaces under the pyspark.sql import path.
__all__ = [
    "Catalog",
    "Column",
    "DataFrame",
    "DataFrameReader",
    "GroupedData",
    "Row",
    "SparkSession",
    "Window",
    "WindowSpec",
    "functions",
    "types",
    "window",
]

# pyspark.sql also exposes these; they are NOT implemented in repark. Access via attribute or
# ``from repark.sql import …`` must fail loud with a message that names the gap.
_PYSPARK_SQL_ABSENT: frozenset[str] = frozenset(
    {
        "SQLContext",
        "HiveContext",
        "UDFRegistration",
        "UDTFRegistration",
        "Observation",
        "DataFrameNaFunctions",
        "DataFrameStatFunctions",
        "VariantVal",
        "Geography",
        "Geometry",
        "DataFrameWriter",
        "DataFrameWriterV2",
        "MergeIntoWriter",
        "PandasCogroupedOps",
        "is_remote",
        # ReparkSession is available as SparkSession alias; the Repark-native name is root-only.
    }
)


def __getattr__(name: str) -> object:
    """Loud gap for pyspark.sql names repark has not implemented (never a stub)."""
    if name in _PYSPARK_SQL_ABSENT:
        raise AttributeError(
            f"repark.sql.{name} is not implemented (pyspark.sql.{name} exists; repark has no "
            f"surface for it yet — import from repark only what the facade documents)."
        )
    raise AttributeError(f"module 'repark.sql' has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)


# SparkSession is the drop-in alias of ReparkSession (root package contract).
if SparkSession is not ReparkSession:  # pragma: no cover — import-time invariant
    raise RuntimeError("repark.sql.SparkSession must be repark.ReparkSession")
