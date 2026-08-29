"""``repark.spark.sql`` — alias package so ``sed 's/pyspark/repark.spark/'`` works.

Re-exports the subset of ``pyspark.sql`` that repark actually implements. Every name here is
the **same object** as its canonical ``repark.spark.*`` binding (``is`` identity). Names that
live on live PySpark's ``pyspark.sql`` but are **not** yet on repark raise :class:`ImportError`
/ :class:`AttributeError` naming the gap — never a silent stub. Canonical home stays
``repark.spark`` / ``repark.spark.functions`` / ``repark.spark.types`` /
``repark.spark.window``; this package is aliases only.
"""

from __future__ import annotations

from repark.spark.catalog import Catalog
from repark.spark.column import Column
from repark.spark.dataframe import DataFrame, GroupedData
from repark.spark.row import Row
from repark.spark.session import DataFrameReader, ReparkSession, SparkSession
from repark.spark.sql import functions, types, window
from repark.spark.window import Window, WindowSpec

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
# ``from repark.spark.sql import …`` must fail loud with a message that names the gap.
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
            f"repark.spark.sql.{name} is not implemented (pyspark.sql.{name} exists; repark has no "
            f"surface for it yet — import from repark.spark only what the facade documents)."
        )
    raise AttributeError(f"module 'repark.spark.sql' has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)


# SparkSession is the drop-in alias of ReparkSession (root package contract).
if SparkSession is not ReparkSession:  # pragma: no cover — import-time invariant
    raise RuntimeError("repark.spark.sql.SparkSession must be repark.spark.ReparkSession")
