"""repark.spark — the near-drop-in PySpark facade package.

The one-line import is ``from repark.spark import ReparkSession``. Multi-import
scripts sed-swap ``pyspark`` → ``repark.spark`` so ``from repark.spark.sql import
SparkSession`` / ``functions`` / ``types`` keep working (same-object identity).

Exports are loaded lazily (PEP 562) so sibling submodules can import each other
without cycling through this package's eager bindings.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

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
    "ta",
    "types",
]

# name → (module, attribute or None = the module object itself)
_EXPORTS: dict[str, tuple[str, str | None]] = {
    "Catalog": ("repark.spark.catalog", "Catalog"),
    "Column": ("repark.spark.column", "Column"),
    "DataFrame": ("repark.spark.dataframe", "DataFrame"),
    "ReParkSession": ("repark.spark.session", "ReParkSession"),
    "ReparkSession": ("repark.spark.session", "ReparkSession"),
    "Row": ("repark.spark.row", "Row"),
    "SparkSession": ("repark.spark.session", "SparkSession"),
    "StorageLevel": ("repark.spark.storage", "StorageLevel"),
    "Window": ("repark.spark.window", "Window"),
    "WindowSpec": ("repark.spark.window", "WindowSpec"),
    "catalog": ("repark.spark.catalog", None),
    "column": ("repark.spark.column", None),
    "dataframe": ("repark.spark.dataframe", None),
    "errors": ("repark.errors", None),
    "functions": ("repark.spark.functions", None),
    "merge": ("repark.spark.merge", None),
    "ml": ("repark.spark.ml", None),
    "polars": ("repark.spark.polars", None),
    "session": ("repark.spark.session", None),
    "ta": ("repark.spark.ta", None),
    "types": ("repark.spark.types", None),
}


def __getattr__(name: str) -> Any:
    """Resolve a public facade name on first access; cache on the module."""
    spec = _EXPORTS.get(name)
    if spec is None:
        raise AttributeError(f"module 'repark.spark' has no attribute {name!r}")
    module_name, attribute = spec
    module = import_module(module_name)
    value: Any = module if attribute is None else getattr(module, attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(__all__) | {key for key in globals() if not key.startswith("_")})
