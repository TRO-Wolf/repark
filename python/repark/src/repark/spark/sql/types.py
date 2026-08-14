"""``repark.spark.sql.types`` — thin re-export of :mod:`repark.spark.types`.

``from repark.spark.sql.types import StringType`` is the sed-swap of
``from pyspark.sql.types import StringType``. Every public name is the same object as
``repark.spark.types.<name>`` (``is`` identity).
"""

from __future__ import annotations

from repark.spark import types as _canonical

for _name in _canonical.__all__:
    globals()[_name] = getattr(_canonical, _name)

__all__ = list(_canonical.__all__)


def __getattr__(name: str) -> object:
    """Loud gap for names not on the canonical types surface (never a stub)."""
    raise AttributeError(
        f"repark.spark.sql.types.{name} is not implemented "
        f"(no repark.spark.types.{name}; not a stub)."
    )


def __dir__() -> list[str]:
    return sorted(__all__)
