"""``repark.sql.types`` — thin re-export of :mod:`repark.types` (aliases only).

``from repark.sql.types import StringType`` is the sed-swap of
``from pyspark.sql.types import StringType``. Every public name is the same object as
``repark.types.<name>`` (``is`` identity).
"""

from __future__ import annotations

from repark import types as _canonical

for _name in _canonical.__all__:
    globals()[_name] = getattr(_canonical, _name)

__all__ = list(_canonical.__all__)


def __getattr__(name: str) -> object:
    """Loud gap for names not on the canonical types surface (never a stub)."""
    raise AttributeError(
        f"repark.sql.types.{name} is not implemented (no repark.types.{name}; not a stub)."
    )


def __dir__() -> list[str]:
    return sorted(__all__)
