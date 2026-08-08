"""``repark.sql.functions`` — thin re-export of :mod:`repark.functions` (aliases only).

``from repark.sql.functions import col`` is the sed-swap of
``from pyspark.sql.functions import col``. Every public name is the same object as
``repark.functions.<name>`` (``is`` identity).
"""

from __future__ import annotations

from repark import functions as _canonical

# Populate this module's globals from the canonical __all__ so star-imports and
# ``from repark.sql.functions import col`` resolve to the same objects.
for _name in _canonical.__all__:
    globals()[_name] = getattr(_canonical, _name)

__all__ = list(_canonical.__all__)


def __getattr__(name: str) -> object:
    """Loud gap for names not on the canonical functions surface (never a stub)."""
    raise AttributeError(
        f"repark.sql.functions.{name} is not implemented (no repark.functions.{name}; not a stub)."
    )


def __dir__() -> list[str]:
    return sorted(__all__)
