"""``repark.spark.sql.window`` — thin re-export of :mod:`repark.spark.window`.

``from repark.spark.sql.window import Window`` is the sed-swap of
``from pyspark.sql.window import Window``. Every public name is the same object as
``repark.spark.window.<name>`` (``is`` identity).
"""

from __future__ import annotations

from repark.spark.window import Window, WindowSpec

__all__ = ["Window", "WindowSpec"]


def __getattr__(name: str) -> object:
    """Loud gap for names not on the canonical window surface (never a stub)."""
    raise AttributeError(
        f"repark.spark.sql.window.{name} is not implemented "
        f"(no repark.spark.window.{name}; not a stub)."
    )


def __dir__() -> list[str]:
    return sorted(__all__)
