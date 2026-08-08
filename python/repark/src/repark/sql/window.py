"""``repark.sql.window`` — thin re-export of :mod:`repark.window` (aliases only).

``from repark.sql.window import Window`` is the sed-swap of
``from pyspark.sql.window import Window``. Every public name is the same object as
``repark.window.<name>`` (``is`` identity).
"""

from __future__ import annotations

from repark.window import Window, WindowSpec

__all__ = ["Window", "WindowSpec"]


def __getattr__(name: str) -> object:
    """Loud gap for names not on the canonical window surface (never a stub)."""
    raise AttributeError(
        f"repark.sql.window.{name} is not implemented (no repark.window.{name}; not a stub)."
    )


def __dir__() -> list[str]:
    return sorted(__all__)
