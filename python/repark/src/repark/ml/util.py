"""ML utilities — :class:`Identifiable`, uid generation, ML read/write base.

Spark uid shape is ``{ClassName}_{8hex}`` (e.g. ``StringIndexer_3b09b1f53f96``). Uids leak into
``explainParams`` output, so format parity is load-bearing (greylight Q9).
"""

from __future__ import annotations

import secrets
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from typing import Self

T = TypeVar("T")


def _random_uid(prefix: str) -> str:
    """Return ``{prefix}_{8hex}`` matching Spark's uid layout."""
    return f"{prefix}_{secrets.token_hex(4)}"


class Identifiable:
    """Object with a stable ``uid`` (PySpark ``ml.util.Identifiable``)."""

    def __init__(self) -> None:
        """Assign a uid from the concrete class name."""
        self.uid: str = _random_uid(type(self).__name__)

    def __repr__(self) -> str:
        """Render as ``ClassName_uid`` (Spark-shaped)."""
        return self.uid


class MLWriter(ABC):
    """Base writer for ML persistence (repark-ml v1 format)."""

    def __init__(self, instance: Any) -> None:
        """Hold the instance being saved."""
        self.instance = instance
        self.should_overwrite = False

    def overwrite(self) -> Self:
        """Allow overwriting an existing path (Spark ``write().overwrite()``)."""
        self.should_overwrite = True
        return self

    def save(self, path: str) -> None:
        """Save to ``path`` (directory layout)."""
        self.saveImpl(path)

    @abstractmethod
    def saveImpl(self, path: str) -> None:
        """Subclass implement: write files under ``path``."""


class MLReader(ABC):
    """Base reader for ML persistence."""

    @abstractmethod
    def load(self, path: str) -> Any:
        """Load an instance from ``path``."""


class MLWritable(ABC):
    """Mixin for objects that support ``write().save(path)`` / ``save(path)``."""

    @abstractmethod
    def write(self) -> MLWriter:
        """Return a writer for this instance."""

    def save(self, path: str) -> None:
        """Shortcut: ``self.write().save(path)``."""
        self.write().save(path)


class MLReadable(ABC):
    """Mixin for class-level ``load(path)`` / ``read().load(path)``."""

    @classmethod
    @abstractmethod
    def read(cls) -> MLReader:
        """Return a reader for this class."""

    @classmethod
    def load(cls, path: str) -> Any:
        """Shortcut: ``cls.read().load(path)``."""
        return cls.read().load(path)


__all__ = [
    "Identifiable",
    "MLReadable",
    "MLReader",
    "MLWritable",
    "MLWriter",
    "_random_uid",
]
