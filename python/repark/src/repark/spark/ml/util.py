"""ML identity and persistence interfaces."""

from __future__ import annotations

import secrets
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from typing import Self

T = TypeVar("T")


def _random_uid(prefix: str) -> str:
    """Return a Spark-shaped ``{prefix}_{8hex}`` identifier."""
    return f"{prefix}_{secrets.token_hex(4)}"


class Identifiable:
    """Object with a stable Spark-shaped ``uid``."""

    def __init__(self) -> None:
        """Assign a random uid using the concrete class name."""
        self.uid: str = _random_uid(type(self).__name__)

    def __repr__(self) -> str:
        """Return the uid."""
        return self.uid


class MLWriter(ABC):
    """Base writer for ML persistence."""

    def __init__(self, instance: Any) -> None:
        """Initialize a writer for ``instance``."""
        self.instance = instance
        self.should_overwrite = False

    def overwrite(self) -> Self:
        """Allow replacing an existing path."""
        self.should_overwrite = True
        return self

    def save(self, path: str) -> None:
        """Save the instance under ``path``."""
        self.saveImpl(path)

    @abstractmethod
    def saveImpl(self, path: str) -> None:
        """Write persistence files under ``path``."""


class MLReader(ABC):
    """Base reader for ML persistence."""

    @abstractmethod
    def load(self, path: str) -> Any:
        """Load an instance from ``path``."""


class MLWritable(ABC):
    """Mixin exposing ``write`` and ``save``."""

    @abstractmethod
    def write(self) -> MLWriter:
        """Return a writer for this instance."""

    def save(self, path: str) -> None:
        """Save this instance to ``path``."""
        self.write().save(path)


class MLReadable(ABC):
    """Mixin exposing class-level ``read`` and ``load``."""

    @classmethod
    @abstractmethod
    def read(cls) -> MLReader:
        """Return a reader for this class."""

    @classmethod
    def load(cls, path: str) -> Any:
        """Load an instance from ``path``."""
        return cls.read().load(path)


__all__ = [
    "Identifiable",
    "MLReadable",
    "MLReader",
    "MLWritable",
    "MLWriter",
    "_random_uid",
]
