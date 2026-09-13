"""Named database sources declared by ``repark.toml`` (the CFG-2 facade door)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark.spark.catalog import SourceMetadata

if TYPE_CHECKING:
    from repark.spark.session.session_core import ReparkSession


class NamedSource:
    """Read-only handle for one declared database source (RePark extension)."""

    __slots__ = ("_key_path", "_kind", "_name", "_session")

    def __init__(self, session: ReparkSession, name: str, kind: str, key_path: str) -> None:
        """Bind the handle to its session and declared identity."""
        self._session = session
        self._name = name
        self._kind = kind
        self._key_path = key_path

    @property
    def name(self) -> str:
        """The declared source name."""
        return self._name

    @property
    def kind(self) -> str:
        """The declared connector kind spelling."""
        return self._kind

    @property
    def key_path(self) -> str:
        """The ``<profile>.database.<kind>.<name>`` key path."""
        return self._key_path

    def ping(self) -> None:
        """Refuse with the connector-pending message until the connector lands."""
        inner = self._session._ensure_alive()
        from repark import _native

        _native.session_source_ping(inner, self._name)


def sources(session: ReparkSession) -> list[SourceMetadata]:
    """List declared database sources as ``SourceMetadata`` rows in declaration order."""
    inner = session._ensure_alive()
    from repark import _native

    return [
        SourceMetadata(
            name=name,
            kind=kind,
            key_path=key_path,
            auto_register=auto_register,
            properties=dict(properties),
        )
        for name, kind, key_path, auto_register, properties in _native.session_sources(inner)
    ]


def source(session: ReparkSession, name: str) -> NamedSource:
    """Return the :class:`NamedSource` handle for one declared source name."""
    inner = session._ensure_alive()
    from repark import _native

    source_name, kind, key_path = _native.session_source(inner, name)
    return NamedSource(session, source_name, kind, key_path)
