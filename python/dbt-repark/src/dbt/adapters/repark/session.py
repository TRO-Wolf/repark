"""One shared ReparkSession, and the pyodbc-shaped cursor dbt drives it through."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

if TYPE_CHECKING:
    from dbt.adapters.repark.connections import ReparkCredentials

logger = AdapterLogger("RePark")

_LOCK = threading.Lock()
_ACTIVE: dict[str, Any] = {}


def session_key(credentials: ReparkCredentials) -> tuple[Any, ...]:
    """Everything about a profile that a live session has already baked in."""
    return (
        credentials.database,
        credentials.warehouse,
        tuple(sorted(credentials.catalog_properties.items())),
        tuple(sorted(credentials.session_properties.items())),
    )


def _build_session(credentials: ReparkCredentials) -> Any:
    """Build the session from the profile, then register the memory catalog if one is named."""
    from repark import ReparkSession

    builder = ReparkSession.builder.appName("dbt-repark")
    for key, value in sorted(credentials.catalog_properties.items()):
        builder = builder.config(key, value)
    for key, value in sorted(credentials.session_properties.items()):
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    _register_memory_catalog(session, credentials)
    return session


def _register_memory_catalog(session: Any, credentials: ReparkCredentials) -> None:
    """Register the warehouse as a memory catalog unless the profile configured a real one."""
    catalog = credentials.database
    if credentials.catalog_properties:
        return
    if credentials.warehouse is None:
        raise DbtRuntimeError(
            "profile needs either `warehouse` (a memory-catalog directory) or "
            "`catalog_properties` (a spark.sql.catalog.<name>.* block)"
        )
    if any(entry.name == catalog for entry in session.catalog.list_catalogs()):
        return
    session.register_memory_catalog(catalog, credentials.warehouse)


def acquire_session(credentials: ReparkCredentials) -> Any:
    """The process-wide session for this profile, built on first use.

    RePark's ``getOrCreate`` answers one session per process, so a second profile whose
    catalog or session configuration differs would silently run against the first one's
    engine. That is refused rather than reused.
    """
    key = session_key(credentials)
    with _LOCK:
        if _ACTIVE.get("session") is None:
            _ACTIVE["session"] = _build_session(credentials)
            _ACTIVE["key"] = key
        elif _ACTIVE["key"] != key:
            raise DbtRuntimeError(
                "a RePark session is already live for a different profile; "
                "release it before connecting with another catalog or warehouse "
                f"(live {_ACTIVE['key']!r}, requested {key!r})"
            )
        return _ACTIVE["session"]


def release_session() -> None:
    """Stop the shared session and forget it. Callers that own the process use this."""
    with _LOCK:
        session = _ACTIVE.pop("session", None)
        _ACTIVE.pop("key", None)
    if session is not None:
        session.stop()


class ReparkCursor:
    """One statement at a time against the shared session, in the shape dbt expects."""

    def __init__(self, session: Any) -> None:
        """Hold the session; a result arrives only on execute."""
        self._session = session
        self._table: Any = None
        self._rows: list[tuple[Any, ...]] | None = None

    def __enter__(self) -> ReparkCursor:
        """Cursors are used as context managers by some dbt call sites."""
        return self

    def __exit__(self, *exc_info: object) -> bool:
        """Close on exit and never swallow the exception."""
        self.close()
        return False

    @property
    def description(self) -> list[tuple[str, str, None, None, None, None, bool]] | None:
        """Column metadata of the last result, or None when the statement returned no columns."""
        if self._table is None or not self._table.column_names:
            return None
        return [
            (field.name, str(field.type), None, None, None, None, field.nullable)
            for field in self._table.schema
        ]

    @property
    def row_count(self) -> int | None:
        """Rows in the last result, or None when the statement returned no columns."""
        if self._table is None or not self._table.column_names:
            return None
        return self._table.num_rows

    def close(self) -> None:
        """Drop the held result."""
        self._table = None
        self._rows = None

    def execute(self, sql: str, bindings: Any = None) -> None:
        """Run one statement through the RePark SQL door and materialise its Arrow result."""
        if bindings:
            raise DbtRuntimeError(
                "dbt-repark does not interpolate bindings; render the literal into the SQL"
            )
        statement = sql.strip()
        if statement.endswith(";"):
            statement = statement[:-1]
        self._rows = None
        self._table = self._session.sql(statement).to_arrow()

    def _materialise(self) -> list[tuple[Any, ...]]:
        """Every row of the held result as a tuple, computed once."""
        if self._rows is None:
            if self._table is None or not self._table.column_names:
                self._rows = []
            else:
                names = self._table.column_names
                self._rows = [tuple(row[name] for name in names) for row in self._table.to_pylist()]
        return self._rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Every remaining row."""
        rows = self._materialise()
        self._rows = []
        return rows

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        """The next ``size`` rows."""
        rows = self._materialise()
        head, self._rows = rows[:size], rows[size:]
        return head

    def fetchone(self) -> tuple[Any, ...] | None:
        """The next row, or None."""
        head = self.fetchmany(1)
        return head[0] if head else None


class ReparkHandle:
    """The connection handle dbt stores; it hands out one cursor per call."""

    def __init__(self, session: Any) -> None:
        """Hold the shared session."""
        self._session = session

    @property
    def session(self) -> Any:
        """The shared RePark session behind this handle."""
        return self._session

    def cursor(self) -> ReparkCursor:
        """A fresh cursor. dbt calls this once per statement, on the calling thread."""
        return ReparkCursor(self._session)

    def cancel(self) -> None:
        """RePark statements are synchronous, so there is nothing in flight to cancel."""

    def close(self) -> None:
        """The session outlives the connection; closing a handle releases nothing."""

    def rollback(self) -> None:
        """RePark has no transactions; every Iceberg commit is its own snapshot."""
