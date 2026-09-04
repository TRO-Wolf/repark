"""Profile fields and the connection manager over the shared RePark session."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, ClassVar

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
    Credentials,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.repark.session import ReparkHandle, acquire_session
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("RePark")


@dataclass
class ReparkCredentials(Credentials):
    """A RePark profile: which catalog, where its warehouse is, and how to build it.

    ``catalog`` is the profile spelling of dbt's ``database``. ``warehouse`` names a
    directory for a memory catalog. ``catalog_properties`` carries a
    ``spark.sql.catalog.<name>.*`` block verbatim for Glue or S3 Tables, in the shape
    ``python/repark/tests/_acceptance.py`` builds. The two are alternatives.
    """

    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    catalog_properties: dict[str, str] = field(default_factory=dict)
    session_properties: dict[str, str] = field(default_factory=dict)

    _ALIASES: ClassVar[dict[str, str]] = {"catalog": "database"}

    def __post_init__(self) -> None:
        """Refuse a profile that names no catalog or no namespace."""
        if not self.database:
            raise DbtRuntimeError("dbt-repark needs `catalog` in the profile")
        if not self.schema:
            raise DbtRuntimeError("dbt-repark needs `schema` (the RePark namespace) in the profile")

    @property
    def type(self) -> str:
        """The adapter name dbt matches the profile against."""
        return "repark"

    @property
    def unique_field(self) -> str:
        """The catalog is what identifies one deployment of this adapter."""
        return str(self.database)

    def _connection_keys(self) -> tuple[str, ...]:
        """Fields dbt prints when it reports the connection."""
        return ("database", "schema", "warehouse", "catalog_properties")


class ReparkConnectionManager(SQLConnectionManager):
    """dbt threads share one RePark session; each statement gets its own cursor."""

    TYPE = "repark"

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        """Re-raise RePark's refusal as the dbt error type, keeping its message."""
        try:
            yield
        except DbtRuntimeError:
            raise
        except Exception as error:
            logger.debug(f"RePark refused:\n{sql}")
            raise DbtRuntimeError(str(error)) from error

    def cancel(self, connection: Connection) -> None:
        """RePark statements are synchronous, so there is nothing in flight to cancel."""

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        """RePark reports no affected-row count for DDL; a result set reports its rows."""
        rows = getattr(cursor, "row_count", None)
        if rows is None:
            return AdapterResponse(_message="OK")
        return AdapterResponse(_message=f"OK {rows}", rows_affected=rows)

    def add_begin_query(self, *args: Any, **kwargs: Any) -> None:
        """RePark has no transactions; every Iceberg commit is its own snapshot."""

    def add_commit_query(self, *args: Any, **kwargs: Any) -> None:
        """RePark has no transactions; every Iceberg commit is its own snapshot."""

    def commit(self, *args: Any, **kwargs: Any) -> None:
        """RePark has no transactions; every Iceberg commit is its own snapshot."""

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        """RePark has no transactions; roll back with CALL rollback_to_snapshot instead."""

    def repark_session(self) -> Any:
        """The shared session, for adapter methods that use the facade rather than SQL."""
        connection = self.get_thread_connection()
        if connection.handle is None or connection.state != ConnectionState.OPEN:
            connection = self.open(connection)
        return connection.handle.session

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        """Attach the thread's connection to the shared session."""
        if connection.state == ConnectionState.OPEN:
            return connection
        try:
            session = acquire_session(connection.credentials)
        except DbtRuntimeError:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise
        connection.handle = ReparkHandle(session)
        connection.state = ConnectionState.OPEN
        return connection
