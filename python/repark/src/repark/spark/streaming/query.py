from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

from repark.errors import PySparkValueError

if TYPE_CHECKING:
    from repark._native import StreamingQueryException
    from repark.spark.session.session_core import ReparkSession


def _stub_terminal() -> NoReturn:
    raise NotImplementedError("MB-4 stub: the streaming driver is not wired in this round")


class StreamingQuery:
    """A handle to a query that is executing continuously in the background as new data arrives.

    All these methods are thread-safe.
    """

    @property
    def id(self) -> str:
        """Returns the unique id of this query."""
        _stub_terminal()

    @property
    def runId(self) -> str:  # noqa: N802 — PySpark property name
        """Returns the unique id of this run of the query."""
        _stub_terminal()

    @property
    def name(self) -> str | None:
        """Returns the name of the query, or None if not specified."""
        _stub_terminal()

    @property
    def isActive(self) -> bool:  # noqa: N802 — PySpark property name
        """Whether this query is currently active."""
        _stub_terminal()

    @property
    def status(self) -> dict[str, object]:
        """Returns the current status of the query."""
        _stub_terminal()

    @property
    def recentProgress(self) -> list[dict[str, object]]:  # noqa: N802 — PySpark property name
        """Returns an array of the most recent progress updates for this query."""
        _stub_terminal()

    @property
    def lastProgress(self) -> dict[str, object] | None:  # noqa: N802 — PySpark property name
        """Returns the most recent progress update of this streaming query."""
        _stub_terminal()

    def awaitTermination(self, timeout: int | None = None) -> bool | None:  # noqa: N802
        """Waits for the termination of `this` query, either by query.stop() or by an exception.

        Parameters
        ----------
        timeout : int, optional
            default ``None``. The waiting time for specified streaming query to terminate.
        """
        if timeout is not None and (not isinstance(timeout, (int, float)) or timeout <= 0):
            raise PySparkValueError(
                errorClass="VALUE_NOT_POSITIVE",
                messageParameters={
                    "arg_name": "timeout",
                    "arg_value": type(timeout).__name__,
                },
            )
        _stub_terminal()

    def stop(self) -> None:
        """Stop this streaming query."""
        _stub_terminal()

    def exception(self) -> StreamingQueryException | None:
        """Returns the StreamingQueryException if the query was terminated by an exception."""
        _stub_terminal()


class StreamingQueryManager:
    """A class to manage all the StreamingQuery StreamingQueries active."""

    def __init__(self, session: ReparkSession) -> None:
        """Bind to the owning session; each ``spark.streams`` access returns a fresh manager."""
        self._session = session

    @property
    def active(self) -> list[StreamingQuery]:
        """Returns a list of active queries associated with this SQLContext."""
        self._session._ensure_alive()
        return []

    def get(self, id: str) -> StreamingQuery | None:
        """Returns an active query from this SparkSession.

        Parameters
        ----------
        id : str
            The unique id of specified query.
        """
        self._session._ensure_alive()
        return None

    def awaitAnyTermination(self, timeout: int | None = None) -> bool | None:  # noqa: N802
        """Wait until any of the queries on the associated SparkSession has terminated.

        Parameters
        ----------
        timeout : int, optional
            default ``None``. The waiting time for any streaming query to terminate.
        """
        if timeout is not None and (not isinstance(timeout, (int, float)) or timeout < 0):
            raise PySparkValueError(
                errorClass="VALUE_NOT_POSITIVE",
                messageParameters={
                    "arg_name": "timeout",
                    "arg_value": type(timeout).__name__,
                },
            )
        self._session._ensure_alive()
        _stub_terminal()
