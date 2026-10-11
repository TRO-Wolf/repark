from __future__ import annotations

import json
from typing import TYPE_CHECKING, NoReturn

from repark import _native
from repark.errors import (
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
)

if TYPE_CHECKING:
    from repark._native import PyStreamingQuery, StreamingQueryException
    from repark.spark.session.session_core import ReparkSession


def _not_implemented(feature: str) -> NoReturn:
    raise PySparkNotImplementedError(
        f"[NOT_IMPLEMENTED] {feature} is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": feature},
    )


class StreamingQuery:
    """A handle to a query that is executing continuously in the background as new data arrives.

    All these methods are thread-safe.
    """

    def __init__(self, handle: PyStreamingQuery) -> None:
        """Bind to a native query handle; only the writer start doors construct queries."""
        self._handle = handle

    @property
    def id(self) -> str:
        """Returns the unique id of this query."""
        return self._handle.id()

    @property
    def runId(self) -> str:  # noqa: N802 — PySpark property name
        """Returns the unique id of this run of the query."""
        return self._handle.run_id()

    @property
    def name(self) -> str | None:
        """Returns the name of the query, or None if not specified."""
        return self._handle.name()

    @property
    def isActive(self) -> bool:  # noqa: N802 — PySpark property name
        """Whether this query is currently active."""
        return self._handle.is_active()

    @property
    def status(self) -> dict[str, object]:
        """Returns the current status of the query."""
        return json.loads(self._handle.status())

    @property
    def recentProgress(self) -> list[dict[str, object]]:  # noqa: N802 — PySpark property name
        """Returns an array of the most recent progress updates for this query."""
        return [json.loads(text) for text in self._handle.recent_progress()]

    @property
    def lastProgress(self) -> dict[str, object] | None:  # noqa: N802 — PySpark property name
        """Returns the most recent progress update of this streaming query."""
        text = self._handle.last_progress()
        return None if text is None else json.loads(text)

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
        return self._handle.await_termination(timeout)

    def stop(self) -> None:
        """Stop this streaming query."""
        self._handle.stop()

    def exception(self) -> StreamingQueryException | None:
        """Returns the StreamingQueryException if the query was terminated by an exception."""
        return self._handle.exception()

    def processAllAvailable(self) -> None:  # noqa: N802 — PySpark method name
        """Blocks until all available data in the source has been processed.

        Declared: the blocking drain is deferred.

        pins: mb-4/C-043
        """
        _not_implemented("processAllAvailable")

    def explain(self, extended: bool = False) -> None:
        """Prints the (logical and physical) plans to the console.

        Declared: query-plan printing is deferred.

        pins: mb-4/C-043
        """
        _not_implemented("explain")


class StreamingQueryManager:
    """A class to manage all the StreamingQuery StreamingQueries active."""

    def __init__(self, session: ReparkSession) -> None:
        """Bind to the owning session; each ``spark.streams`` access returns a fresh manager."""
        self._session = session

    @property
    def active(self) -> list[StreamingQuery]:
        """Returns a list of active queries associated with this SQLContext."""
        inner = self._session._ensure_alive()
        return [StreamingQuery(handle) for handle in _native.streams_active(inner)]

    def get(self, id: str) -> StreamingQuery | None:
        """Returns an active query from this SparkSession.

        Parameters
        ----------
        id : str
            The unique id of specified query.
        """
        inner = self._session._ensure_alive()
        if not isinstance(id, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={
                    "arg_name": "id",
                    "arg_type": type(id).__name__,
                },
            )
        handle = _native.streams_get(inner, id)
        return None if handle is None else StreamingQuery(handle)

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
        inner = self._session._ensure_alive()
        return _native.streams_await_any_termination(inner, timeout)

    def resetTerminated(self) -> None:  # noqa: N802 — PySpark method name
        """Clears the terminated-query record so awaitAnyTermination waits for a new one."""
        inner = self._session._ensure_alive()
        _native.streams_reset_terminated(inner)

    def addListener(self, listener: object) -> None:  # noqa: N802 — PySpark method name
        """Registers a listener for query life cycle events.

        Declared: listener up-calls are deferred.

        pins: mb-4/C-043
        """
        self._session._ensure_alive()
        _not_implemented("addListener")

    def removeListener(self, listener: object) -> None:  # noqa: N802 — PySpark method name
        """Deregisters a query life cycle listener.

        Declared: listener up-calls are deferred.

        pins: mb-4/C-043
        """
        self._session._ensure_alive()
        _not_implemented("removeListener")
