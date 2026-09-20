"""The Iceberg incremental-read door: forward a window of option strings to the engine."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.spark.dataframe import DataFrame

if TYPE_CHECKING:
    from repark.spark.session.session_core import ReparkSession


def load_incremental(
    session: ReparkSession,
    table_name: str,
    window: dict[str, str],
    travel: dict[str, Any] | None,
) -> DataFrame:
    """Read ``table_name`` over an incremental snapshot window (Iceberg reader options).

    ``window`` holds the raw ``start-snapshot-id`` / ``end-snapshot-id`` /
    ``start-timestamp`` / ``end-timestamp`` strings and ``travel`` the time-travel pins
    found beside them; the engine decides every refusal and resolution.
    """
    from repark import _native

    resolved = session.resolve_table_name(table_name, prefer_temp_view=False)
    inner = session._ensure_alive()
    frame = _native.read_iceberg_incremental(
        inner,
        resolved,
        dict(window),
        {key: str(value) for key, value in (travel or {}).items() if value is not None},
    )
    return DataFrame(frame, inner, session._alive_token)
