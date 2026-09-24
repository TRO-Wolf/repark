"""The Iceberg path-read door: ``format("iceberg").load`` arguments containing ``/``.

Spark's ``IcebergSource`` rule — a ``load`` argument containing ``/`` is a filesystem
path, not a table identifier. A path ending ``.metadata.json`` pins that metadata
file; any other path is a table location whose current metadata the engine resolves
through ``version-hint.text`` or the highest-numbered metadata file. Time-travel and
incremental reader options refuse here — a path already names the snapshot — and
nothing is registered in a catalog.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from repark.spark.dataframe import DataFrame
from repark.spark.session.reader_support import (
    _ICEBERG_INCREMENTAL_OPTIONS,
    _ICEBERG_TIME_TRAVEL_OPTIONS,
)

if TYPE_CHECKING:
    from repark.spark.session.reader import DataFrameReader


def load_iceberg_path(reader: DataFrameReader, path: str) -> DataFrame:
    """Read the Iceberg table at ``path`` as one pinned static snapshot.

    ``DataFrameReader.load`` runs the reader's semantic-option gate
    (``_reject_unsupported_semantic_options``) before this door, so the options it
    refuses never arrive here. Of the options that pass it, time-travel
    (``snapshot-id`` / ``as-of-timestamp`` / ``branch`` / ``tag`` / ``versionAsOf`` /
    ``timestampAsOf``) and incremental window options refuse with the pinned
    ``AnalysisException``, and every remaining option is ignored.
    """
    from repark import _native
    from repark.errors import AnalysisException

    unsupported = sorted(
        key.lower()
        for key in reader._options
        if key.lower() in _ICEBERG_TIME_TRAVEL_OPTIONS
        or key.lower() in _ICEBERG_INCREMENTAL_OPTIONS
    )
    if unsupported:
        raise AnalysisException(
            "format('iceberg').load(<path>) reads one pinned metadata snapshot and "
            "does not support time-travel or incremental options; "
            f"got {', '.join(unsupported)}"
        )
    session = reader._session
    inner = session._ensure_alive()
    frame = _native.read_iceberg_path(inner, path)
    return DataFrame(frame, inner, session._alive_token)
