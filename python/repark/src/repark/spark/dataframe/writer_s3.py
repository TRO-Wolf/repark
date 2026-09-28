"""S3 path-write forward — route ``s3://`` and ``s3a://`` writer paths to the engine."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from repark import _native

_S3_SCHEMES = frozenset({"s3", "s3a"})


def is_s3_url(path: str) -> bool:
    """Tell whether ``path`` addresses S3 without touching the filesystem."""
    return urlsplit(str(path)).scheme.lower() in _S3_SCHEMES


def write_s3_path(writer: Any, path: str, *, stored_as: str) -> None:
    """Forward one S3 path write to the engine save-mode protocol.

    The engine owns the save mode, the part layout, and the commit; this
    forward only carries the writer state across the binding.
    """
    dataframe = writer._dataframe
    dataframe._ensure_alive()
    _native.session_write_path(
        dataframe._session,
        dataframe._native_for_registration(),
        path,
        stored_as.lower(),
        writer._mode,
        dict(writer._options),
        [str(column) for column in writer._partition_columns],
    )
