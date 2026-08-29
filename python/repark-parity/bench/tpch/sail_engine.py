"""Sail (LakeSail) Spark Connect engine adapter for the TPC-H scoreboard.

Sail is **measurement prior-art only** — not a RePark product dependency. This module
is imported only when ``--engine sail`` / ``both`` is selected. CI never requires
``pysail`` or ``pyspark-client`` (repo uv.lock / pyproject untouched).

Lifecycle: start ``SparkConnectServer`` (loopback gRPC) → remote ``SparkSession`` →
register the eight TPC-H parquet tables as temp views (same short names as repark).
"""

from __future__ import annotations

import logging
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from .datagen import TABLES

LOGGER = logging.getLogger(__name__)


class SailUnavailableError(RuntimeError):
    """Raised when pysail / pyspark-client cannot be imported in this interpreter."""


class SailSession:
    """Owns a Sail Spark Connect server + remote session for the scoreboard lifetime."""

    def __init__(self, spark: Any, server: Any, *, port: int, version: str) -> None:
        self.spark = spark
        self.server = server
        self.port = port
        self.version = version

    def stop(self) -> None:
        try:
            self.spark.stop()
        except Exception:
            LOGGER.exception("Sail SparkSession.stop failed")
        try:
            stop = getattr(self.server, "stop", None)
            if callable(stop):
                stop()
        except Exception:
            LOGGER.exception("Sail SparkConnectServer.stop failed")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        self.stop()


def require_sail_imports() -> tuple[Any, Any]:
    """Import pysail + pyspark; raise :class:`SailUnavailableError` if missing."""
    try:
        from pysail.spark import SparkConnectServer  # type: ignore[import-not-found]
    except Exception as exc:
        msg = (
            "pysail is not importable in this interpreter. Install into a separate "
            "Sail venv (never into repo pyproject/uv.lock): pip install pysail "
            f"pyspark-client. Underlying error: {type(exc).__name__}: {exc}"
        )
        raise SailUnavailableError(msg) from exc
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except Exception as exc:
        msg = (
            "pyspark-client / pyspark is not importable (needed for Spark Connect). "
            "Install pyspark-client into the Sail venv only. "
            f"Underlying error: {type(exc).__name__}: {exc}"
        )
        raise SailUnavailableError(msg) from exc
    return SparkConnectServer, SparkSession


def sail_package_versions() -> dict[str, str]:
    """Return installed pysail / pyspark version strings (empty values if missing)."""
    versions: dict[str, str] = {"pysail": "", "pyspark": ""}
    try:
        import pysail  # type: ignore[import-not-found]

        versions["pysail"] = str(getattr(pysail, "__version__", "") or "unknown")
    except Exception:
        pass
    try:
        import pyspark  # type: ignore[import-not-found]

        versions["pyspark"] = str(getattr(pyspark, "__version__", "") or "unknown")
    except Exception:
        pass
    return versions


def open_sail_over_parquet(data_dir: Path) -> SailSession:
    """Start Sail Spark Connect on a free local port and register TPC-H parquet views."""
    connect_server_cls, spark_session_cls = require_sail_imports()
    versions = sail_package_versions()
    version_label = (
        f"pysail={versions.get('pysail') or '?'} pyspark-client={versions.get('pyspark') or '?'}"
    )

    server = connect_server_cls()
    server.start()
    listening = getattr(server, "listening_address", None)
    if listening is None:
        if hasattr(server, "stop"):
            server.stop()
        msg = "Sail SparkConnectServer started but listening_address is missing"
        raise RuntimeError(msg)
    _host, port = listening
    port_int = int(port)
    remote = f"sc://localhost:{port_int}"
    LOGGER.info("Sail Spark Connect listening on %s (%s)", remote, version_label)

    spark: Any = None
    try:
        spark = spark_session_cls.builder.remote(remote).getOrCreate()
        for table_name in TABLES:
            path = data_dir / f"{table_name}.parquet"
            spark.read.parquet(str(path)).createOrReplaceTempView(table_name)
            LOGGER.info("Sail temp view ready: %s", table_name)
    except Exception:
        # Stop remote session first when partially created, then the Connect server
        # (C1-Q-003) — otherwise a failed view register leaks the client session.
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                LOGGER.exception("Sail SparkSession.stop after open failure")
        try:
            if hasattr(server, "stop"):
                server.stop()
        except Exception:
            LOGGER.exception("Sail server stop after open failure")
        raise

    return SailSession(spark, server, port=port_int, version=version_label)


def collect_rows(spark: Any, sql: str) -> list[tuple[Any, ...]]:
    """Collect Sail / Spark Connect SQL results as tuples (no Arrow path required)."""
    frame = spark.sql(sql)
    # Connect clients often lack to_arrow; collect() is the portable path.
    if hasattr(frame, "to_arrow"):
        try:
            table = frame.to_arrow()
            names = list(table.column_names)
            return [tuple(row[name] for name in names) for row in table.to_pylist()]
        except Exception:
            LOGGER.debug("Sail to_arrow failed; falling back to collect()", exc_info=True)
    rows = frame.collect()
    return [tuple(row) for row in rows]
