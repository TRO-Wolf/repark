"""DuckDB and PySpark 4.1.2 adapters. Imported only by the measurement driver."""

from __future__ import annotations

import os
import statistics
import time
from pathlib import Path
from typing import Any

from .classify import OUTCOME_ERROR, OUTCOME_OK, OUTCOME_SKIP, classify_exception_text
from .models import CellTiming
from .roster import DEFAULT_ITERATIONS, DEFAULT_WARMUP

ZULU_17 = Path("/usr/lib/jvm/zulu-17-amd64")
DUCKDB_PIN = "1.5.5"
PYSPARK_PIN = "4.1.2"


def peak_rss_bytes() -> int:
    """Peak resident set of this process in bytes (Linux ``ru_maxrss`` KiB)."""
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def ensure_java_home() -> str | None:
    """Prefer a Java 17 home for PySpark 4.1.2; return the path used or None."""
    current = os.environ.get("JAVA_HOME")
    if current and Path(current, "bin", "java").is_file():
        return current
    if ZULU_17.joinpath("bin", "java").is_file():
        os.environ["JAVA_HOME"] = str(ZULU_17)
        java_bin = str(ZULU_17 / "bin")
        path = os.environ.get("PATH", "")
        if java_bin not in path.split(":"):
            os.environ["PATH"] = java_bin + ":" + path
        return str(ZULU_17)
    return current


def duckdb_version() -> tuple[str | None, str | None]:
    """Return ``(version, skip_reason)`` for DuckDB."""
    try:
        import duckdb
    except ImportError as error:
        return None, f"import_error:{error}"
    version = str(duckdb.__version__)
    if version != DUCKDB_PIN:
        return version, f"version_mismatch:have={version} pin={DUCKDB_PIN}"
    return version, None


def pyspark_version() -> tuple[str | None, str | None]:
    """Return ``(version, skip_reason)`` for PySpark."""
    try:
        import pyspark
    except ImportError as error:
        return None, f"import_error:{error}"
    version = str(pyspark.__version__)
    if version != PYSPARK_PIN:
        return version, f"version_mismatch:have={version} pin={PYSPARK_PIN}"
    java_home = ensure_java_home()
    if java_home is None:
        return version, "no_java_home"
    return version, None


def _median(samples: list[float]) -> float | None:
    """Median of a non-empty sample list."""
    if not samples:
        return None
    return float(statistics.median(samples))


def run_duckdb_sql(
    parquet_path: Path,
    sql: str,
    *,
    warmup: int = DEFAULT_WARMUP,
    iterations: int = DEFAULT_ITERATIONS,
) -> CellTiming:
    """Time ``sql`` on DuckDB against the seed parquet registered as ``t``.

    Args:
        parquet_path: seed file.
        sql: statement that reads ``t``.
        warmup: untimed passes.
        iterations: timed passes.

    Returns:
        A :class:`CellTiming` for engine ``duckdb``.
    """
    version, skip = duckdb_version()
    if skip is not None and version is None:
        return CellTiming(
            engine="duckdb", outcome=OUTCOME_SKIP, warmup=warmup, iterations=0, message=skip
        )
    try:
        import duckdb
    except ImportError as error:
        return CellTiming(
            engine="duckdb",
            outcome=OUTCOME_SKIP,
            warmup=warmup,
            iterations=0,
            message=f"import_error:{error}",
        )
    connection = duckdb.connect()
    try:
        connection.execute(
            f"CREATE OR REPLACE VIEW t AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        for _ in range(warmup):
            connection.execute(sql).fetch_arrow_table()
        samples: list[float] = []
        answer: Any = None
        for _ in range(iterations):
            started = time.perf_counter()
            table = connection.execute(sql).fetch_arrow_table()
            samples.append((time.perf_counter() - started) * 1000.0)
            answer = table.to_pylist()
        return CellTiming(
            engine="duckdb",
            outcome=OUTCOME_OK,
            warmup=warmup,
            iterations=iterations,
            samples_ms=samples,
            median_ms=_median(samples),
            peak_rss_bytes=peak_rss_bytes(),
            answer=answer,
            version=version,
        )
    except Exception as error:
        text = f"{type(error).__name__}: {error}"
        return CellTiming(
            engine="duckdb",
            outcome=classify_exception_text(text),
            warmup=warmup,
            iterations=0,
            message=text,
            version=version,
            peak_rss_bytes=peak_rss_bytes(),
        )
    finally:
        connection.close()


def run_pyspark_sql(
    parquet_path: Path,
    sql: str,
    *,
    warmup: int = DEFAULT_WARMUP,
    iterations: int = DEFAULT_ITERATIONS,
    session: Any | None = None,
) -> CellTiming:
    """Time ``sql`` on live PySpark 4.1.2 against the seed parquet as ``t``.

    Args:
        parquet_path: seed file.
        sql: statement that reads ``t``.
        warmup: untimed passes.
        iterations: timed passes.
        session: an existing ``SparkSession``, or None to build one.

    Returns:
        A :class:`CellTiming` for engine ``pyspark``.
    """
    version, skip = pyspark_version()
    if skip is not None:
        return CellTiming(
            engine="pyspark",
            outcome=OUTCOME_SKIP,
            warmup=warmup,
            iterations=0,
            message=skip,
            version=version,
        )
    owns_session = session is None
    try:
        if session is None:
            from pyspark.sql import SparkSession

            session = (
                SparkSession.builder.master("local[1]")
                .appName("w0-oracle")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
        session.read.parquet(str(parquet_path)).createOrReplaceTempView("t")
        for _ in range(warmup):
            session.sql(sql).toArrow()
        samples: list[float] = []
        answer: Any = None
        for _ in range(iterations):
            started = time.perf_counter()
            table = session.sql(sql).toArrow()
            samples.append((time.perf_counter() - started) * 1000.0)
            answer = table.to_pylist()
        return CellTiming(
            engine="pyspark",
            outcome=OUTCOME_OK,
            warmup=warmup,
            iterations=iterations,
            samples_ms=samples,
            median_ms=_median(samples),
            peak_rss_bytes=peak_rss_bytes(),
            answer=answer,
            version=version,
        )
    except Exception as error:
        text = f"{type(error).__name__}: {error}"
        outcome = classify_exception_text(text)
        if outcome == "error" and "toArrow" in text:
            outcome = OUTCOME_ERROR
        return CellTiming(
            engine="pyspark",
            outcome=outcome,
            warmup=warmup,
            iterations=0,
            message=text,
            version=version,
            peak_rss_bytes=peak_rss_bytes(),
        )
    finally:
        if owns_session and session is not None:
            session.stop()
