"""Facade-boundary cells: Arrow export, collect, createDataFrame, and withColumn chains."""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

THREADS = 8
DEFAULT_ITERATIONS = 5
DEFAULT_WARMUP = 1
DEFAULT_FLOOR_REPEATS = 5
FLOOR_CELL = "collect/100000"
CHAIN_DEPTHS = (10, 50, 100)
EXPORT_ROWS = (100_000, 1_000_000)
CREATE_ROWS = 100_000
COLLECT_ITERATIONS = 3


def load1() -> float:
    """The 1-minute load average."""
    return float(Path("/proc/loadavg").read_text(encoding="utf-8").split()[0])


def native_is_release() -> bool:
    """True when the loaded native module was compiled without debug assertions."""
    from repark import _native

    return not getattr(_native, "__debug_assertions__", True)


def release_proof() -> dict[str, Any]:
    """Refuse a debug native module and return its identifying fields."""
    import repark
    from repark import _native

    if not native_is_release():
        msg = "PERF-FACADE-1: refusing to measure on a debug native build"
        raise RuntimeError(msg)
    return {
        "module": repark.__file__,
        "native_bytes": Path(_native.__file__).stat().st_size,
        "debug_assertions": _native.__debug_assertions__,
        "version": getattr(repark, "__version__", None),
    }


def build_session(threads: int = THREADS) -> Any:
    """Build a repark session pinned to ``threads`` shuffle partitions."""
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("facade-bench")
        .config("spark.sql.shuffle.partitions", str(threads))
        .getOrCreate()
    )


def time_cell(
    name: str,
    call: Callable[[], Any],
    *,
    iterations: int = DEFAULT_ITERATIONS,
    warmup: int = DEFAULT_WARMUP,
) -> dict[str, Any]:
    """Time one cell and return its median, min, spread and load window."""
    start_load = load1()
    for _ in range(warmup):
        call()
    samples: list[float] = []
    for _ in range(iterations):
        started = time.perf_counter()
        call()
        samples.append((time.perf_counter() - started) * 1000.0)
    return {
        "cell": name,
        "samples_ms": samples,
        "median_ms": statistics.median(samples),
        "min_ms": min(samples),
        "spread_ms": max(samples) - min(samples),
        "iterations": iterations,
        "warmup": warmup,
        "load1_start": start_load,
        "load1_end": load1(),
    }


def _old_columns(frame: Any) -> list[str]:
    """The pre-PERF-FACADE-1 ``DataFrame.columns``: names from the analyzed schema."""
    frame._ensure_alive()
    if frame._display_names is not None:
        return list(frame._display_names)
    if frame._map_bridge is not None:
        return list(frame._map_bridge["schema"].names)
    return list(frame._inner.column_names())


def _old_iter_bound_columns(frame: Any) -> list[Any]:
    """The pre-PERF-FACADE-1 ``_iter_bound_columns``: one ``columns`` read per column."""
    if frame._display_names is not None and frame._engine_names is not None:
        return [
            frame._bind_engine_display_column(display, engine)
            for display, engine in zip(frame._display_names, frame._engine_names, strict=True)
        ]
    return [frame._bind_schema_column(name) for name in frame.columns]


def build_chain_stacked(base: Any, depth: int) -> Any:
    """Build a depth-N dependent ``withColumn`` chain, each layer stacked on the last."""
    from repark.spark import functions as F  # noqa: N812 — PySpark idiom

    frame = base
    for index in range(depth):
        frame = frame.withColumn(f"c{index}", F.col("v") * index + F.col("vi"))
    return frame


def build_chain_collapsed(base: Any, depth: int) -> Any:
    """Build the same depth-N chain as one flat projection rebuilt on the base each step."""
    from repark.spark import functions as F  # noqa: N812 — PySpark idiom

    expressions = [F.col(name) for name in base.columns]
    frame = base
    for index in range(depth):
        expressions.append((F.col("v") * index + F.col("vi")).alias(f"c{index}"))
        frame = base.select(*expressions)
    return frame


def _old_rows_from_arrow_table(table: Any) -> list[Any]:
    """The pre-PERF-FACADE-1 ``DataFrame._rows_from_arrow_table``: the pure-Python converter."""
    from repark.spark.dataframe.rows_export import rows_from_arrow_table_python

    return rows_from_arrow_table_python(table)


def collect_with_old_converter(frame: Any) -> int:
    """Run ``collect()`` end to end with the pre-unit row converter swapped back in."""
    from repark.spark.dataframe.core import DataFrame

    shipped = DataFrame._rows_from_arrow_table
    DataFrame._rows_from_arrow_table = staticmethod(_old_rows_from_arrow_table)
    try:
        return len(frame.collect())
    finally:
        DataFrame._rows_from_arrow_table = shipped


def rows_via_new(batches: list[Any]) -> int:
    """Materialize rows from pre-collected batches through the shipped converter."""
    from repark.spark.dataframe.rows_export import rows_from_arrow_table

    return sum(len(rows_from_arrow_table(batch)) for batch in batches)


def rows_via_old(batches: list[Any]) -> int:
    """Materialize rows from pre-collected batches through the pure-Python converter."""
    from repark.spark.dataframe.rows_export import rows_from_arrow_table_python

    return sum(len(rows_from_arrow_table_python(batch)) for batch in batches)
