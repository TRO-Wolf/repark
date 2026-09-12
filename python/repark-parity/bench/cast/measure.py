"""Time CAST plan shapes: warmup, repetitions, medians, CSV rows."""

from __future__ import annotations

import csv
import math
import os
import re
import statistics
import time
from pathlib import Path
from typing import Any, Final

from cast.models import CellRecord
from cast.shapes import CAST_COUNTS, ROW_COUNT, SHAPES, SOURCE_VIEW, cast_token_count, sql_for_shape

CSV_COLUMNS: Final[tuple[str, ...]] = (
    "shape",
    "cast_count",
    "rows",
    "native_debug",
    "rep0_seconds",
    "rep1_seconds",
    "rep2_seconds",
    "median_seconds",
    "median_plan_seconds",
    "median_execute_seconds",
    "sql_bytes",
    "cast_in_sql",
    "execute_kind",
)

ELAPSED_COMPUTE: Final[re.Pattern[str]] = re.compile(
    r"elapsed_compute=([0-9.]+)\s*(ns|us|µs|ms|s)\b"
)

_UNIT_SECONDS: Final[dict[str, float]] = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
}


def native_debug_assertions() -> bool:
    """True when the loaded native module was compiled with debug assertions."""
    from repark import _native

    return bool(getattr(_native, "__debug_assertions__", True))


def open_session(app_name: str = "cast-cost", extra_config: dict[str, str] | None = None) -> Any:
    """Open a ReparkSession, applying optional DataFusion knobs."""
    from repark import ReparkSession

    builder = ReparkSession.builder.appName(app_name)
    if extra_config:
        for key, value in extra_config.items():
            builder = builder.config(key, value)
    return builder.getOrCreate()


def register_source(session: Any, row_count: int = ROW_COUNT, view_name: str = SOURCE_VIEW) -> None:
    """Materialize a range MemTable and register it as the CAST source view."""
    frame = session.range(row_count).eager()
    frame.createOrReplaceTempView(view_name)


def elapsed_compute_seconds(plan_text: str) -> float:
    """Sum every elapsed_compute counter in an EXPLAIN ANALYZE dump."""
    total = 0.0
    for number, unit in ELAPSED_COMPUTE.findall(plan_text):
        total += float(number) * _UNIT_SECONDS[unit]
    return total


def explain_text(session: Any, sql: str, analyze: bool = False) -> str:
    """Run EXPLAIN or EXPLAIN ANALYZE and join the plan column."""
    verb = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    rows = session.sql(f"{verb} {sql}").collect()
    chunks: list[str] = []
    for row in rows:
        mapping = row.asDict(recursive=False) if hasattr(row, "asDict") else None
        if mapping is not None and "plan" in mapping:
            chunks.append(str(mapping["plan"]))
        elif hasattr(row, "__getitem__") and hasattr(row, "__len__") and len(row) > 1:
            chunks.append(str(row[1]))
        else:
            chunks.append(str(row))
    return "\n".join(chunks)


def time_plan_and_execute(session: Any, sql: str) -> tuple[float, float, str]:
    """Time session.sql (plan) then EXPLAIN ANALYZE (execute, output discarded)."""
    started = time.perf_counter()
    _ = session.sql(sql)
    plan_seconds = time.perf_counter() - started
    started = time.perf_counter()
    _ = session.sql(f"EXPLAIN ANALYZE {sql}").collect()
    execute_seconds = time.perf_counter() - started
    return plan_seconds, execute_seconds, "explain_analyze"


def median_of(values: list[float]) -> float:
    """Median of a non-empty sample list."""
    return float(statistics.median(values))


def measure_one_cell(
    *,
    shape: str,
    cast_count: int,
    row_count: int = ROW_COUNT,
    warmup: int = 1,
    repeats: int = 3,
    session: Any | None = None,
) -> CellRecord:
    """Warm up, then time one CAST cell; return medians and the three reps."""
    owns_session = session is None
    working = session if session is not None else open_session()
    try:
        register_source(working, row_count)
        sql = sql_for_shape(shape, cast_count)
        for _ in range(warmup):
            time_plan_and_execute(working, sql)
        plan_reps: list[float] = []
        execute_reps: list[float] = []
        totals: list[float] = []
        execute_kind = "explain_analyze"
        for _ in range(repeats):
            plan_seconds, execute_seconds, execute_kind = time_plan_and_execute(working, sql)
            plan_reps.append(plan_seconds)
            execute_reps.append(execute_seconds)
            totals.append(plan_seconds + execute_seconds)
        while len(totals) < 3:
            totals.append(totals[-1] if totals else 0.0)
            plan_reps.append(plan_reps[-1] if plan_reps else 0.0)
            execute_reps.append(execute_reps[-1] if execute_reps else 0.0)
        return CellRecord(
            shape=shape,
            cast_count=cast_count,
            row_count=row_count,
            native_debug=native_debug_assertions(),
            repetitions=totals[:3],
            plan_repetitions=plan_reps[:3],
            execute_repetitions=execute_reps[:3],
            median_seconds=median_of(totals),
            median_plan_seconds=median_of(plan_reps),
            median_execute_seconds=median_of(execute_reps),
            sql_bytes=len(sql.encode("utf-8")),
            cast_in_sql=cast_token_count(sql),
            execute_kind=execute_kind,
        )
    finally:
        if owns_session:
            working.stop()


def measure_grid(
    *,
    shapes: tuple[str, ...] = SHAPES,
    cast_counts: tuple[int, ...] = CAST_COUNTS,
    row_count: int = ROW_COUNT,
    warmup: int = 1,
    repeats: int = 3,
) -> list[CellRecord]:
    """Time every requested cell on one session and one source view."""
    session = open_session()
    records: list[CellRecord] = []
    try:
        for shape in shapes:
            for cast_count in cast_counts:
                records.append(
                    measure_one_cell(
                        shape=shape,
                        cast_count=cast_count,
                        row_count=row_count,
                        warmup=warmup,
                        repeats=repeats,
                        session=session,
                    )
                )
        return records
    finally:
        session.stop()


def write_cells_csv(path: Path, records: list[CellRecord]) -> None:
    """Write one CSV row per cell."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        for record in records:
            reps = [*list(record.repetitions), 0.0, 0.0, 0.0]
            writer.writerow(
                {
                    "shape": record.shape,
                    "cast_count": record.cast_count,
                    "rows": record.row_count,
                    "native_debug": str(record.native_debug).lower(),
                    "rep0_seconds": f"{reps[0]:.6f}",
                    "rep1_seconds": f"{reps[1]:.6f}",
                    "rep2_seconds": f"{reps[2]:.6f}",
                    "median_seconds": f"{record.median_seconds:.6f}",
                    "median_plan_seconds": f"{record.median_plan_seconds:.6f}",
                    "median_execute_seconds": f"{record.median_execute_seconds:.6f}",
                    "sql_bytes": record.sql_bytes,
                    "cast_in_sql": record.cast_in_sql,
                    "execute_kind": record.execute_kind,
                }
            )


def exponent_fit(sizes: list[int], times: list[float]) -> float:
    """Least-squares exponent b in time ~ a * size^b from three (or more) points."""
    log_size = [math.log(size) for size in sizes]
    log_time = [math.log(max(sample, 1e-12)) for sample in times]
    count = len(sizes)
    mean_size = sum(log_size) / count
    mean_time = sum(log_time) / count
    numerator = sum(
        (size_value - mean_size) * (time_value - mean_time)
        for size_value, time_value in zip(log_size, log_time, strict=True)
    )
    denominator = sum((size_value - mean_size) ** 2 for size_value in log_size)
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


def load1() -> float:
    """The 1-minute load average."""
    return os.getloadavg()[0]
