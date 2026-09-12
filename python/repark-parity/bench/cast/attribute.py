"""Attribution probes: parse, plan, optimizer passes, physical, execute."""

from __future__ import annotations

import functools
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from cast.measure import (
    elapsed_compute_seconds,
    explain_text,
    native_debug_assertions,
    open_session,
    register_source,
)
from cast.models import AttributionRecord, PhaseSample
from cast.shapes import ROW_COUNT, sql_for_shape

OPTIMIZED_BY: re.Pattern[str] = re.compile(r"optimized by ([A-Za-z][A-Za-z0-9_]+)")
VERBOSE_RULE: re.Pattern[str] = re.compile(
    r"(?:Optimizer rule:\s*|rule\s+)['\"]([A-Za-z][A-Za-z0-9_]+)['\"]"
)


def wall_seconds(operation: Any) -> float:
    """Wall-clock one nullary call."""
    started = time.perf_counter()
    operation()
    return time.perf_counter() - started


def optimizer_pass_names(verbose_text: str) -> list[str]:
    """Named optimizer rules mentioned in an EXPLAIN VERBOSE dump, in order."""
    names: list[str] = []
    seen: set[str] = set()
    for pattern in (OPTIMIZED_BY, VERBOSE_RULE):
        for match in pattern.finditer(verbose_text):
            name = match.group(1)
            if name in seen:
                continue
            seen.add(name)
            names.append(name)
    return names


def invoke_sql(session: Any, sql: str) -> None:
    """session.sql without collecting."""
    session.sql(sql)


def invoke_collect_sql(session: Any, sql: str) -> None:
    """session.sql(...).collect()."""
    session.sql(sql).collect()


def invoke_shape_sql(shape: str, cast_count: int) -> None:
    """Build the SQL string for one shape and discard it."""
    sql_for_shape(shape, cast_count)


def time_sql(session: Any, sql: str) -> float:
    """Time session.sql without collecting."""
    return wall_seconds(functools.partial(invoke_sql, session, sql))


def time_collect_sql(session: Any, sql: str) -> float:
    """Time session.sql(...).collect()."""
    return wall_seconds(functools.partial(invoke_collect_sql, session, sql))


def plan_with_max_passes(sql: str, row_count: int, max_passes: int) -> float:
    """Open a fresh session with max_passes set and time session.sql."""
    session = open_session(
        f"cast-cost-attr-pass{max_passes}",
        extra_config={"datafusion.optimizer.max_passes": str(max_passes)},
    )
    try:
        register_source(session, row_count)
        return time_sql(session, sql)
    finally:
        session.stop()


def verbose_plan_text(session: Any, sql: str) -> str:
    """EXPLAIN VERBOSE text, or a measured-unavailable note."""
    try:
        return explain_text(session, f"VERBOSE {sql}", analyze=False)
    except Exception as error:
        return f"EXPLAIN VERBOSE unavailable: {error}"


def run_attribution(
    *,
    shape: str,
    cast_count: int,
    row_count: int = ROW_COUNT,
    profiler_summary: str = "",
    profiler: str = "",
) -> AttributionRecord:
    """Probe one cell: SQL width, plan, optimizer knobs, EXPLAIN ANALYZE."""
    sql = sql_for_shape(shape, cast_count)
    session = open_session("cast-cost-attr")
    try:
        register_source(session, row_count)
        width_sql = f"SELECT {', '.join(f'1 AS c{index}' for index in range(cast_count))}"
        phases = [
            PhaseSample(
                name="sql_string_build",
                seconds=wall_seconds(functools.partial(invoke_shape_sql, shape, cast_count)),
                note="Python SQL text construction only",
            ),
            PhaseSample(
                name="sql_parse_literals_same_width",
                seconds=time_sql(session, width_sql),
                note="same SELECT-list width, no CAST, no source scan",
            ),
            PhaseSample(
                name="logical_and_physical_plan",
                seconds=time_sql(session, sql),
                note="session.sql: parse + analyzer + optimizer + DataFrame wrap",
            ),
            PhaseSample(
                name="explain",
                seconds=time_collect_sql(session, f"EXPLAIN {sql}"),
                note="logical + physical plan text, no execution",
            ),
            PhaseSample(
                name="explain_analyze",
                seconds=time_collect_sql(session, f"EXPLAIN ANALYZE {sql}"),
                note="plans again, then per-batch evaluation; output discarded",
            ),
        ]
        plan_text = explain_text(session, sql, analyze=True)
        elapsed = elapsed_compute_seconds(plan_text)
        phases.append(
            PhaseSample(
                name="explain_analyze_elapsed_compute",
                seconds=elapsed,
                note="sum of DataFusion elapsed_compute counters",
            )
        )
        names = optimizer_pass_names(verbose_plan_text(session, sql))
    finally:
        session.stop()
    phases.append(
        PhaseSample(
            name="plan_max_passes_0",
            seconds=plan_with_max_passes(sql, row_count, 0),
            note="optimizer loop skipped (max_passes=0)",
        )
    )
    phases.append(
        PhaseSample(
            name="plan_max_passes_1",
            seconds=plan_with_max_passes(sql, row_count, 1),
            note="one optimizer pass over the default rule list",
        )
    )
    phases.append(
        PhaseSample(
            name="plan_max_passes_3",
            seconds=plan_with_max_passes(sql, row_count, 3),
            note="DataFusion default max_passes=3",
        )
    )
    return AttributionRecord(
        shape=shape,
        cast_count=cast_count,
        row_count=row_count,
        native_debug=native_debug_assertions(),
        phases=phases,
        optimizer_pass_names=names,
        elapsed_compute_seconds=elapsed,
        physical_cast_count=plan_text.count("CAST(") + plan_text.count("TryCast("),
        plan_text_bytes=len(plan_text.encode("utf-8")),
        profiler=profiler,
        profiler_summary=profiler_summary,
    )


def runner_path() -> Path:
    """CLI path for the CAST runner."""
    return Path(__file__).resolve().parent / "run_cast.py"


def capture_py_entry_spans(
    *,
    shape: str,
    cast_count: int,
    row_count: int,
    timeout_s: int = 600,
) -> str:
    """Re-run one sql()+EXPLAIN ANALYZE under REPARK_LOG=info and return stderr."""
    env = os.environ.copy()
    env["REPARK_LOG"] = "info"
    completed = subprocess.run(
        [
            sys.executable,
            str(runner_path()),
            "--shapes",
            shape,
            "--sizes",
            str(cast_count),
            "--rows",
            str(row_count),
            "--warmup",
            "0",
            "--repeats",
            "1",
            "--out",
            os.devnull,
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout_s,
    )
    return completed.stderr[-8000:]
