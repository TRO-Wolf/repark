"""W-0: run the window-shape driver at gate scale against the native module.

pins: w-0-window-bench/C-002, C-005, C-006, C-009
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

_BENCH_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "bench"


def _load_measure() -> Any:
    """Import ``windows.measure`` from the bench tree as a synthetic package."""
    import importlib

    package_name = "repark_w0_bench"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_BENCH_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package
    return importlib.import_module(f"{package_name}.windows.measure")


measure = _load_measure()


def _load_roster() -> Any:
    """Import ``windows.roster`` through the same synthetic package as measure."""
    import importlib

    return importlib.import_module("repark_w0_bench.windows.roster")


@pytest.fixture(scope="module")
def gate_run(tmp_path_factory: pytest.TempPathFactory) -> Any:
    """One driver run at gate scale, oracles skipped (no JVM in this suite)."""
    root = tmp_path_factory.mktemp("w0-gate")
    return measure.run_window_measurement(
        root,
        scale="gate",
        skip_duckdb=True,
        skip_pyspark=True,
    )


def test_iceberg_lead_lag_runs_over_an_unsorted_table(gate_run: Any) -> None:
    """C-005: the Iceberg cell writes a table and times lead/lag OVER ORDER BY ts."""
    cell = next(item for item in gate_run.cells if item.label == "iceberg_lead_lag")
    assert cell.rows == measure.GATE_ROWS
    assert "lag(v, 1) OVER (ORDER BY ts)" in cell.sql
    repark = next(timing for timing in cell.timings if timing.engine == "repark")
    assert repark.outcome in {"ok", "error", "refuse", "oom", "spill", "crash"}
    if repark.outcome == "ok":
        assert "SortExec" in repark.plan_tokens or "WindowAggExec" in repark.plan_tokens


def test_memory_limit_cell_records_an_outcome_class(gate_run: Any) -> None:
    """C-006: over-limit cell records one of the five classes; it does not retry."""
    cell = next(item for item in gate_run.cells if item.label.startswith("memory_limit_"))
    repark = next(timing for timing in cell.timings if timing.engine == "repark")
    assert repark.outcome in {"ok", "spill", "oom", "error", "crash"}
    assert gate_run.scratch_deleted is True
    assert gate_run.dataset_bytes


def test_probe_covers_the_roster(gate_run: Any) -> None:
    """C-002 live half: every roster name has exactly one outcome at gate scale."""
    names = [row.name for row in gate_run.probe]
    assert names == list(_load_roster().PROBE_NAMES)
    outcomes = {row.outcome for row in gate_run.probe}
    assert outcomes <= {"ok", "refuse", "absent", "error", "oom", "spill", "crash"}


def test_sliding_refuse_set_matches_the_frozen_roster(gate_run: Any) -> None:
    """C-009: live sliding-accumulator refusals equal the frozen WIN-SLIDE names."""
    roster = _load_roster()
    live = tuple(sorted(row.name for row in gate_run.probe if row.outcome == "refuse"))
    assert live == roster.REFUSING_SLIDING_NAMES
    for name in roster.REFUSING_SLIDING_NAMES:
        row = next(item for item in gate_run.probe if item.name == name)
        assert row.outcome == "refuse"
        assert row.message is not None
        assert "retract_batch" in row.message or "sliding accumulator" in row.message.lower()


def test_remaining_absents_fail_at_planning(gate_run: Any) -> None:
    """C-009 recount: remaining absents are planning misses, not sliding refuses."""
    roster = _load_roster()
    live_absent = tuple(sorted(row.name for row in gate_run.probe if row.outcome == "absent"))
    assert live_absent == roster.ABSENT_PLANNING_NAMES
    for name in roster.ABSENT_PLANNING_NAMES:
        row = next(item for item in gate_run.probe if item.name == name)
        text = row.message or ""
        assert "Error during planning" in text or "Invalid function" in text
        assert "retract_batch" not in text
        assert "sliding accumulator" not in text.lower()
