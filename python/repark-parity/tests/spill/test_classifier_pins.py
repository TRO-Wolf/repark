"""Pins for the three-outcome classifier and the KILLED folding rule."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from matrix_harness import (
    COMPLETED,
    KILLED,
    REFUSED,
    SPILLED,
    MatrixKilledError,
    classify_cell,
    require_no_killed_cells,
    run_cell,
)

_REFUSAL_NAMES: tuple[str, ...] = ("SortExec", "ExternalSorter")

_COMPLETED_NO_SPILL: dict[str, object] = {"outcome": "completed", "spill_bytes": 0}
_COMPLETED_SPILLED: dict[str, object] = {"outcome": "completed", "spill_bytes": 211812352}
_REFUSED_SORTER: dict[str, object] = {
    "outcome": "refused",
    "error_type": "PySparkException",
    "message": (
        "Not enough memory to continue external sort. caused by Resources exhausted: "
        "Failed to allocate additional 10.0 MB for ExternalSorterMerge[0] - fair(pool_size: 2.0 MB)"
    ),
}
_REFUSED_ANALYSIS: dict[str, object] = {
    "outcome": "refused",
    "error_type": "AnalysisException",
    "message": "Resources exhausted: Failed to allocate additional 1.0 MB for SortExec[2]",
}
_REFUSED_MEMORY: dict[str, object] = {
    "outcome": "refused",
    "error_type": "MemoryError",
    "message": "",
}


def test_classifier_three_outcomes_only() -> None:
    """Completed inputs fold to completed or spilled, refusals to refused, nothing else exists."""
    assert classify_cell(0, _COMPLETED_NO_SPILL, _REFUSAL_NAMES) == COMPLETED
    assert classify_cell(0, _COMPLETED_SPILLED, _REFUSAL_NAMES) == SPILLED
    assert classify_cell(0, _REFUSED_SORTER, _REFUSAL_NAMES) == REFUSED
    assert classify_cell(0, _REFUSED_ANALYSIS, _REFUSAL_NAMES) == REFUSED
    assert classify_cell(0, _REFUSED_MEMORY, _REFUSAL_NAMES) == REFUSED
    vocabulary = {
        classify_cell(0, dict(payload), _REFUSAL_NAMES)
        for payload in (
            _COMPLETED_NO_SPILL,
            _COMPLETED_SPILLED,
            _REFUSED_SORTER,
            _REFUSED_ANALYSIS,
            _REFUSED_MEMORY,
        )
    }
    assert vocabulary == {COMPLETED, SPILLED, REFUSED}


def test_classifier_never_folds_a_non_refusal_failure() -> None:
    """Anything outside the card's three outcomes folds to KILLED, never to a fourth name."""
    degraded: dict[str, object] = {"outcome": "degraded", "skipped_aggregation_rows": 5}
    clean_error: dict[str, object] = {"outcome": "clean_error", "message": "Resources exhausted"}
    ok: dict[str, object] = {"outcome": "ok"}
    unnamed: dict[str, object] = {
        "outcome": "refused",
        "error_type": "PySparkException",
        "message": "unrelated planner trouble",
    }
    untyped: dict[str, object] = {
        "outcome": "refused",
        "error_type": "ValueError",
        "message": "SortExec refused",
    }
    no_spill_bytes: dict[str, object] = {"outcome": "completed"}
    negative_spill: dict[str, object] = {"outcome": "completed", "spill_bytes": -1}
    assert classify_cell(0, degraded, _REFUSAL_NAMES) == KILLED
    assert classify_cell(0, clean_error, _REFUSAL_NAMES) == KILLED
    assert classify_cell(0, ok, _REFUSAL_NAMES) == KILLED
    assert classify_cell(0, unnamed, _REFUSAL_NAMES) == KILLED
    assert classify_cell(0, untyped, _REFUSAL_NAMES) == KILLED
    assert classify_cell(0, no_spill_bytes, _REFUSAL_NAMES) == KILLED
    assert classify_cell(0, negative_spill, _REFUSAL_NAMES) == KILLED
    assert classify_cell(0, None, _REFUSAL_NAMES) == KILLED
    assert classify_cell(3, _COMPLETED_SPILLED, _REFUSAL_NAMES) == KILLED


def test_killed_subprocess_fails_matrix(tmp_path: Path) -> None:
    """A killed or non-zero subprocess without a refusal is KILLED and fails the matrix."""
    killed = run_cell(
        [sys.executable, "-c", "import os; os.kill(os.getpid(), 9)"],
        tmp_path / "killed.json",
        tmp_path,
        30.0,
    )
    assert killed.outcome == KILLED
    assert killed.returncode != 0
    failed = run_cell(
        [sys.executable, "-c", "raise SystemExit(3)"],
        tmp_path / "failed.json",
        tmp_path,
        30.0,
    )
    assert failed.outcome == KILLED
    silent = run_cell([sys.executable, "-c", "pass"], tmp_path / "silent.json", tmp_path, 30.0)
    assert silent.outcome == KILLED
    assert silent.returncode == 0
    with pytest.raises(MatrixKilledError):
        require_no_killed_cells([killed, failed, silent])
