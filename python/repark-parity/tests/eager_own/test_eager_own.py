"""EAGER-OWN-1 step-0 pins: each bare ``eager()`` call leaves one orphan ``__repark_cache_*`` view.

pins: eager-own-1/C-001
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

_WORKER_PATH: Path = Path(__file__).resolve().parent / "eager_own_worker.py"
_BENCH_ENV = "REPARK_EAGER_OWN_BENCH"
_BENCH_JSON_ENV = "REPARK_EAGER_OWN_BENCH_JSON"
_SMALL_ROWS = 2_000
_SMALL_ITERATIONS = 3
_BENCH_ROWS = 1_000_000
_BENCH_ITERATIONS = 10
_SMALL_TIMEOUT_S = 300.0
_BENCH_TIMEOUT_S = 3600.0
_TOTAL_COLUMNS = 25


def _run_worker(rows: int, iterations: int, json_out: Path, timeout_s: float) -> dict[str, Any]:
    """Run the measurement worker in a subprocess and return its JSON payload."""
    proc = subprocess.run(
        [
            sys.executable,
            str(_WORKER_PATH),
            "--rows",
            str(rows),
            "--iterations",
            str(iterations),
            "--json-out",
            str(json_out),
        ],
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )
    assert proc.returncode == 0, f"eager_own worker exited {proc.returncode}: {proc.stderr[-2000:]}"
    return json.loads(json_out.read_text(encoding="utf-8"))


def _assert_accumulation_shape(payload: dict[str, Any], iterations: int) -> None:
    """The base-tree defect: one orphan ``__repark_cache_*`` registration per bare eager()."""
    assert len(payload["iterations"]) == iterations
    assert payload["fixture"]["total_columns"] == _TOTAL_COLUMNS
    for record in payload["iterations"]:
        assert record["registrations_after_call"] == record["iteration"] + 1
    assert payload["post_loop"]["registrations"] == iterations
    assert payload["post_gc"]["registrations"] == iterations
    assert payload["post_gc"]["temp_view_registrations"] == iterations
    assert payload["post_clear_cache"]["registrations"] == 0
    assert payload["post_clear_cache"]["temp_view_registrations"] == 0


def test_bare_eager_registrations_accumulate_small(tmp_path: Path) -> None:
    """2,000 rows x 3 bare eager() calls: three orphan registrations on the base tree."""
    payload = _run_worker(_SMALL_ROWS, _SMALL_ITERATIONS, tmp_path / "small.json", _SMALL_TIMEOUT_S)
    _assert_accumulation_shape(payload, _SMALL_ITERATIONS)


@pytest.mark.skipif(
    os.environ.get(_BENCH_ENV) != "1",
    reason="the 1e6-row x 10-iteration loop is opt-in; set REPARK_EAGER_OWN_BENCH=1",
)
def test_bare_eager_registrations_accumulate_million_rows(tmp_path: Path) -> None:
    """The card fixture: 1e6 rows x 10 bare eager() calls leave ten registrations."""
    json_out = Path(os.environ.get(_BENCH_JSON_ENV, str(tmp_path / "bench.json")))
    payload = _run_worker(_BENCH_ROWS, _BENCH_ITERATIONS, json_out, _BENCH_TIMEOUT_S)
    _assert_accumulation_shape(payload, _BENCH_ITERATIONS)
