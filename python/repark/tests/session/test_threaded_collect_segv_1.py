"""THREADED-COLLECT-SEGV-1 — the collect family answers on fresh non-main threads.

pins: threaded-collect-segv-1/C-001, threaded-collect-segv-1/C-002
"""

from __future__ import annotations

import json
import subprocess
import sys
from importlib import metadata

import pytest

_WORKER = """
import json
import sys
import threading

import repark

door = sys.argv[1]
session = repark.ReparkSession.builder.appName("threaded-collect-segv-1").getOrCreate()
counts = []
failures = []
pyarrow_on_main_before_first_thread = "pyarrow" in sys.modules


def frame():
    return session.range(0, 2000).selectExpr("id", "id % 10 as p", "cast(id as string) as s")


def use(target):
    if door == "collect":
        return len(target.collect())
    if door == "take":
        return len(target.take(7))
    if door == "head":
        return len(target.head(7))
    if door == "first":
        return len(target.first())
    if door == "toLocalIterator":
        return sum(1 for _ in target.toLocalIterator())
    if door == "toArrow":
        return target.toArrow().num_rows
    raise SystemExit(f"unknown door {door}")


def run():
    try:
        counts.append(use(frame()))
    except Exception as raised:
        failures.append(f"{type(raised).__name__}: {raised}")


for _ in range(6):
    thread = threading.Thread(target=run)
    thread.start()
    thread.join()

print(json.dumps({
    "counts": counts,
    "failures": failures,
    "pyarrow_on_main_before_first_thread": pyarrow_on_main_before_first_thread,
}))
"""

_EXPECTED_COUNT = {
    "collect": 2000,
    "take": 7,
    "head": 7,
    "first": 3,
    "toLocalIterator": 2000,
    "toArrow": 2000,
}

_TIMEOUT_SECONDS = 300

_PYARROW_25_0_0_REASON = (
    "pyarrow 25.0.0 bundles a mimalloc whose mi_thread_init dereferences a null pointer when "
    "libarrow is first loaded on a non-main thread that exits (apache/arrow GH-50471, fixed in "
    "pyarrow 25.0.1); the crash is pyarrow's and reproduces without RePark"
)


@pytest.mark.parametrize("door", sorted(_EXPECTED_COUNT))
def test_door_answers_on_six_fresh_threads_threaded_collect_segv_1(door: str) -> None:
    """pins: threaded-collect-segv-1/C-001, threaded-collect-segv-1/C-002."""
    if metadata.version("pyarrow") == "25.0.0":
        pytest.skip(_PYARROW_25_0_0_REASON)
    proc = subprocess.run(
        [sys.executable, "-c", _WORKER, door],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"{door} on fresh threads must not kill the interpreter (rc={proc.returncode}): "
        f"{proc.stderr[-2000:]}"
    )
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result["pyarrow_on_main_before_first_thread"] is False, (
        "the worker must first load pyarrow on a non-main thread, or it does not exercise the "
        "crash's precondition"
    )
    assert result["failures"] == []
    assert result["counts"] == [_EXPECTED_COUNT[door]] * 6
