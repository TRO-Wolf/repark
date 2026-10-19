"""DEEP-FILTER-CHAIN-CRASH-1 CI-segv fold — deep expression builds never overflow a caller stack.

pins: deep-filter-chain-crash-1/C-012
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

pytestmark = pytest.mark.slow

_PREAMBLE = """
import json
import threading
from functools import reduce

from repark import ReparkSession
from repark.spark import functions as F

results = {}
session = ReparkSession.builder.appName("deep-expr-build-1").getOrCreate()
base = session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")
"""

_EPILOGUE = """
print(json.dumps(results))
"""

_MAIN_THREAD_SHAPE = """
main_or = reduce(
    lambda x, y: x | y, [F.col("a") == i for i in range(6000)]
)
print("main-built", flush=True)
try:
    base.filter(main_or).count()
    results["or_6000_main"] = "answered"
except Exception as raised:
    results["or_6000_main"] = type(raised).__name__
del main_or
"""

_EIGHT_MEG_THREAD_SHAPE = """
threading.stack_size(8 * 1024 * 1024)
small_out = {}
def small_work():
    try:
        built = reduce(
            lambda x, y: x | y, [F.col("a") == i for i in range(6000)]
        )
        base.filter(built).count()
        small_out["v"] = "answered"
    except Exception as raised:
        small_out["v"] = type(raised).__name__
worker = threading.Thread(target=small_work)
worker.start()
worker.join()
print("thread-built", flush=True)
results["or_6000_smallthread"] = small_out["v"]
"""

_TIMEOUT_SECONDS = 600


def _run_shape(shape: str) -> dict[str, object]:
    """Drive one shape of the expression-build battery in its own isolated interpreter.

    Args:
        shape: The shape's worker source; it runs between the shared preamble and epilogue.

    Returns:
        The JSON dict the worker prints on its last stdout line.
    """
    proc = subprocess.run(
        [sys.executable, "-c", _PREAMBLE + shape + _EPILOGUE],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"the expression-build worker must survive every shape (rc={proc.returncode}): "
        f"{proc.stderr[-2000:]}"
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_six_thousand_term_or_build_refuses_through_filter() -> None:
    """A 6,000-term OR builds on the caller thread, then refuses at `filter()`."""
    worker_results = _run_shape(_MAIN_THREAD_SHAPE)
    assert worker_results["or_6000_main"] == "AnalysisException"


def test_six_thousand_term_or_build_refuses_on_small_thread() -> None:
    """A 6,000-term OR builds on an 8 MiB thread, then refuses at `filter()`."""
    worker_results = _run_shape(_EIGHT_MEG_THREAD_SHAPE)
    assert worker_results["or_6000_smallthread"] == "AnalysisException"
