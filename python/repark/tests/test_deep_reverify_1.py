"""DEEP-FILTER-CHAIN-CRASH-1 re-verify fold — VD2 pins on main, small stacks, and GC.

pins: deep-filter-chain-crash-1/C-013, deep-filter-chain-crash-1/C-014,
deep-filter-chain-crash-1/C-015, deep-filter-chain-crash-1/C-016,
deep-filter-chain-crash-1/C-017, deep-filter-chain-crash-1/C-018
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

_MAIN_WORKER = """
import json
from functools import reduce

from repark import ReparkSession
from repark.spark import functions as F

results = {}
session = ReparkSession.builder.appName("deep-reverify-main-1").getOrCreate()
base = session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")

or_text = " OR ".join(f"a = {i}" for i in range(2000))
results["fexpr_2000_or"] = base.filter(F.expr(or_text)).count()
results["selectexpr_2000_or"] = base.selectExpr(f"CASE WHEN {or_text} THEN a END").count()
results["stringfilter_2000_or"] = base.filter(or_text).count()

deep_or = reduce(lambda x, y: x | y, [F.col("a") == i for i in range(1500)])
try:
    base.filter(deep_or).count()
    results["or_1500_df"] = "answered"
except Exception as raised:
    results["or_1500_df"] = type(raised).__name__
del deep_or

mixed = reduce(lambda x, y: x | y, [F.col("a") == i for i in range(300)])
mixed = mixed | F.expr(" OR ".join(f"a = {i}" for i in range(300, 5001)))
results["mixed_5001"] = base.filter(mixed).count()
del mixed

chained = base
for i in range(1000):
    chained = chained.filter(F.col("a") >= i % 7 - 10)
chained.createOrReplaceTempView("deepv")
results["deep_view_count"] = session.sql("SELECT count(*) FROM deepv").collect()[0][0]
text = chained._explain_text() if hasattr(chained, "_explain_text") else ""
results["explain_has_filter"] = "Filter" in text

print(json.dumps(results))
"""

_SMALL_WORKER = """
import gc
import json
import sys
import threading
from functools import reduce

from repark import ReparkSession
from repark.spark import functions as F

STACK = int(sys.argv[1])
results = {}
session = ReparkSession.builder.appName("deep-reverify-small-1").getOrCreate()
base = session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")

chained = base
for i in range(500):
    chained = chained.filter(F.col("a") >= i % 7 - 10)

threading.stack_size(STACK)
out = {}
def work():
    try:
        shallow = base
        for i in range(16):
            shallow = shallow.filter(F.col("a") >= i % 7 - 10)
        out["count16"] = shallow.count()
        built = reduce(lambda x, y: x | y, [F.col("a") == i for i in range(2000)])
        try:
            base.filter(built).count()
            out["or2000"] = "answered"
        except Exception as raised:
            out["or2000"] = type(raised).__name__
        del built
        out["columns"] = len(chained.columns)
        cycle = []
        cycle.append(cycle)
        cycle.append(reduce(lambda x, y: x | y, [F.col("a") == i for i in range(2000)]))
        del cycle
        gc.collect()
        out["gc"] = "survived"
    except BaseException as raised:
        out["failed"] = type(raised).__name__
worker = threading.Thread(target=work)
worker.start()
worker.join()
results.update(out)

print(json.dumps(results))
"""

_GC_WORKER = """
import gc
import json
import threading
from functools import reduce

from repark import ReparkSession
from repark.spark import functions as F

results = {}
session = ReparkSession.builder.appName("deep-reverify-gc-1").getOrCreate()
base = session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")

deep_column = reduce(lambda x, y: x | y, [F.col("a") == i for i in range(6000)])
chained = base
for i in range(1000):
    chained = chained.filter(F.col("a") >= i % 7 - 10)
cycle = []
cycle.append(cycle)
cycle.append(deep_column)
cycle.append(chained)
del deep_column, chained, cycle
collected = gc.collect()
results["gc_main"] = "survived" if collected >= 0 else "failed"

threading.stack_size(256 * 1024)
out = {}
def work():
    try:
        column = reduce(lambda x, y: x | y, [F.col("a") == i for i in range(6000)])
        frame = base
        for i in range(200):
            frame = frame.filter(F.col("a") >= i % 7 - 10)
        cycle = []
        cycle.append(cycle)
        cycle.append(column)
        cycle.append(frame)
        del column, frame, cycle
        gc.collect()
        out["v"] = "survived"
    except BaseException as raised:
        out["v"] = type(raised).__name__
worker = threading.Thread(target=work)
worker.start()
worker.join()
results["gc_small"] = out["v"]

print(json.dumps(results))
"""

_TIMEOUT_SECONDS = 1200


def _run_worker(worker: str, *args: str) -> dict[str, object]:
    """Drive one battery once in an isolated interpreter."""
    proc = subprocess.run(
        [sys.executable, "-c", worker, *args],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"the re-verify worker must survive every shape (rc={proc.returncode}): "
        f"{proc.stderr[-2000:]}"
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


@pytest.fixture(scope="module")
def main_results() -> dict[str, object]:
    """Drive the main-thread battery once in an isolated interpreter."""
    return _run_worker(_MAIN_WORKER)


@pytest.fixture(scope="module")
def small256_results() -> dict[str, object]:
    """Drive the 256 KiB-thread battery once in an isolated interpreter."""
    return _run_worker(_SMALL_WORKER, str(256 * 1024))


@pytest.fixture(scope="module")
def small512_results() -> dict[str, object]:
    """Drive the 512 KiB-thread battery once in an isolated interpreter."""
    return _run_worker(_SMALL_WORKER, str(512 * 1024))


@pytest.fixture(scope="module")
def gc_results() -> dict[str, object]:
    """Drive the GC battery once in an isolated interpreter."""
    return _run_worker(_GC_WORKER)


def test_sql_text_or_chains_answer_on_every_door(
    main_results: dict[str, object],
) -> None:
    """2,000-term SQL-text OR answers 50 through F.expr, selectExpr, and filter."""
    assert main_results["fexpr_2000_or"] == 50
    assert main_results["selectexpr_2000_or"] == 50
    assert main_results["stringfilter_2000_or"] == 50


def test_dataframe_built_or_past_cap_refuses_clean(
    main_results: dict[str, object],
) -> None:
    """A 1,500-term DF-built OR refuses AnalysisException, like Spark refuses."""
    assert main_results["or_1500_df"] == "AnalysisException"


def test_mixed_dataframe_and_text_or_answers(
    main_results: dict[str, object],
) -> None:
    """A 5,001-term mixed DF/text OR answers 50; Spark answers it too."""
    assert main_results["mixed_5001"] == 50


def test_deep_view_sql_and_explain_answer(
    main_results: dict[str, object],
) -> None:
    """sql() over a 1,000-deep view counts 50 and explain names Filter."""
    assert main_results["deep_view_count"] == 50
    assert main_results["explain_has_filter"] is True


def test_small_stack_thread_answers_and_collects(
    small256_results: dict[str, object],
) -> None:
    """A 256 KiB thread counts, refuses a 2,000-term OR, reads columns, GCs."""
    assert small256_results["count16"] == 50
    assert small256_results["or2000"] == "AnalysisException"
    assert small256_results["columns"] == 2
    assert small256_results["gc"] == "survived"
    assert "failed" not in small256_results


def test_half_meg_stack_thread_answers_and_collects(
    small512_results: dict[str, object],
) -> None:
    """A 512 KiB thread counts, refuses a 2,000-term OR, reads columns, GCs."""
    assert small512_results["count16"] == 50
    assert small512_results["or2000"] == "AnalysisException"
    assert small512_results["columns"] == 2
    assert small512_results["gc"] == "survived"
    assert "failed" not in small512_results


def test_gc_collects_deep_cycles_on_main_and_small_threads(
    gc_results: dict[str, object],
) -> None:
    """gc.collect() frees deep column/frame cycles on main and 256 KiB threads."""
    assert gc_results["gc_main"] == "survived"
    assert gc_results["gc_small"] == "survived"
