"""DEEP-FILTER-CHAIN-CRASH-1 verifier fold — subquery plans and deep expressions.

pins: deep-filter-chain-crash-1/C-005, deep-filter-chain-crash-1/C-006,
deep-filter-chain-crash-1/C-007, deep-filter-chain-crash-1/C-008
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
session = ReparkSession.builder.appName("deep-subq-expr-1").getOrCreate()
base = session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")
"""

_EPILOGUE = """
print(json.dumps(results))
"""

_SUBQUERY_SHAPE = """
chained = base
for i in range(1000):
    chained = chained.filter(F.col("a") >= i % 7 - 10)
results["subquery_1000_scalar"] = (
    base.limit(1).select(chained.agg(F.count("*")).scalar().alias("c")).collect()[0][0]
)
chained.createOrReplaceTempView("deepv")
base.createOrReplaceTempView("t")
results["subquery_1000_in"] = session.sql(
    "SELECT count(*) FROM t WHERE a IN (SELECT a FROM deepv)"
).collect()[0][0]
"""

_OR_5000_SQL_SHAPE = """
base.createOrReplaceTempView("t")
or_5000 = "SELECT count(*) FROM t WHERE " + " OR ".join(
    f"a = {i}" for i in range(5000)
)
results["or_5000_sql"] = session.sql(or_5000).collect()[0][0]
"""

_OR_20000_DF_SHAPE = """
deep_or = reduce(
    lambda x, y: x | y, [F.col("a") == i for i in range(20000)]
)
try:
    base.filter(deep_or).count()
    results["or_20000_df"] = "answered"
except Exception as raised:
    results["or_20000_df"] = type(raised).__name__
"""

_ARITH_2000_SHAPE = """
deep_expr = F.col("a")
for _ in range(2000):
    deep_expr = deep_expr + 1
try:
    base.select(deep_expr.alias("x")).count()
    results["arith_2000_select"] = "answered"
except Exception as raised:
    results["arith_2000_select"] = type(raised).__name__
"""

_SQL_LONG_SHAPE = """
long_query = "SELECT 1" + " " * 1100000
results["sql_long"] = session.sql(long_query).collect()[0][0]
"""

_SMALL_STACK_SHAPE = """
threading.stack_size(512 * 1024)
shallow = base
for i in range(16):
    shallow = shallow.filter(F.col("a") >= i % 7 - 10)
small_out = {}
def small_work():
    try:
        small_out["v"] = shallow.count()
    except Exception as raised:
        small_out["v"] = type(raised).__name__
worker = threading.Thread(target=small_work)
worker.start()
worker.join()
results["smallstack_16"] = small_out["v"]
"""

_TIMEOUT_SECONDS = 1200


def _run_shape(shape: str) -> dict[str, object]:
    """Drive one shape of the subquery-expression battery in its own isolated interpreter.

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
        f"the subquery-expression worker must survive every shape (rc={proc.returncode}): "
        f"{proc.stderr[-2000:]}"
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_thousand_filter_chain_answers_under_subqueries() -> None:
    """A 1,000-deep chain answers 50 under scalar-subquery and IN shapes."""
    worker_results = _run_shape(_SUBQUERY_SHAPE)
    assert worker_results["subquery_1000_scalar"] == 50
    assert worker_results["subquery_1000_in"] == 50


def test_five_thousand_term_or_answers_through_sql() -> None:
    """A 5,000-term OR answers 50 through `sql()` on the debug build."""
    worker_results = _run_shape(_OR_5000_SQL_SHAPE)
    assert worker_results["or_5000_sql"] == 50


@pytest.mark.xdist_group("test_deep_subquery_expression_1")
def test_twenty_thousand_term_or_refuses_through_filter() -> None:
    """A 20,000-term OR raises AnalysisException at `filter()`, like Spark raises."""
    worker_results = _run_shape(_OR_20000_DF_SHAPE)
    assert worker_results["or_20000_df"] == "AnalysisException"


@pytest.mark.xdist_group("test_deep_subquery_expression_1")
def test_two_thousand_deep_select_refuses_clean() -> None:
    """A 2,000-deep `+1` select raises AnalysisException, never a crash."""
    worker_results = _run_shape(_ARITH_2000_SHAPE)
    assert worker_results["arith_2000_select"] == "AnalysisException"


def test_long_sql_text_answers_without_a_cap() -> None:
    """1.1 MB of SQL text answers 1; the query-text length cap is gone."""
    worker_results = _run_shape(_SQL_LONG_SHAPE)
    assert worker_results["sql_long"] == 1


def test_shallow_chain_answers_on_small_stack_thread() -> None:
    """A 16-deep chain counts 50 on a 512 KiB thread via the backstop."""
    worker_results = _run_shape(_SMALL_STACK_SHAPE)
    assert worker_results["smallstack_16"] == 50
