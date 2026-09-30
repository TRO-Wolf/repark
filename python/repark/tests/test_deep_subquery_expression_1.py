"""DEEP-FILTER-CHAIN-CRASH-1 verifier fold — subquery plans and deep expressions.

pins: deep-filter-chain-crash-1/C-005, deep-filter-chain-crash-1/C-006,
deep-filter-chain-crash-1/C-007, deep-filter-chain-crash-1/C-008
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

_WORKER = """
import json
import threading
from functools import reduce

from repark import ReparkSession
from repark.spark import functions as F

results = {}
session = ReparkSession.builder.appName("deep-subq-expr-1").getOrCreate()
base = session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")

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

or_5000 = "SELECT count(*) FROM t WHERE " + " OR ".join(
    f"a = {i}" for i in range(5000)
)
results["or_5000_sql"] = session.sql(or_5000).collect()[0][0]

deep_or = reduce(
    lambda x, y: x | y, [F.col("a") == i for i in range(20000)]
)
try:
    base.filter(deep_or).count()
    results["or_20000_df"] = "answered"
except Exception as raised:
    results["or_20000_df"] = type(raised).__name__

deep_expr = F.col("a")
for _ in range(2000):
    deep_expr = deep_expr + 1
try:
    base.select(deep_expr.alias("x")).count()
    results["arith_2000_select"] = "answered"
except Exception as raised:
    results["arith_2000_select"] = type(raised).__name__

long_query = "SELECT 1" + " " * 1100000
results["sql_long"] = session.sql(long_query).collect()[0][0]

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

print(json.dumps(results))
"""

_TIMEOUT_SECONDS = 1200


@pytest.fixture(scope="module")
def worker_results() -> dict[str, object]:
    """Drive the subquery-expression battery once in an isolated interpreter."""
    proc = subprocess.run(
        [sys.executable, "-c", _WORKER],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"the subquery-expression worker must survive every shape (rc={proc.returncode}): "
        f"{proc.stderr[-2000:]}"
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_thousand_filter_chain_answers_under_subqueries(
    worker_results: dict[str, object],
) -> None:
    """A 1,000-deep chain answers 50 under scalar-subquery and IN shapes."""
    assert worker_results["subquery_1000_scalar"] == 50
    assert worker_results["subquery_1000_in"] == 50


def test_five_thousand_term_or_answers_through_sql(
    worker_results: dict[str, object],
) -> None:
    """A 5,000-term OR answers 50 through `sql()` on the debug build."""
    assert worker_results["or_5000_sql"] == 50


def test_twenty_thousand_term_or_refuses_through_filter(
    worker_results: dict[str, object],
) -> None:
    """A 20,000-term OR raises AnalysisException at `filter()`, like Spark raises."""
    assert worker_results["or_20000_df"] == "AnalysisException"


def test_two_thousand_deep_select_refuses_clean(
    worker_results: dict[str, object],
) -> None:
    """A 2,000-deep `+1` select raises AnalysisException, never a crash."""
    assert worker_results["arith_2000_select"] == "AnalysisException"


def test_long_sql_text_answers_without_a_cap(
    worker_results: dict[str, object],
) -> None:
    """1.1 MB of SQL text answers 1; the query-text length cap is gone."""
    assert worker_results["sql_long"] == 1


def test_shallow_chain_answers_on_small_stack_thread(
    worker_results: dict[str, object],
) -> None:
    """A 16-deep chain counts 50 on a 512 KiB thread via the backstop."""
    assert worker_results["smallstack_16"] == 50
