"""DEEP-FILTER-CHAIN-CRASH-1 — deep operator chains answer instead of crashing.

pins: deep-filter-chain-crash-1/C-001, deep-filter-chain-crash-1/C-002,
deep-filter-chain-crash-1/C-003, deep-filter-chain-crash-1/C-004,
deep-filter-chain-crash-1/C-009, deep-filter-chain-crash-1/C-010,
deep-filter-chain-crash-1/C-011
"""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

_WORKER = """
import json
import sys
from functools import reduce

from repark import ReparkSession
from repark.spark import functions as F

results = {}
session = ReparkSession.builder.appName("deep-chain-1").getOrCreate()
base = session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")

chained = base
for i in range(1000):
    chained = chained.filter(F.col("a") >= i % 7 - 10)
results["filter_1000_df"] = chained.count()

joined = base
other = session.createDataFrame([(0, 0)], "a INT, b INT")
for _ in range(200):
    joined = joined.join(other, "a")
results["join_200_df"] = joined.count()

unioned = base
for _ in range(300):
    unioned = unioned.union(base)
results["union_300_df"] = unioned.count()

widened = base
for i in range(120):
    widened = widened.withColumn(f"c{i}", F.col("a") + i)
results["with_column_120_df"] = widened.count()

nested = "SELECT * FROM (VALUES (1, 2)) AS t(a, b)"
for i in range(1000):
    nested = f"SELECT * FROM ({nested}) WHERE a >= {i % 7 - 10}"
try:
    session.sql(nested).count()
    results["filter_1000_sql"] = "answered"
except Exception as raised:
    results["filter_1000_sql"] = type(raised).__name__

flat = "SELECT * FROM (VALUES (1, 2)) AS t(a, b)"
for _ in range(600):
    flat = f"SELECT * FROM (VALUES (1, 2)) AS t(a, b) UNION ALL {flat}"
results["union_600_sql"] = session.sql(flat).count()

in_text = "SELECT count(*) FROM range(1000) WHERE id IN (" + ",".join(
    str(i) for i in range(200000)
) + ")"
results["in_200k_range"] = session.sql(in_text).count()

unioned8193 = base
for _ in range(8193):
    unioned8193 = unioned8193.union(base)
results["union_8193_df"] = unioned8193.count()

chained8192 = base
for i in range(8192):
    chained8192 = chained8192.filter(F.col("a") >= i % 7 - 10)
try:
    chained8192.count()
    results["filter_8192_df"] = "answered"
except Exception as raised:
    results["filter_8192_df"] = type(raised).__name__

and_300 = reduce(lambda x, y: x & y, [F.col("a") >= -i for i in range(300)])
results["and_300_df"] = base.filter(and_300).count()

and_1500 = reduce(lambda x, y: x & y, [F.col("a") >= -i for i in range(1500)])
try:
    base.filter(and_1500).count()
    results["and_1500_df"] = "answered"
except Exception as raised:
    results["and_1500_df"] = type(raised).__name__

print(json.dumps(results))
"""

_TIMEOUT_SECONDS = 1500


@pytest.fixture(scope="module")
def worker_results() -> dict[str, object]:
    """Drive the deep-chain battery once in an isolated interpreter."""
    proc = subprocess.run(
        [sys.executable, "-c", _WORKER],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_SECONDS,
    )
    assert proc.returncode == 0, (
        f"the deep-chain worker must survive every chain (rc={proc.returncode}): "
        f"{proc.stderr[-2000:]}"
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_thousand_filter_chain_answers_on_dataframe_door(
    worker_results: dict[str, object],
) -> None:
    """A 1,000-deep filter chain counts like Spark instead of killing the process."""
    assert worker_results["filter_1000_df"] == 50


def test_deep_join_and_union_chains_answer_on_dataframe_door(
    worker_results: dict[str, object],
) -> None:
    """200 joins and 300 unions count exactly; neither crashes the interpreter."""
    assert worker_results["join_200_df"] == 1
    assert worker_results["union_300_df"] == 15050


def test_wide_with_column_chain_answers_within_ceiling(
    worker_results: dict[str, object],
) -> None:
    """120 chained ``withColumn`` calls answer; plan-build time caps the depth."""
    assert worker_results["with_column_120_df"] == 50


def test_deep_sql_shapes_refuse_clean_or_answer(
    worker_results: dict[str, object],
) -> None:
    """1,000-deep nested SQL raises a catchable exception; flat 600-union SQL counts."""
    assert worker_results["filter_1000_sql"] == "RecursionError"
    assert worker_results["union_600_sql"] == 601


def test_two_hundred_thousand_item_in_list_answers(
    worker_results: dict[str, object],
) -> None:
    """A 1.3 MB 200,000-item IN list counts 1000; no query-text length cap remains."""
    assert worker_results["in_200k_range"] == 1000


def test_union_past_plan_cap_answers(
    worker_results: dict[str, object],
) -> None:
    """8,193 unions count 409700; union spines do not count toward the plan cap."""
    assert worker_results["union_8193_df"] == 409700


def test_filter_past_plan_cap_refuses_clean(
    worker_results: dict[str, object],
) -> None:
    """8,192 filters refuse AnalysisException; base crashes and Spark refuses there."""
    assert worker_results["filter_8192_df"] == "AnalysisException"


def test_and_chain_at_must_answer_depth_answers(
    worker_results: dict[str, object],
) -> None:
    """A 300-term AND answers 50; Spark answers 300 and refuses 350."""
    assert worker_results["and_300_df"] == 50


def test_and_chain_past_expression_cap_refuses_clean(
    worker_results: dict[str, object],
) -> None:
    """A 1,500-term AND refuses AnalysisException at filter(), like Spark refuses."""
    assert worker_results["and_1500_df"] == "AnalysisException"
