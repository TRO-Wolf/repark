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

pytestmark = pytest.mark.slow

_PREAMBLE = """
import json
import sys
from functools import reduce

from repark import ReparkSession
from repark.spark import functions as F

results = {}
session = ReparkSession.builder.appName("deep-chain-1").getOrCreate()
base = session.createDataFrame([(i, i * 2) for i in range(50)], "a INT, b INT")
"""

_EPILOGUE = """
print(json.dumps(results))
"""

_FILTER_1000_SHAPE = """
chained = base
for i in range(1000):
    chained = chained.filter(F.col("a") >= i % 7 - 10)
results["filter_1000_df"] = chained.count()
"""

_JOIN_UNION_SHAPE = """
joined = base
other = session.createDataFrame([(0, 0)], "a INT, b INT")
for _ in range(200):
    joined = joined.join(other, "a")
results["join_200_df"] = joined.count()

unioned = base
for _ in range(300):
    unioned = unioned.union(base)
results["union_300_df"] = unioned.count()
"""

_WITH_COLUMN_SHAPE = """
widened = base
for i in range(120):
    widened = widened.withColumn(f"c{i}", F.col("a") + i)
results["with_column_120_df"] = widened.count()
"""

_SQL_SHAPES = """
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
"""

_IN_200K_SHAPE = """
in_text = "SELECT count(*) FROM range(1000) WHERE id IN (" + ",".join(
    str(i) for i in range(200000)
) + ")"
results["in_200k_range"] = session.sql(in_text).collect()[0][0]
"""

_UNION_8193_SHAPE = """
unioned8193 = base
for _ in range(8193):
    unioned8193 = unioned8193.union(base)
results["union_8193_df"] = unioned8193.count()
"""

_FILTER_8192_SHAPE = """
chained8192 = base
for i in range(8192):
    chained8192 = chained8192.filter(F.col("a") >= i % 7 - 10)
try:
    chained8192.count()
    results["filter_8192_df"] = "answered"
except Exception as raised:
    results["filter_8192_df"] = type(raised).__name__
"""

_AND_300_SHAPE = """
and_300 = reduce(lambda x, y: x & y, [F.col("a") >= -i for i in range(300)])
results["and_300_df"] = base.filter(and_300).count()
"""

_AND_1500_SHAPE = """
and_1500 = reduce(lambda x, y: x & y, [F.col("a") >= -i for i in range(1500)])
try:
    base.filter(and_1500).count()
    results["and_1500_df"] = "answered"
except Exception as raised:
    results["and_1500_df"] = type(raised).__name__
"""

_TIMEOUT_SECONDS = 1500


def _run_shape(shape: str) -> dict[str, object]:
    """Drive one shape of the deep-chain battery in its own isolated interpreter.

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
        f"the deep-chain worker must survive every chain (rc={proc.returncode}): "
        f"{proc.stderr[-2000:]}"
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_thousand_filter_chain_answers_on_dataframe_door() -> None:
    """A 1,000-deep filter chain counts like Spark instead of killing the process."""
    worker_results = _run_shape(_FILTER_1000_SHAPE)
    assert worker_results["filter_1000_df"] == 50


def test_deep_join_and_union_chains_answer_on_dataframe_door() -> None:
    """200 joins and 300 unions count exactly; neither crashes the interpreter."""
    worker_results = _run_shape(_JOIN_UNION_SHAPE)
    assert worker_results["join_200_df"] == 1
    assert worker_results["union_300_df"] == 15050


def test_wide_with_column_chain_answers_within_ceiling() -> None:
    """120 chained ``withColumn`` calls answer; plan-build time caps the depth."""
    worker_results = _run_shape(_WITH_COLUMN_SHAPE)
    assert worker_results["with_column_120_df"] == 50


def test_deep_sql_shapes_refuse_clean_or_answer() -> None:
    """1,000-deep nested SQL raises a catchable exception; flat 600-union SQL counts."""
    worker_results = _run_shape(_SQL_SHAPES)
    assert worker_results["filter_1000_sql"] == "RecursionError"
    assert worker_results["union_600_sql"] == 601


def test_two_hundred_thousand_item_in_list_answers() -> None:
    """A 1.3 MB 200,000-item IN list counts 1000; no query-text length cap remains."""
    worker_results = _run_shape(_IN_200K_SHAPE)
    assert worker_results["in_200k_range"] == 1000


def test_union_past_plan_cap_answers() -> None:
    """8,193 unions count 409700; union spines do not count toward the plan cap."""
    worker_results = _run_shape(_UNION_8193_SHAPE)
    assert worker_results["union_8193_df"] == 409700


def test_filter_past_plan_cap_refuses_clean() -> None:
    """8,192 filters refuse AnalysisException; base crashes and Spark refuses there."""
    worker_results = _run_shape(_FILTER_8192_SHAPE)
    assert worker_results["filter_8192_df"] == "AnalysisException"


def test_and_chain_at_must_answer_depth_answers() -> None:
    """A 300-term AND answers 50; Spark answers 300 and refuses 350."""
    worker_results = _run_shape(_AND_300_SHAPE)
    assert worker_results["and_300_df"] == 50


def test_and_chain_past_expression_cap_refuses_clean() -> None:
    """A 1,500-term AND refuses AnalysisException at filter(), like Spark refuses."""
    worker_results = _run_shape(_AND_1500_SHAPE)
    assert worker_results["and_1500_df"] == "AnalysisException"
