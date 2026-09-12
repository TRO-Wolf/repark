"""PERF-DESCRIBE-1 — describe()/summary() aggregate the source in one lazy pass.

pins: perf-describe-1/C-002, perf-describe-1/C-003, perf-describe-1/C-005, perf-describe-1/C-007
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812
from repark.spark.dataframe.core import DataFrame


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-perf-describe-1").getOrCreate()
    yield session
    session.stop()


def test_describe_scans_the_source_once(spark: ReparkSession) -> None:
    """describe() returns a frame whose aggregate parent scans the source once.

    The returned frame is a lazy mapInArrow bridge: its ``_map_bridge.parent``
    is the single ``AggregateExec`` plan computing count/avg/stddev/min/max per
    column (one ``TableScan`` / one ``DataSourceExec``, no union, no sort), and
    the bridge's Python unpivot emits the five summary rows in literal order —
    no ordinal column, no ORDER BY.

    pins: perf-describe-1/C-002
    """
    frame = (
        spark.range(1_000)
        .select(
            F.col("id").alias("k"),
            (F.col("id") % F.lit(100)).cast("double").alias("v"),
        )
        .eager()
    )
    described = frame.describe()
    assert described._map_bridge is not None
    explained = described._map_bridge["parent"]._explain_text(extended=True)
    assert explained.count("TableScan") == 1, explained
    assert explained.count("DataSourceExec") == 1, explained
    assert "AggregateExec" in explained
    for forbidden in ("UnionExec", "SortExec", "SortPreservingMergeExec"):
        assert forbidden not in explained, forbidden
    assert "CAST(count" not in explained
    assert explained.count("CAST(min(") == 2 and explained.count("CAST(max(") == 2
    assert described.collect() == [
        ("count", "1000", "1000"),
        ("mean", "499.5", "49.5"),
        ("stddev", "288.8194360957494", "28.880513916550804"),
        ("min", "0", "0.0"),
        ("max", "999", "99.0"),
    ]


def test_describe_runs_nothing_until_an_action(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """describe()/summary() are lazy: the call builds a plan and executes nothing.

    The source frame fails at scan time (a ``CAST('abc' AS INT)`` over real rows);
    a lazy ``describe`` returns a DataFrame and the failure surfaces only when an
    action runs the plan. The stream spy proves no Arrow export happens inside
    the call (DISPLAY-LAZY-1 / R-22 contract).

    pins: perf-describe-1/C-005
    """
    frame = spark.createDataFrame([("abc", 1), ("def", 2)], ["s", "k"])
    bad = frame.withColumn("n", F.col("s").cast("int"))
    materialized: list[str] = []
    real_stream = DataFrame.__arrow_c_stream__
    real_to_arrow = DataFrame.to_arrow

    def recording_stream(self: DataFrame, requested_schema: Any = None) -> Any:
        materialized.append("stream")
        return real_stream(self, requested_schema)

    def recording_to_arrow(self: DataFrame) -> Any:
        materialized.append("to_arrow")
        return real_to_arrow(self)

    monkeypatch.setattr(DataFrame, "__arrow_c_stream__", recording_stream)
    monkeypatch.setattr(DataFrame, "to_arrow", recording_to_arrow)
    described = bad.describe("n")
    assert materialized == []
    assert described.columns == ["summary", "n"]
    with pytest.raises(PySparkException, match="Cannot cast"):
        described.collect()
    assert materialized != []
    monkeypatch.undo()
    materialized.clear()
    summarized = bad.summary("count", "max")
    assert materialized == []
    with pytest.raises(PySparkException, match="Cannot cast"):
        summarized.collect()


def test_summary_duplicate_stats_keep_requested_order(spark: ReparkSession) -> None:
    """A repeated stat name answers one row per occurrence in the requested order.

    pins: perf-describe-1/C-003
    """
    frame = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["k", "v"])
    assert frame.summary("mean", "count", "mean").collect() == [
        ("mean", "1.5", "15.0"),
        ("count", "2", "2"),
        ("mean", "1.5", "15.0"),
    ]
