"""PERF-DESCRIBE-1 — describe()/summary() aggregates the source in one pass.

pins: perf-describe-1/C-001, perf-describe-1/C-002, perf-describe-1/C-003
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark.spark.dataframe.core import DataFrame


class _RecordingSession:
    """Session proxy recording ``sql`` / temp-view calls; drops are swallowed so EXPLAIN can
    still resolve the scratch view after ``describe`` returns. PyO3 methods are read-only on
    the native handle, so the spy wraps it (same pattern as the cache-materialize pin)."""

    def __init__(self, real: Any) -> None:
        """Wrap the native session handle with empty call logs."""
        self._real = real
        self.queries: list[str] = []
        self.views: list[str] = []
        self.drops: list[str] = []

    def sql(self, query: str) -> Any:
        """Record the query text and forward to the real session."""
        self.queries.append(query)
        return self._real.sql(query)

    def create_or_replace_temp_view(self, name: str, plan: Any) -> Any:
        """Record the registration and forward to the real session."""
        self.views.append(name)
        return self._real.create_or_replace_temp_view(name, plan)

    def drop_temp_view(self, name: str) -> None:
        """Record the drop without executing it (the plan texts need the view live)."""
        self.drops.append(name)

    def __getattr__(self, name: str) -> Any:
        """Delegate every other attribute to the real session."""
        return getattr(self._real, name)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-perf-describe-1").getOrCreate()
    yield session
    session.stop()


def _plans(spark: ReparkSession, query: str) -> tuple[str, str]:
    """The (logical, physical) plan text of one captured query."""
    rows = spark.sql(f"EXPLAIN {query}").collect()
    logical = "\n".join(row["plan"] for row in rows if row["plan_type"] == "logical_plan")
    physical = "\n".join(row["plan"] for row in rows if row["plan_type"] == "physical_plan")
    return logical, physical


def test_describe_scans_the_source_once(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """describe() issues exactly one source scan and the result frame sorts nothing.

    One ``AggregateExec`` computes every requested statistic; the five summary rows are
    literals, so the returned frame carries no UNION, no sort, and no scan of the source.
    The aggregate is a native plan (not session SQL), so the spy wraps ``to_arrow`` —
    every frame materialized while ``describe`` runs is explained.

    pins: perf-describe-1/C-001, perf-describe-1/C-002
    """
    frame = (
        spark.range(1_000)
        .select(
            F.col("id").alias("k"),
            (F.col("id") % F.lit(100)).cast("double").alias("v"),
        )
        .eager()
    )
    materialized_plans: list[str] = []
    real_to_arrow = DataFrame.to_arrow

    def recording_to_arrow(self: DataFrame) -> Any:
        materialized_plans.append(self._explain_text(extended=True))
        return real_to_arrow(self)

    real_session = frame._session
    recorder = _RecordingSession(real_session)
    frame._session = recorder
    monkeypatch.setattr(DataFrame, "to_arrow", recording_to_arrow)
    try:
        described = frame.describe()
    finally:
        monkeypatch.undo()
    assert described.collect() == [
        ("count", "1000", "1000"),
        ("mean", "499.5", "49.5"),
        ("stddev", "288.8194360957494", "28.880513916550804"),
        ("min", "0", "0.0"),
        ("max", "999", "99.0"),
    ]
    try:
        agg_plans = [plan for plan in materialized_plans if "AggregateExec" in plan]
        assert len(agg_plans) == 1, (
            f"expected one aggregate plan; session queries: {recorder.queries}"
        )
        agg_plan = agg_plans[0]
        assert agg_plan.count("TableScan") == 1, agg_plan
        assert agg_plan.count("DataSourceExec") == 1, agg_plan
        assert "UnionExec" not in agg_plan
        for query in recorder.queries:
            if query.lstrip().upper().startswith("EXPLAIN"):
                continue
            logical, _physical = _plans(spark, query)
            assert "TableScan" not in logical, logical
        explained = described._explain_text(extended=True)
        assert "Values:" in explained
        assert "TableScan" not in explained
        for forbidden in ("UnionExec", "SortExec", "SortPreservingMergeExec"):
            assert forbidden not in explained, explained
    finally:
        frame._session = real_session
        described._session = real_session
        for view in recorder.views:
            real_session.drop_temp_view(view)


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
