"""df-surface-b-1 pins: foreach, foreachPartition, observe, Observation."""

from __future__ import annotations

import io
import json
import threading
from collections.abc import Iterator
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, ClassVar

import pytest

import repark.functions as F  # noqa: N812
from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    PySparkAssertionError,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark.column import Column
from repark.spark.dataframe import DataFrame
from repark.spark.observation import Observation
from repark.spark.row import Row

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__).with_name("facade_dataframe_surface_oracle.json").read_text(encoding="utf-8")
)["cells"]


class _RowSink:
    """Record rows passed to foreach."""

    def __init__(self) -> None:
        self.rows: list[Row] = []

    def __call__(self, row: Row) -> None:
        self.rows.append(row)


class _PartitionSink:
    """Record each foreachPartition iterator as a list of ``a`` values."""

    def __init__(self) -> None:
        self.parts: list[list[int]] = []

    def __call__(self, iterator: Iterator[Row]) -> None:
        self.parts.append([row.a for row in iterator])


class _EmptyPartitionSink:
    """Record each foreachPartition iterator as a list of rows."""

    def __init__(self) -> None:
        self.calls: list[list[Row]] = []

    def __call__(self, iterator: Iterator[Row]) -> None:
        self.calls.append(list(iterator))


class _CollectCallLog:
    """Hold the original DataFrame.collect and the call count."""

    original: ClassVar[Any] = None
    calls: ClassVar[int] = 0


def _collect_logged(frame: DataFrame) -> list[Row]:
    """Forward DataFrame.collect while counting calls."""
    _CollectCallLog.calls += 1
    return _CollectCallLog.original(frame)


class _AggCallLog:
    """Hold the original DataFrame.agg and recorded argument tuples."""

    original: ClassVar[Any] = None
    calls: ClassVar[list[tuple[Any, ...]]] = []


def _agg_logged(frame: DataFrame, *exprs: Column | dict[str, str]) -> DataFrame:
    """Forward DataFrame.agg while recording argument tuples."""
    _AggCallLog.calls.append(exprs)
    return _AggCallLog.original(frame, *exprs)


class _BarrierAction:
    """Run ``frame.collect()`` after both threads reach a shared barrier."""

    def __init__(self, frame: DataFrame, barrier: threading.Barrier) -> None:
        self._frame = frame
        self._barrier = barrier
        self.error: Exception | None = None

    def __call__(self) -> None:
        try:
            self._barrier.wait()
            self._frame.collect()
        except Exception as error:
            self.error = error


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A per-test facade session."""
    session = ReparkSession.builder.appName("pytest-df-surface-b-1").getOrCreate()
    yield session
    session.stop()


def _kv_frame(spark: ReparkSession) -> DataFrame:
    """The (key, a, b) frame the oracle cells ran against."""
    return spark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "key string, a int, b int")


def _assert_frame_cell(frame: DataFrame, cell_id: str) -> None:
    """Compare a frame against one recorded DataFrame oracle cell."""
    cell = _ORACLE[cell_id]["result"]
    assert cell["kind"] == "DataFrame"
    assert frame.columns == cell["columns"]
    assert frame.schema.simpleString() == cell["schema"]
    assert [repr(row) for row in frame.collect()] == cell["rows"]


def _assert_error_cell(error: BaseException, cell_id: str) -> None:
    """Compare a raised error against one recorded error oracle cell."""
    cell = _ORACLE[cell_id]["error"]
    assert type(error).__name__ == cell["raises"]
    if cell["condition"] is not None:
        assert error.getCondition() == cell["condition"]
    if cell["params"] is not None:
        assert error.getMessageParameters() == cell["params"]
    recorded_lines = cell["message"].splitlines()
    actual_lines = str(error).splitlines()
    assert actual_lines[0] == recorded_lines[0]
    if len(recorded_lines) == 1:
        assert str(error) == cell["message"]


def _assert_dict_cell(value: object, cell_id: str) -> None:
    """Compare a dict against one recorded dict oracle cell."""
    cell = _ORACLE[cell_id]["result"]
    assert cell["kind"] == "dict"
    expected = {key: item["value"] for key, item in cell["items"].items()}
    assert value == expected


def test_foreach_returns_none_and_visits_each_row(spark: ReparkSession) -> None:
    """foreach calls f once per Row and returns None. pins: df-surface-b-1/C-001"""
    frame = _kv_frame(spark)
    seen = _RowSink()
    result = frame.foreach(seen)
    assert result is None
    assert _ORACLE["foreach_return"]["result"]["kind"] == "NoneType"
    assert _ORACLE["foreach_return"]["result"]["value"] is None
    assert [row.a for row in seen.rows] == [1, 3]
    assert all(isinstance(row, Row) for row in seen.rows)


def test_foreach_does_not_collect_the_whole_frame(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """foreach streams through toLocalIterator, never collect(). pins: df-surface-b-1/C-001"""
    frame = _kv_frame(spark)
    _CollectCallLog.calls = 0
    _CollectCallLog.original = DataFrame.collect
    monkeypatch.setattr(DataFrame, "collect", _collect_logged)
    frame.foreach(_RowSink())
    assert _CollectCallLog.calls == 0


def test_foreach_rejects_non_callable(spark: ReparkSession) -> None:
    """Non-callable f raises NOT_CALLABLE at the call. pins: df-surface-b-1/C-001"""
    frame = _kv_frame(spark)
    with pytest.raises(PySparkTypeError) as raised:
        frame.foreach(5)
    assert raised.value.getCondition() == "NOT_CALLABLE"
    assert raised.value.getMessageParameters() == {"arg_name": "f", "arg_type": "int"}
    assert str(raised.value).startswith("[NOT_CALLABLE]")


def test_foreach_propagates_user_exception(spark: ReparkSession) -> None:
    """An exception raised by f propagates unchanged. pins: df-surface-b-1/C-001"""
    frame = _kv_frame(spark)
    with pytest.raises(ZeroDivisionError):
        frame.foreach(_raise_zero_division)


def test_foreach_partition_returns_none_and_visits_batches(spark: ReparkSession) -> None:
    """foreachPartition calls f once per batch with a Row iterator. pins: df-surface-b-1/C-002"""
    frame = _kv_frame(spark)
    sink = _PartitionSink()
    result = frame.foreachPartition(sink)
    assert result is None
    assert _ORACLE["foreachPartition_return"]["result"]["kind"] == "NoneType"
    assert _ORACLE["foreachPartition_return"]["result"]["value"] is None
    assert sum(len(part) for part in sink.parts) == 2
    assert [value for part in sink.parts for value in part] == [1, 3]


def test_foreach_partition_empty_frame_calls_once(spark: ReparkSession) -> None:
    """An empty frame calls f(iter([])) at least once. pins: df-surface-b-1/C-002"""
    frame = spark.createDataFrame([], "key string, a int, b int")
    sink = _EmptyPartitionSink()
    frame.foreachPartition(sink)
    assert len(sink.calls) >= 1
    assert all(part == [] for part in sink.calls)


def test_foreach_partition_rejects_non_callable(spark: ReparkSession) -> None:
    """Non-callable f raises NOT_CALLABLE at the call. pins: df-surface-b-1/C-002"""
    frame = _kv_frame(spark)
    with pytest.raises(PySparkTypeError) as raised:
        frame.foreachPartition(5)
    assert raised.value.getCondition() == "NOT_CALLABLE"
    assert raised.value.getMessageParameters() == {"arg_name": "f", "arg_type": "int"}


def test_observation_empty_name_raises(spark: ReparkSession) -> None:
    """Observation('') raises VALUE_NOT_NON_EMPTY_STR. pins: df-surface-b-1/C-003"""
    _ = spark
    with pytest.raises(PySparkValueError) as raised:
        Observation("")
    _assert_error_cell(raised.value, "observation_empty_name")


def test_observation_non_str_name_raises(spark: ReparkSession) -> None:
    """A non-str Observation name raises NOT_STR. pins: df-surface-b-1/C-003"""
    _ = spark
    with pytest.raises(PySparkTypeError) as raised:
        Observation(1)
    assert raised.value.getCondition() == "NOT_STR"
    assert raised.value.getMessageParameters() == {"arg_name": "name", "arg_type": "int"}


def test_observation_generated_name_constructs(spark: ReparkSession) -> None:
    """Observation() constructs with a generated name. pins: df-surface-b-1/C-003"""
    _ = spark
    first = Observation()
    second = Observation()
    assert type(first).__name__ == "Observation"
    assert "Observation" in repr(first)
    assert first is not second


def test_observe_no_exprs_raises(spark: ReparkSession) -> None:
    """observe with no exprs raises CANNOT_BE_EMPTY. pins: df-surface-b-1/C-003"""
    frame = _kv_frame(spark)
    with pytest.raises(PySparkValueError) as raised:
        frame.observe("m3")
    _assert_error_cell(raised.value, "observe_no_exprs")


def test_observe_reuse_raises(spark: ReparkSession) -> None:
    """An Observation used twice raises REUSE_OBSERVATION. pins: df-surface-b-1/C-003"""
    frame = _kv_frame(spark)
    observation = Observation("reuse")
    frame.observe(observation, F.count(F.lit(1)).alias("c"))
    with pytest.raises(PySparkAssertionError) as raised:
        frame.observe(observation, F.count(F.lit(1)).alias("c"))
    _assert_error_cell(raised.value, "observe_obs_reuse_error")


def test_observe_bad_first_arg_raises_not_list_of_column(spark: ReparkSession) -> None:
    """A non-Observation non-str first arg raises NOT_LIST_OF_COLUMN. pins: df-surface-b-1/C-003"""
    frame = _kv_frame(spark)
    with pytest.raises(PySparkTypeError) as raised:
        frame.observe(5, F.count(F.lit(1)).alias("c"))
    assert raised.value.getCondition() == "NOT_LIST_OF_COLUMN"
    assert raised.value.getMessageParameters() == {
        "arg_name": "observation",
        "arg_type": "int",
    }


def test_observe_non_agg_raises_at_first_action(spark: ReparkSession) -> None:
    """A non-aggregate expr raises at the first action. pins: df-surface-b-1/C-003"""
    frame = _kv_frame(spark)
    observed = frame.observe("m4", F.col("a"))
    with pytest.raises(AnalysisException) as raised:
        observed.collect()
    error = raised.value
    assert error.getCondition() == "INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE"
    assert error.getMessageParameters() == {"expr": '"a"'}
    recorded = _ORACLE["observe_non_agg"]["error"]
    assert str(error).splitlines()[0] == recorded["message"].splitlines()[0]


def test_observe_name_keeps_rows_and_schema(spark: ReparkSession) -> None:
    """observe(str, ...) returns the same rows and schema. pins: df-surface-b-1/C-004"""
    frame = _kv_frame(spark)
    observed = frame.observe("m", F.count(F.lit(1)).alias("c"))
    _assert_frame_cell(observed, "observe_name")


def test_observe_unaliased_keeps_rows(spark: ReparkSession) -> None:
    """Unaliased observe keeps the source rows. pins: df-surface-b-1/C-004"""
    frame = _kv_frame(spark)
    rows = frame.observe("m5", F.count(F.lit(1))).collect()
    cell = _ORACLE["observe_unaliased"]["result"]
    assert [repr(row) for row in rows] == [item["repr"] for item in cell["items"]]


def test_observe_obs_get_after_action(spark: ReparkSession) -> None:
    """obs.get after the first action is the metric dict. pins: df-surface-b-1/C-004"""
    frame = _kv_frame(spark)
    observation = Observation("m")
    observed = frame.observe(observation, F.count(F.lit(1)).alias("c"), F.sum("a").alias("s"))
    observed.collect()
    _assert_dict_cell(observation.get, "observe_obs")


def test_observe_obs_get_twice_is_stable(spark: ReparkSession) -> None:
    """A second get after one action does not change the dict. pins: df-surface-b-1/C-004"""
    frame = _kv_frame(spark)
    observation = Observation("twice")
    observed = frame.observe(observation, F.count(F.lit(1)).alias("c"))
    observed.collect()
    first = observation.get
    second = observation.get
    _assert_dict_cell(second, "observe_obs_twice_get")
    assert first == second


def test_observe_agg_runs_once_across_actions(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One extra agg on the first action, not per action. pins: df-surface-b-1/C-004"""
    frame = _kv_frame(spark)
    observation = Observation("once")
    observed = frame.observe(observation, F.count(F.lit(1)).alias("c"))
    _AggCallLog.calls = []
    _AggCallLog.original = DataFrame.agg
    monkeypatch.setattr(DataFrame, "agg", _agg_logged)
    observed.collect()
    observed.count()
    assert len(_AggCallLog.calls) == 1
    assert observation.get == {"c": 2}


def test_observe_get_before_attach_raises(spark: ReparkSession) -> None:
    """get before observe raises NO_OBSERVE_BEFORE_GET. pins: df-surface-b-1/C-004"""
    _ = spark
    observation = Observation("never")
    with pytest.raises(PySparkAssertionError) as raised:
        _ = observation.get
    assert raised.value.getCondition() == "NO_OBSERVE_BEFORE_GET"
    assert str(raised.value) == (
        "[NO_OBSERVE_BEFORE_GET] Should observe by calling `DataFrame.observe` before `get`."
    )


def test_observe_get_before_action_raises(spark: ReparkSession) -> None:
    """get after observe with no action raises NO_OBSERVE_BEFORE_GET. pins: df-surface-b-1/C-004"""
    frame = _kv_frame(spark)
    observation = Observation("m2")
    frame.observe(observation, F.count(F.lit(1)).alias("c"))
    with pytest.raises(PySparkAssertionError) as raised:
        _ = observation.get
    assert raised.value.getCondition() == "NO_OBSERVE_BEFORE_GET"


def test_observe_unaliased_metric_name_follows_display(spark: ReparkSession) -> None:
    """Unaliased metric names follow the column display name. pins: df-surface-b-1/C-004"""
    frame = _kv_frame(spark)
    observation = Observation("display")
    observed = frame.observe(observation, F.count(F.lit(1)))
    observed.collect()
    assert observation.get == {"count(1)": 2}


def test_observation_is_exported_from_spark(spark: ReparkSession) -> None:
    """Observation is the spark and spark.sql export. pins: df-surface-b-1/C-005"""
    _ = spark
    import repark.spark as spark_pkg
    import repark.spark.sql as sql_pkg

    assert spark_pkg.Observation is Observation
    assert sql_pkg.Observation is Observation


def _observed_two_metric(spark: ReparkSession, name: str) -> tuple[Observation, DataFrame]:
    """Attach count/sum metrics named by the caller to the two-row oracle frame."""
    observation = Observation(name)
    observed = _kv_frame(spark).observe(
        observation, F.count(F.lit(1)).alias("c"), F.sum("a").alias("s")
    )
    return observation, observed


def test_observe_take_head_first_isempty_fill_full_metrics(
    spark: ReparkSession,
) -> None:
    """L-001: peek actions fill full observed metrics once. pins: df-surface-b-1/C-007"""
    peek_actions = [
        lambda observed: observed.take(1),
        lambda observed: observed.take(0),
        lambda observed: observed.head(),
        lambda observed: observed.first(),
        lambda observed: observed.isEmpty(),
    ]
    for action in peek_actions:
        observation, observed = _observed_two_metric(spark, "peek")
        action(observed)
        assert observation.get == {"c": 2, "s": 4}
        observed.collect()
        assert observation.get == {"c": 2, "s": 4}


def test_observe_show_fills_full_metrics_both_styles(spark: ReparkSession) -> None:
    """L-001: show(1) under both display styles fills full metrics. pins: df-surface-b-1/C-007"""
    for style in ("spark", "polars"):
        spark.display_style = style
        observation, observed = _observed_two_metric(spark, "show")
        with redirect_stdout(io.StringIO()):
            observed.show(1)
        assert observation.get == {"c": 2, "s": 4}
        observed.collect()
        assert observation.get == {"c": 2, "s": 4}
    spark.display_style = "spark"


def test_observe_filter_descendant_fills_observed_metrics(spark: ReparkSession) -> None:
    """L-002: a filter descendant aggregates the observed frame. pins: df-surface-b-1/C-007"""
    observation, observed = _observed_two_metric(spark, "filt")
    rows = observed.filter(F.col("a") > 1).collect()
    assert [row.a for row in rows] == [3]
    assert observation.get == {"c": 2, "s": 4}
    observed.collect()
    assert observation.get == {"c": 2, "s": 4}


def test_observe_union_descendant_fills_observed_metrics(spark: ReparkSession) -> None:
    """L-002: a union descendant aggregates the observed side only. pins: df-surface-b-1/C-007"""
    other = spark.createDataFrame([("z", 9, 9)], "key string, a int, b int")
    observation = Observation("uni")
    observed = _kv_frame(spark).observe(observation, F.count(F.lit(1)).alias("c"))
    rows = observed.union(other).collect()
    assert len(rows) == 3
    assert observation.get == {"c": 2}


def test_observe_limit_descendant_fills_observed_metrics(spark: ReparkSession) -> None:
    """L-002: a limit descendant aggregates the observed frame. pins: df-surface-b-1/C-007"""
    observation, observed = _observed_two_metric(spark, "lim")
    rows = observed.limit(1).collect()
    assert len(rows) == 1
    assert observation.get == {"c": 2, "s": 4}


def test_observe_select_descendant_fills_observed_metrics(spark: ReparkSession) -> None:
    """L-002: select fills metrics on projected-out columns. pins: df-surface-b-1/C-007"""
    observation = Observation("sel")
    observed = _kv_frame(spark).observe(
        observation, F.sum("b").alias("s"), F.max("key").alias("mk")
    )
    rows = observed.select("a").collect()
    assert [repr(row) for row in rows] == ["Row(a=1)", "Row(a=3)"]
    assert observation.get == {"s": 6, "mk": "y"}


def test_observe_drop_descendant_fills_observed_metrics(spark: ReparkSession) -> None:
    """L-002: a drop descendant fills a metric on the dropped column. pins: df-surface-b-1/C-007"""
    observation = Observation("drp")
    observed = _kv_frame(spark).observe(observation, F.sum("a").alias("s"))
    rows = observed.drop("a").collect()
    assert [repr(row) for row in rows] == ["Row(key='x', b=2)", "Row(key='y', b=4)"]
    assert observation.get == {"s": 4}


def test_observe_map_in_arrow_take_fills(spark: ReparkSession) -> None:
    """L-003: a mapInArrow peek action fills the Observation. pins: df-surface-b-1/C-007"""
    frame = _kv_frame(spark)
    mapped = frame.mapInArrow(lambda batch: batch, frame.schema)
    observation = Observation("mia")
    observed = mapped.observe(observation, F.count(F.lit(1)).alias("c"))
    rows = observed.take(1)
    assert [repr(row) for row in rows] == ["Row(key='x', a=1, b=2)"]
    assert observation.get == {"c": 2}


def test_observe_concurrent_fills_are_independent(spark: ReparkSession) -> None:
    """L-004: two threads fill two Observations on sibling observes. pins: df-surface-b-1/C-007"""
    for _ in range(20):
        frame = _kv_frame(spark)
        observation_a = Observation("a")
        observation_b = Observation("b")
        observed_a = frame.observe(observation_a, F.sum("a").alias("s"))
        observed_b = frame.observe(observation_b, F.sum("a").alias("s"))
        barrier = threading.Barrier(2)
        action_a = _BarrierAction(observed_a, barrier)
        action_b = _BarrierAction(observed_b, barrier)
        threads = [threading.Thread(target=action_a), threading.Thread(target=action_b)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert action_a.error is None and action_b.error is None
        assert observation_a.get == {"s": 4}
        assert observation_b.get == {"s": 4}


def test_observe_literal_metric_fills(spark: ReparkSession) -> None:
    """L-005: literal metrics fill their literal value. pins: df-surface-b-1/C-007"""
    observation = Observation("lit")
    observed = _kv_frame(spark).observe(observation, F.lit(42).alias("k"))
    observed.collect()
    assert observation.get == {"k": 42}
    unaliased = Observation("lit2")
    _kv_frame(spark).observe(unaliased, F.lit(7)).collect()
    assert unaliased.get == {"7": 7}


def test_observe_non_column_exprs_raise_at_observe(spark: ReparkSession) -> None:
    """L-006: non-Column exprs raise NOT_LIST_OF_COLUMN at observe. pins: df-surface-b-1/C-007"""
    frame = _kv_frame(spark)
    with pytest.raises(PySparkTypeError) as raised:
        frame.observe(Observation("s"), "count(1)")
    assert raised.value.getCondition() == "NOT_LIST_OF_COLUMN"
    assert raised.value.getMessageParameters() == {"arg_name": "exprs"}
    with pytest.raises(PySparkTypeError):
        frame.observe(Observation("s2"), F.count(F.lit(1)).alias("c"), "sum(a)")


def test_observe_free_attribute_outside_aggregate_refuses(spark: ReparkSession) -> None:
    """L-006: a bare attribute in a metric refuses at first action. pins: df-surface-b-1/C-007"""
    observed = _kv_frame(spark).observe(Observation("mix"), F.col("a") + F.sum("b").alias("s"))
    with pytest.raises(AnalysisException) as raised:
        observed.collect()
    assert (
        raised.value.getCondition()
        == "INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE"
    )


def test_observe_agg_runs_once_across_action_sequences(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """L-007: one extra agg across mixed actions and descendants. pins: df-surface-b-1/C-007"""
    observation = Observation("once")
    observed = _kv_frame(spark).observe(observation, F.count(F.lit(1)).alias("c"))
    _AggCallLog.calls = []
    _AggCallLog.original = DataFrame.agg
    monkeypatch.setattr(DataFrame, "agg", _agg_logged)
    observed.take(1)
    observed.head()
    with redirect_stdout(io.StringIO()):
        observed.show(1)
    observed.count()
    observed.collect()
    observed.toPandas()
    assert len(_AggCallLog.calls) == 1
    assert observation.get == {"c": 2}
    descendant = observed.filter(F.col("a") > 0)
    descendant.take(1)
    descendant.collect()
    assert len(_AggCallLog.calls) == 1


def _raise_zero_division(row: Row) -> None:
    """Raise ZeroDivisionError for the foreach propagation pin."""
    del row
    raise ZeroDivisionError
