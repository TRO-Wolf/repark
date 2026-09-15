"""df-stream-batch-1 pins: the streaming-named DataFrame surface on a batch frame."""

from __future__ import annotations

import datetime
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkTypeError,
)
from repark.spark.dataframe import DataFrame

_ORACLE: dict[str, Any] = json.loads(
    Path(__file__)
    .with_name("facade_dataframe_streaming_declared_oracle.json")
    .read_text(encoding="utf-8")
)["cells"]


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A per-test facade session."""
    session = ReparkSession.builder.appName("pytest-df-stream-batch-1").getOrCreate()
    yield session
    session.stop()


def _ts_frame(spark: ReparkSession) -> DataFrame:
    """The (ts timestamp, v int) frame the oracle cells ran against."""
    return spark.createDataFrame(
        [(datetime.datetime(2023, 12, 31, 19, 0), 1)], "ts timestamp, v int"
    )


def _kv_frame(spark: ReparkSession) -> DataFrame:
    """The (k, v) frame the dedup oracle cells ran against."""
    return spark.createDataFrame([(1, 2)], "k int, v int")


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


def test_write_stream_refuses_with_spark_error_class(spark: ReparkSession) -> None:
    """writeStream raises Spark's WRITE_STREAM_NOT_ALLOWED; isStreaming stays False."""
    frame = _ts_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        _ = frame.writeStream
    _assert_error_cell(raised.value, "writeStream_batch")
    assert raised.value.getSqlState() == "42601"
    assert frame.isStreaming is False
    assert frame.is_streaming is False


def test_with_watermark_returns_the_same_batch_frame(spark: ReparkSession) -> None:
    """withWatermark returns self on batch (cells withWatermark_batch, not_ts, bad_col)."""
    frame = _ts_frame(spark)
    assert frame.withWatermark("ts", "1 minute") is frame
    _assert_frame_cell(frame, "withWatermark_batch")
    v_frame = spark.createDataFrame([(1,)], "v int")
    assert v_frame.withWatermark("v", "1 minute") is v_frame
    _assert_frame_cell(v_frame, "withWatermark_not_ts")
    assert v_frame.withWatermark("zz", "1 minute") is v_frame
    _assert_frame_cell(v_frame, "withWatermark_bad_col")
    assert frame.withWatermark("s.t", "1 minute") is frame
    assert frame.with_watermark("ts", "10 seconds") is frame


def test_with_watermark_parses_spark_interval_forms(spark: ReparkSession) -> None:
    """Signed, decimal, interval-prefixed, and multi-group delays all parse."""
    frame = _ts_frame(spark)
    for delay in (
        "0 seconds",
        "1 minute",
        "10 seconds",
        "2 hours",
        "1 day",
        "interval 1 minute",
        "1.5 hours",
        "1 day 2 hours",
        "+1 minute",
        "2 weeks",
        "100 milliseconds",
        "3 microseconds",
    ):
        assert frame.withWatermark("ts", delay) is frame, delay
    ts_only = spark.createDataFrame([(datetime.datetime(2023, 12, 31, 19, 0),)], "ts timestamp")
    _assert_frame_cell(ts_only.withWatermark("ts", "0 seconds"), "withWatermark_zero")


def test_with_watermark_rejects_non_str_arguments(spark: ReparkSession) -> None:
    """Non-str eventTime/delayThreshold raise NOT_STR (R-1: the Connect shape, not classic)."""
    frame = _ts_frame(spark)
    with pytest.raises(PySparkTypeError) as raised_event:
        frame.withWatermark(frame.v, "1 minute")
    assert raised_event.value.getCondition() == "NOT_STR"
    assert raised_event.value.getMessageParameters() == {
        "arg_name": "eventTime",
        "arg_type": "Column",
    }
    with pytest.raises(PySparkTypeError) as raised_delay:
        frame.withWatermark("ts", 5)
    _assert_error_cell(raised_delay.value, "withWatermark_delay_not_str")


def test_with_watermark_rejects_unparsable_delay(spark: ReparkSession) -> None:
    """An unparsable delay raises CANNOT_PARSE_INTERVAL with Spark's message."""
    frame = _ts_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.withWatermark("ts", "banana")
    _assert_error_cell(raised.value, "withWatermark_bad_delay")


def test_with_watermark_rejects_negative_delay(spark: ReparkSession) -> None:
    """A negative delay raises IllegalArgumentException echoing the input string."""
    frame = _ts_frame(spark)
    with pytest.raises(IllegalArgumentException) as raised:
        frame.withWatermark("ts", "-1 minute")
    _assert_error_cell(raised.value, "withWatermark_negative")
    with pytest.raises(IllegalArgumentException, match="should not be negative"):
        frame.withWatermark("ts", "interval -1 minute")


def test_drop_duplicates_within_watermark_rejects_bad_subset(spark: ReparkSession) -> None:
    """A non-list/tuple subset raises NOT_LIST_OR_TUPLE before the batch refusal."""
    frame = _kv_frame(spark)
    with pytest.raises(PySparkTypeError) as raised:
        frame.dropDuplicatesWithinWatermark("zz")
    _assert_error_cell(raised.value, "ddww_bad_subset")


def test_drop_duplicates_within_watermark_reports_missing_column(
    spark: ReparkSession,
) -> None:
    """An unresolvable subset name raises _LEGACY_ERROR_TEMP_1201 before the refusal."""
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.dropDuplicatesWithinWatermark(["zz"])
    _assert_error_cell(raised.value, "ddww_missing_col")
    with pytest.raises(AnalysisException) as raised_order:
        frame.dropDuplicatesWithinWatermark(["k", "zz"])
    _assert_error_cell(raised_order.value, "ddww_order")
    with pytest.raises(AnalysisException, match='Cannot resolve column name "zz"'):
        frame.drop_duplicates_within_watermark(["K", "zz"])


def test_drop_duplicates_within_watermark_refuses_on_batch(spark: ReparkSession) -> None:
    """Batch frames refuse with _LEGACY_ERROR_TEMP_3102; the first line is Spark's."""
    frame = _kv_frame(spark)
    with pytest.raises(AnalysisException) as raised:
        frame.dropDuplicatesWithinWatermark(["k"])
    _assert_error_cell(raised.value, "ddww_batch")
    with pytest.raises(AnalysisException) as raised_noargs:
        frame.dropDuplicatesWithinWatermark()
    _assert_error_cell(raised_noargs.value, "ddww_batch_noargs")
    with pytest.raises(AnalysisException, match="not supported with batch"):
        frame.drop_duplicates_within_watermark()


def test_rdd_property_raises_spark_not_implemented(spark: ReparkSession) -> None:
    """DataFrame.rdd raises NOT_IMPLEMENTED even when a column is named rdd."""
    frame = _kv_frame(spark)
    with pytest.raises(PySparkNotImplementedError) as raised:
        _ = frame.rdd
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "rdd"}
    assert str(raised.value) == _ORACLE["connect_rdd_shape"]["result"]["value"]
    rdd_named = spark.createDataFrame([(1,)], ["rdd"])
    with pytest.raises(PySparkNotImplementedError) as raised_shadow:
        _ = rdd_named.rdd
    assert raised_shadow.value.getCondition() == "NOT_IMPLEMENTED"


def test_pandas_api_raises_spark_not_implemented(spark: ReparkSession) -> None:
    """DataFrame.pandas_api raises NOT_IMPLEMENTED with the feature name."""
    frame = _kv_frame(spark)
    for call in (lambda: frame.pandas_api(), lambda: frame.pandas_api(index_col="k")):
        with pytest.raises(PySparkNotImplementedError) as raised:
            call()
        assert raised.value.getCondition() == "NOT_IMPLEMENTED"
        assert raised.value.getMessageParameters() == {"feature": "pandas_api"}
        assert str(raised.value) == "[NOT_IMPLEMENTED] pandas_api is not implemented."


def test_plot_property_raises_spark_not_implemented(spark: ReparkSession) -> None:
    """DataFrame.plot raises NOT_IMPLEMENTED with the feature name."""
    frame = _kv_frame(spark)
    with pytest.raises(PySparkNotImplementedError) as raised:
        _ = frame.plot
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "plot"}
    assert str(raised.value) == "[NOT_IMPLEMENTED] plot is not implemented."
