from pathlib import Path

import pytest

from repark import _native
from repark.errors import AnalysisException, PySparkNotImplementedError
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming import DataStreamReader, DataStreamWriter

_DM3_TEXT = (
    "Queries with streaming sources must be executed with writeStream.start(), "
    "or from a streaming table or flow definition within a Spark Declarative Pipeline.;\niceberg"
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-wireup").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def stream_table(spark: ReparkSession, tmp_path: Path) -> str:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.mb4")
    spark.sql("CREATE TABLE sc.mb4.orders (id BIGINT, k STRING)")
    return "sc.mb4.orders"


def _batch_frame(spark: ReparkSession) -> DataFrame:
    return spark.sql("SELECT 1 AS id")


def _stream_frame(spark: ReparkSession, table: str) -> DataFrame:
    return DataStreamReader(spark).format("iceberg").load(table)


def _schema_shape(frame: DataFrame) -> list[tuple[str, str, bool]]:
    return [
        (field.name, field.dataType.simpleString(), field.nullable) for field in frame.schema.fields
    ]


def _reader(spark: ReparkSession) -> DataStreamReader:
    return DataStreamReader(spark)


def _writer(spark: ReparkSession) -> DataStreamWriter:
    return DataStreamWriter(_batch_frame(spark))


def _terminal_type(excinfo: pytest.ExceptionInfo[BaseException]) -> None:
    assert type(excinfo.value) is NotImplementedError


def _ignore_batch(frame: DataFrame, batch_id: int) -> None:
    raise AssertionError("the stub never runs a batch body")


def test_reader_load_folds_format_case_mb0c_f4_f5(spark: ReparkSession, stream_table: str) -> None:
    for source in ("ICEBERG", "Iceberg"):
        frame = _reader(spark).format(source).load(stream_table)
        assert frame.isStreaming is True
        assert _schema_shape(frame) == [("id", "bigint", True), ("k", "string", True)]


def test_reader_load_returns_streaming_frame_mb0_r1(
    spark: ReparkSession, stream_table: str
) -> None:
    frame = _stream_frame(spark, stream_table)
    assert frame.isStreaming is True
    assert _schema_shape(frame) == [("id", "bigint", True), ("k", "string", True)]
    assert _schema_shape(frame) == _schema_shape(spark.table(stream_table))


def test_reader_table_returns_streaming_frame_mb0_r1(
    spark: ReparkSession, stream_table: str
) -> None:
    frame = _reader(spark).table(stream_table)
    assert frame.isStreaming is True
    assert _schema_shape(frame) == [("id", "bigint", True), ("k", "string", True)]
    assert _schema_shape(frame) == _schema_shape(spark.table(stream_table))


def test_reader_table_ignores_format_mb0c_f6(spark: ReparkSession, stream_table: str) -> None:
    for source in ("parquet", "bogusfmt"):
        frame = _reader(spark).format(source).table(stream_table)
        assert frame.isStreaming is True


def test_reader_option_value_cases_refuse_with_from_options_text(
    spark: ReparkSession, stream_table: str
) -> None:
    cases = [
        (
            "streaming-skip-delete-snapshots",
            "maybe",
            'streaming-skip-delete-snapshots needs true or false, got "maybe"',
        ),
        (
            "streaming-max-files-per-micro-batch",
            "abc",
            "streaming-max-files-per-micro-batch needs a positive integer file count no "
            'larger than 2147483647, got "abc"',
        ),
        (
            "streaming-max-files-per-micro-batch",
            "0",
            "streaming-max-files-per-micro-batch needs a positive integer file count no "
            'larger than 2147483647, got "0"',
        ),
        (
            "streaming-max-rows-per-micro-batch",
            "abc",
            "streaming-max-rows-per-micro-batch needs a positive integer row count no "
            'larger than 2147483647, got "abc"',
        ),
        (
            "stream-from-timestamp",
            "yesterday",
            'stream-from-timestamp needs integer milliseconds since the epoch, got "yesterday"',
        ),
        (
            "repark.cdc.start-after-snapshot-id",
            "abc",
            'repark.cdc.start-after-snapshot-id needs an integer snapshot id, got "abc"',
        ),
    ]
    for key, value, text in cases:
        with pytest.raises(AnalysisException) as excinfo:
            _reader(spark).format("iceberg").option(key, value).load(stream_table)
        assert excinfo.value.getCondition() is None, key
        assert excinfo.value.getSqlState() is None, key
        assert str(excinfo.value) == text, key


def test_reader_unsupported_spark_keys_refuse_with_from_options_text(
    spark: ReparkSession, stream_table: str
) -> None:
    reasons = {
        "streaming-snapshot-polling-interval-ms": "the trigger interval governs polling",
        "async-micro-batch-planning-enabled": (
            "RePark plans each micro-batch synchronously in its trigger"
        ),
        "async-queue-preload-file-limit": (
            "it sizes the asynchronous planning queue, which RePark does not run"
        ),
        "async-queue-preload-row-limit": (
            "it sizes the asynchronous planning queue, which RePark does not run"
        ),
    }
    for key, reason in reasons.items():
        with pytest.raises(AnalysisException) as excinfo:
            _reader(spark).format("iceberg").option(key, "1").load(stream_table)
        assert excinfo.value.getCondition() is None, key
        assert str(excinfo.value) == (
            f"{key} is a Spark/Iceberg streaming option RePark does not support; {reason}"
        ), key


def test_reader_both_start_keys_refuse_with_from_options_text(
    spark: ReparkSession, stream_table: str
) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        (
            _reader(spark)
            .format("iceberg")
            .option("stream-from-timestamp", "0")
            .option("repark.cdc.start-after-snapshot-id", "7")
            .load(stream_table)
        )
    assert excinfo.value.getCondition() is None
    assert str(excinfo.value) == (
        "stream-from-timestamp and repark.cdc.start-after-snapshot-id are both set; pass one start"
    )


def test_reader_folded_key_collision_refuses_with_from_options_text(
    spark: ReparkSession, stream_table: str
) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        _native.load_stream(
            spark._ensure_alive(),
            stream_table,
            {
                "Streaming-Max-Files-Per-Micro-Batch": "1",
                "streaming-max-files-per-micro-batch": "2",
            },
        )
    assert excinfo.value.getCondition() is None
    assert str(excinfo.value) == (
        "streaming options Streaming-Max-Files-Per-Micro-Batch and "
        "streaming-max-files-per-micro-batch are the same key in different case with "
        "different values; pass it once"
    )


def test_batch_actions_on_streaming_frame_refuse_dm3_mb0c_d1(
    spark: ReparkSession, stream_table: str
) -> None:
    frame = _stream_frame(spark, stream_table)
    doors = {
        "collect": frame.collect,
        "take": lambda: frame.take(1),
        "head": lambda: frame.head(1),
        "first": frame.first,
        "tail": lambda: frame.tail(1),
        "isEmpty": frame.isEmpty,
        "toLocalIterator": lambda: list(frame.toLocalIterator()),
        "count": frame.count,
        "show": lambda: frame.show(2),
        "to_arrow": frame.to_arrow,
        "to_arrow_batches": lambda: list(frame.to_arrow_batches()),
        "to_pandas": frame.to_pandas,
        "to_numpy": frame.to_numpy,
    }
    for name, run in doors.items():
        with pytest.raises(AnalysisException) as excinfo:
            run()
        assert (
            excinfo.value.getCondition(),
            excinfo.value.getSqlState(),
            str(excinfo.value),
        ) == ("_LEGACY_ERROR_TEMP_3102", None, _DM3_TEXT), name


def test_batch_frame_actions_unmoved(spark: ReparkSession) -> None:
    frame = _batch_frame(spark)
    assert frame.isStreaming is False
    assert [list(row) for row in frame.collect()] == [[1]]
    assert frame.count() == 1
    assert frame.to_arrow().num_rows == 1
    assert frame.show() is None


def test_with_watermark_on_streaming_frame_refuses_mbe7(
    spark: ReparkSession, stream_table: str
) -> None:
    with pytest.raises(PySparkNotImplementedError) as excinfo:
        _stream_frame(spark, stream_table).withWatermark("id", "1 hour")
    assert excinfo.value.getErrorClass() == "NOT_IMPLEMENTED"
    assert excinfo.value.getMessageParameters() == {"feature": "withWatermark"}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == "[NOT_IMPLEMENTED] withWatermark is not implemented."


def test_writer_totable_ignores_format_mb0c_f9_f11(spark: ReparkSession) -> None:
    for source in ("parquet", "bogusfmt"):
        with pytest.raises(NotImplementedError) as excinfo:
            _writer(spark).format(source).toTable("ice.sales.silver", checkpointLocation="/tmp/x")
        _terminal_type(excinfo)


def test_writer_foreach_start_ignores_format(spark: ReparkSession) -> None:
    with pytest.raises(NotImplementedError) as excinfo:
        (
            _writer(spark)
            .format("bogusfmt")
            .foreachBatch(_ignore_batch)
            .option("repark.cdc.sink", "ice.sales.silver")
            .start(checkpointLocation="/tmp/x")
        )
    _terminal_type(excinfo)
