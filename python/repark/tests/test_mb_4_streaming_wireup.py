import time
from pathlib import Path

import pytest

from repark import _native
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkNotImplementedError,
    StreamingQueryException,
)
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming import DataStreamReader, DataStreamWriter
from repark.spark.streaming.query import StreamingQuery

_DM3_TEXT = (
    "Queries with streaming sources must be executed with writeStream.start(), "
    "or from a streaming table or flow definition within a Spark Declarative Pipeline.;\niceberg"
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-wireup").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
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
        with pytest.raises(AnalysisException) as excinfo:
            _writer(spark).format(source).toTable("ice.sales.silver", checkpointLocation="/tmp/x")
        assert "needs a streaming DataFrame" in str(excinfo.value)


def test_writer_foreach_start_ignores_format(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0')")
    sink = _make_sink(spark, "sc.mb4.silver_fbfmt")
    query = (
        _stream_writer(spark, stream_table)
        .format("bogusfmt")
        .foreachBatch(_AppendTo(sink))
        .option("repark.cdc.sink", sink)
        .option("checkpointLocation", "/tmp/mb4-fb-fmt")
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination() is None
    assert _rows(spark, sink) == [[1, "k1"], [2, "k0"]]


def _stream_writer(spark: ReparkSession, table: str) -> DataStreamWriter:
    return DataStreamWriter(DataStreamReader(spark).format("iceberg").load(table))


def _make_sink(spark: ReparkSession, name: str) -> str:
    spark.sql(f"CREATE TABLE {name} (id BIGINT, k STRING)")
    return name


def _append(spark: ReparkSession, table: str, values: str) -> None:
    spark.sql(f"INSERT INTO {table} VALUES {values}")


def _rows(spark: ReparkSession, table: str) -> list[list[object]]:
    return sorted([list(row) for row in spark.sql(f"SELECT * FROM {table}").collect()])


def _batches(query: StreamingQuery) -> list[dict[str, int]]:
    return [
        {"batchId": progress["batchId"], "numInputRows": progress["numInputRows"]}
        for progress in query.recentProgress
    ]


def _snapshot_log(spark: ReparkSession, table: str) -> list[tuple[str, dict[str, str]]]:
    return [
        (row.operation, dict(row.summary))
        for row in spark.sql(
            f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


def test_table_door_replays_mb0_w1_w2(spark: ReparkSession, stream_table: str) -> None:
    for fanout in ("false", "true"):
        source = f"{stream_table}_w{fanout}"
        spark.sql(f"CREATE TABLE {source} (id BIGINT, k STRING)")
        _append(spark, source, "(1, 'k1'), (2, 'k0')")
        _append(spark, source, "(3, 'k1'), (4, 'k0')")
        sink = _make_sink(spark, f"sc.mb4.silver_w{fanout}")
        query = (
            DataStreamWriter(DataStreamReader(spark).format("iceberg").load(source))
            .format("iceberg")
            .outputMode("append")
            .option("fanout-enabled", fanout)
            .option("checkpointLocation", f"/tmp/mb4-w{fanout}")
            .trigger(availableNow=True)
            .toTable(sink)
        )
        assert query.awaitTermination() is None
        assert _rows(spark, sink) == [[1, "k1"], [2, "k0"], [3, "k1"], [4, "k0"]]
        assert _batches(query) == [{"batchId": 0, "numInputRows": 4}]
        [(operation, summary)] = _snapshot_log(spark, sink)
        assert operation == "append"
        assert summary["added-records"] == "4"
        assert summary["spark.sql.streaming.epochId"] == "0"
        assert summary["spark.sql.streaming.queryId"] == query.id


def test_table_door_replays_mb0_w5_resume(
    spark: ReparkSession, stream_table: str, tmp_path: Path
) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0')")
    sink = _make_sink(spark, "sc.mb4.silver_w5")
    checkpoint = str(tmp_path / "w5")
    first = (
        _stream_writer(spark, stream_table)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(sink)
    )
    assert first.awaitTermination() is None
    assert _batches(first) == [{"batchId": 0, "numInputRows": 2}]
    _append(spark, stream_table, "(3, 'k1'), (4, 'k0')")
    second = (
        _stream_writer(spark, stream_table)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(sink)
    )
    assert second.awaitTermination() is None
    assert _batches(second) == [{"batchId": 1, "numInputRows": 2}]
    third = (
        _stream_writer(spark, stream_table)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(sink)
    )
    assert third.awaitTermination() is None
    assert _batches(third) == [{"batchId": 2, "numInputRows": 0}]
    rows = _rows(spark, sink)
    assert rows == [[1, "k1"], [2, "k0"], [3, "k1"], [4, "k0"]]
    assert len(rows) == len({row[0] for row in rows})


def test_table_door_replays_mb0_w7_summary(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0'), (3, 'k1')")
    sink = _make_sink(spark, "sc.mb4.silver_w7")
    query = (
        _stream_writer(spark, stream_table)
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/mb4-w7")
        .trigger(availableNow=True)
        .toTable(sink)
    )
    assert query.awaitTermination() is None
    [(operation, summary)] = _snapshot_log(spark, sink)
    assert operation == "append"
    assert summary["added-records"] == "3"
    assert summary["spark.sql.streaming.epochId"] == "0"
    assert summary["spark.sql.streaming.queryId"] == query.id


def test_table_door_replays_mb0_t1_once(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0')")
    _append(spark, stream_table, "(3, 'k1')")
    for options in ({}, {"streaming-max-files-per-micro-batch": "1"}):
        sink = _make_sink(spark, f"sc.mb4.silver_t1_{len(options)}")
        reader = DataStreamReader(spark).format("iceberg")
        for key, value in options.items():
            reader = reader.option(key, value)
        query = (
            DataStreamWriter(reader.load(stream_table))
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", f"/tmp/mb4-t1-{len(options)}")
            .trigger(once=True)
            .toTable(sink)
        )
        assert query.awaitTermination() is None
        assert query.isActive is False
        assert _batches(query) == [{"batchId": 0, "numInputRows": 3}]
        assert _rows(spark, sink) == [[1, "k1"], [2, "k0"], [3, "k1"]]


def test_table_door_replays_mb0_t3_progress(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0'), (3, 'k1')")
    sink = _make_sink(spark, "sc.mb4.silver_t3")
    query = (
        _stream_writer(spark, stream_table)
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/mb4-t3")
        .trigger(processingTime="1 hour")
        .toTable(sink)
    )
    deadline = time.monotonic() + 60.0
    while query.lastProgress is None and time.monotonic() < deadline:
        time.sleep(0.1)
    last = query.lastProgress
    assert last is not None
    assert (last["batchId"], last["numInputRows"]) == (0, 3)
    assert last["id"] == query.id
    assert last["runId"] == query.runId
    assert last["sources"][0]["numInputRows"] == 3
    assert last["sources"][0]["description"] == "IcebergMicroBatchStream[mb4.orders]"
    assert last["sink"]["numOutputRows"] == 3
    assert last["sink"]["description"] == sink
    assert set(last["durationMs"]) == {
        "addBatch",
        "commitOffsets",
        "getBatch",
        "latestOffset",
        "queryPlanning",
        "triggerExecution",
        "walCommit",
    }
    assert query.status == {
        "message": "Waiting for next trigger",
        "isDataAvailable": True,
        "isTriggerActive": False,
    }
    assert query.isActive is True
    assert query.stop() is None
    assert query.awaitTermination() is None
    assert query.awaitTermination(1) is True
    assert query.isActive is False
    assert query.exception() is None
    assert query.status == {
        "message": "Stopped",
        "isDataAvailable": False,
        "isTriggerActive": False,
    }
    assert _rows(spark, sink) == [[1, "k1"], [2, "k0"], [3, "k1"]]


def test_table_door_start_with_path_runs(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0'), (3, 'k1')")
    sink = _make_sink(spark, "sc.mb4.silver_path")
    query = (
        _stream_writer(spark, stream_table)
        .format("iceberg")
        .option("checkpointLocation", "/tmp/mb4-path")
        .trigger(availableNow=True)
        .start(sink)
    )
    assert query.awaitTermination() is None
    assert _rows(spark, sink) == [[1, "k1"], [2, "k0"], [3, "k1"]]


def test_table_door_start_with_path_option_runs(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0'), (3, 'k1')")
    sink = _make_sink(spark, "sc.mb4.silver_pathopt")
    query = (
        _stream_writer(spark, stream_table)
        .format("iceberg")
        .option("path", sink)
        .option("checkpointLocation", "/tmp/mb4-pathopt")
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination() is None
    assert _rows(spark, sink) == [[1, "k1"], [2, "k0"], [3, "k1"]]


def test_table_door_sink_busy_refuses_mbe13(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1')")
    sink = _make_sink(spark, "sc.mb4.silver_busy")
    running = (
        _stream_writer(spark, stream_table)
        .option("checkpointLocation", "/tmp/mb4-busy-1")
        .trigger(processingTime="1 hour")
        .toTable(sink)
    )
    assert running.isActive is True
    with pytest.raises(StreamingQueryException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .queryName("busy_other")
            .option("checkpointLocation", "/tmp/mb4-busy-2")
            .trigger(availableNow=True)
            .toTable(sink)
        )
    assert excinfo.value.getCondition() == "STREAM_FAILED"
    assert excinfo.value.getSqlState() == "XXKST"
    assert str(excinfo.value) == (
        "[STREAM_FAILED] [REPARK_MICROBATCH.SINK_BUSY] sink sc.mb4.silver_busy is busy: "
        "another streaming query or batch is active on it; one at a time per sink "
        "SQLSTATE: XXKST"
    )
    with pytest.raises(AnalysisException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .option("checkpointLocation", "/tmp/mb4-busy-4")
            .trigger(availableNow=True)
            .toTable(sink)
        )
    assert excinfo.value.getCondition() is None
    assert "as another query with same id is already active" in str(excinfo.value)
    assert running.stop() is None
    retry = (
        _stream_writer(spark, stream_table)
        .option("checkpointLocation", "/tmp/mb4-busy-3")
        .trigger(availableNow=True)
        .toTable(sink)
    )
    assert retry.awaitTermination() is None


def test_table_door_empty_checkpoint_refuses_mb0c_w3(
    spark: ReparkSession, stream_table: str
) -> None:
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .option("checkpointLocation", "")
            .trigger(availableNow=True)
            .toTable("sc.mb4.silver")
        )
    assert excinfo.value.getCondition() is None
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == "Can not create a Path from an empty string"
    spark.conf.set("spark.sql.streaming.checkpointLocation", "")
    with pytest.raises(IllegalArgumentException) as excinfo:
        _stream_writer(spark, stream_table).trigger(availableNow=True).toTable("sc.mb4.silver")
    assert str(excinfo.value) == "Can not create a Path from an empty string"


def test_table_door_duplicate_name_refuses_mb0c_m3(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1')")
    first_sink = _make_sink(spark, "sc.mb4.silver_dup1")
    second_sink = _make_sink(spark, "sc.mb4.silver_dup2")
    first = (
        _stream_writer(spark, stream_table)
        .queryName("mb4_dup")
        .option("checkpointLocation", "/tmp/mb4-dup-1")
        .trigger(processingTime="1 hour")
        .toTable(first_sink)
    )
    assert first.isActive is True
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .queryName("mb4_dup")
            .option("checkpointLocation", "/tmp/mb4-dup-2")
            .trigger(availableNow=True)
            .toTable(second_sink)
        )
    assert excinfo.value.getCondition() is None
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == (
        "Cannot start query with name mb4_dup as a query with that name is already "
        "active in this SparkSession"
    )
    assert first.stop() is None


def test_table_door_missing_sink_refuses_with_dated_row(
    spark: ReparkSession, stream_table: str
) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .option("checkpointLocation", "/tmp/mb4-nosink")
            .trigger(availableNow=True)
            .toTable("sc.mb4.nosuch")
        )
    assert excinfo.value.getCondition() is None
    assert excinfo.value.getSqlState() is None
    assert "cannot load table" in str(excinfo.value)


def test_table_door_matching_sink_option_runs(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1')")
    sink = _make_sink(spark, "sc.mb4.silver_match")
    query = (
        _stream_writer(spark, stream_table)
        .option("repark.cdc.sink", sink)
        .option("checkpointLocation", "/tmp/mb4-match")
        .trigger(availableNow=True)
        .toTable(sink)
    )
    assert query.awaitTermination() is None
    assert _rows(spark, sink) == [[1, "k1"]]


def test_table_door_catalog_timeout_grammar(spark: ReparkSession, stream_table: str) -> None:
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .option("repark.cdc.catalog-timeout", "bogus")
            .option("checkpointLocation", "/tmp/mb4-cto")
            .trigger(availableNow=True)
            .toTable("sc.mb4.silver")
        )
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .option("repark.cdc.catalog-timeout", "60s")
            .option("checkpointLocation", "/tmp/mb4-cto")
            .trigger(availableNow=True)
            .toTable("sc.mb4.silver")
        )
    assert excinfo.value.getCondition() == "INVALID_INTERVAL_FORMAT.INVALID_VALUE"
    assert excinfo.value.getSqlState() == "22006"
    assert excinfo.value.getMessageParameters() == {"input": "60s", "value": "60s"}
    assert str(excinfo.value) == (
        "[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '60s' to interval. "
        "Please ensure that the value provided is in a valid format for defining an "
        "interval. You can reference the documentation for the correct format. "
        "Invalid value 60s. SQLSTATE: 22006"
    )
    _append(spark, stream_table, "(1, 'k1')")
    sink = _make_sink(spark, "sc.mb4.silver_cto")
    query = (
        _stream_writer(spark, stream_table)
        .option("repark.cdc.catalog-timeout", "5 seconds")
        .option("checkpointLocation", "/tmp/mb4-cto")
        .trigger(availableNow=True)
        .toTable(sink)
    )
    assert query.awaitTermination() is None
    assert _rows(spark, sink) == [[1, "k1"]]


def test_table_door_confs_take_effect(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1')")
    _append(spark, stream_table, "(2, 'k0')")
    _append(spark, stream_table, "(3, 'k1')")
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "2")
    spark.conf.set("spark.sql.streaming.stopTimeout", "15s")
    spark.conf.set("spark.sql.streaming.pollingDelay", "100ms")
    sink = _make_sink(spark, "sc.mb4.silver_conf")
    query = (
        DataStreamWriter(
            DataStreamReader(spark)
            .format("iceberg")
            .option("streaming-max-rows-per-micro-batch", "1")
            .load(stream_table)
        )
        .option("checkpointLocation", "/tmp/mb4-conf")
        .trigger(availableNow=True)
        .toTable(sink)
    )
    assert query.awaitTermination() is None
    assert _batches(query) == [
        {"batchId": 1, "numInputRows": 1},
        {"batchId": 2, "numInputRows": 1},
    ]
    assert _rows(spark, sink) == [[1, "k1"], [2, "k0"], [3, "k1"]]


def test_table_door_bad_confs_refuse_like_spark_set(
    spark: ReparkSession, stream_table: str
) -> None:
    cases = [
        ("spark.sql.streaming.stopTimeout", "time in MILLISECONDS"),
        ("spark.sql.streaming.pollingDelay", "time in MILLISECONDS"),
        ("spark.sql.streaming.numRecentProgressUpdates", "int"),
    ]
    for key, conf_type in cases:
        spark.conf.set(key, "abc")
        with pytest.raises(IllegalArgumentException) as excinfo:
            (
                _stream_writer(spark, stream_table)
                .option("checkpointLocation", "/tmp/mb4-badconf")
                .trigger(availableNow=True)
                .toTable("sc.mb4.silver")
            )
        assert excinfo.value.getCondition() == "INVALID_CONF_VALUE.TYPE_MISMATCH", key
        assert excinfo.value.getSqlState() == "22022", key
        assert excinfo.value.getMessageParameters() == {
            "confName": key,
            "confValue": "abc",
            "confType": conf_type,
        }, key
        assert str(excinfo.value) == (
            f"[INVALID_CONF_VALUE.TYPE_MISMATCH] The value 'abc' in the config \"{key}\" "
            f"is invalid. It should be a/an '{conf_type}' value. SQLSTATE: 22022"
        ), key
        spark.conf.unset(key)


class _AppendTo:
    def __init__(self, table: str) -> None:
        self.table = table
        self.seen: list[tuple[int, list[list[object]]]] = []

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        self.seen.append((batch_id, sorted([list(row) for row in frame.collect()])))
        frame.writeTo(self.table).append()


def test_foreach_door_first_pin_write_to_commits_once_stamped(
    spark: ReparkSession, stream_table: str
) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0'), (3, 'k1')")
    sink = _make_sink(spark, "sc.mb4.silver_fb1")
    body = _AppendTo(sink)
    query = (
        _stream_writer(spark, stream_table)
        .foreachBatch(body)
        .option("repark.cdc.sink", sink)
        .option("checkpointLocation", "/tmp/mb4-fb1")
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination() is None
    assert body.seen == [(0, [[1, "k1"], [2, "k0"], [3, "k1"]])]
    assert _rows(spark, sink) == [[1, "k1"], [2, "k0"], [3, "k1"]]
    log = _snapshot_log(spark, sink)
    assert len(log) == 1
    stamped = [(operation, summary) for operation, summary in log if "repark.cdc.epoch" in summary]
    assert len(stamped) == 1
    assert stamped[0][1]["repark.cdc.epoch"] == "0"
    assert stamped[0][1]["repark.cdc.query-id"] == query.id
    assert "spark.sql.streaming.epochId" not in stamped[0][1]
    assert stamped[0][1]["added-records"] == "3"


def test_start_with_file_path_refuses_as_unresolved_sink(
    spark: ReparkSession, stream_table: str
) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .format("iceberg")
            .option("checkpointLocation", "/tmp/mb4-filepath")
            .trigger(availableNow=True)
            .start("/tmp/x")
        )
    assert excinfo.value.getCondition() is None
    assert 'streaming sink table "/tmp/x" is not a valid identifier' in str(excinfo.value)
