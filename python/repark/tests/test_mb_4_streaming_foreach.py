from pathlib import Path

import pytest

from repark import _native
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    StreamingQueryException,
)
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming import DataStreamReader, DataStreamWriter
from repark.spark.streaming.query import StreamingQuery

_W8_TEXT = (
    'checkpointLocation must be specified either through option("checkpointLocation", ...) '
    'or SparkSession.conf.set("spark.sql.streaming.checkpointLocation", ...).'
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-foreach").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    session.stop()


@pytest.fixture
def stream_table(spark: ReparkSession, tmp_path: Path) -> str:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.mb4")
    spark.sql("CREATE TABLE sc.mb4.orders (id BIGINT, k STRING)")
    return "sc.mb4.orders"


def _stream_writer(spark: ReparkSession, table: str) -> DataStreamWriter:
    return DataStreamWriter(DataStreamReader(spark).format("iceberg").load(table))


def _make_sink(spark: ReparkSession, name: str) -> str:
    spark.sql(f"CREATE TABLE {name} (id BIGINT, k STRING)")
    return name


def _append(spark: ReparkSession, table: str, values: str) -> None:
    spark.sql(f"INSERT INTO {table} VALUES {values}")


def _rows(spark: ReparkSession, table: str) -> list[list[object]]:
    return sorted([list(row) for row in spark.sql(f"SELECT * FROM {table}").collect()])


def _schema_shape(frame: DataFrame) -> list[tuple[str, str, bool]]:
    return [
        (field.name, field.dataType.simpleString(), field.nullable) for field in frame.schema.fields
    ]


def _snapshot_log(spark: ReparkSession, table: str) -> list[tuple[str, dict[str, str]]]:
    return [
        (row.operation, dict(row.summary))
        for row in spark.sql(
            f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at"
        ).collect()
    ]


class _Collect:
    def __init__(self) -> None:
        self.schema: list[tuple[str, str, bool]] | None = None
        self.batches: list[tuple[int, list[list[object]]]] = []

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        if self.schema is None:
            self.schema = _schema_shape(frame)
        self.batches.append((batch_id, sorted([list(row) for row in frame.collect()])))


class _AppendTo:
    def __init__(self, table: str) -> None:
        self.table = table
        self.seen: list[int] = []

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        self.seen.append(batch_id)
        frame.writeTo(self.table).append()


def _boom(frame: DataFrame, batch_id: int) -> None:
    raise RuntimeError("mb4 injected failure at batch")


class _Actions:
    def __init__(self) -> None:
        self.seen: list[tuple[int, list[list[object]]]] = []
        self.counts: list[tuple[int, int]] = []

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        self.seen.append((batch_id, sorted([list(row) for row in frame.collect()])))
        self.counts.append((batch_id, frame.count()))
        frame.show()


def test_foreach_door_replays_mb0_t2_batches(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0')")
    _append(spark, stream_table, "(3, 'k1')")
    _make_sink(spark, "sc.mb4.silver_t2")
    body = _Collect()
    query = (
        DataStreamWriter(
            DataStreamReader(spark)
            .format("iceberg")
            .option("streaming-max-files-per-micro-batch", "1")
            .load(stream_table)
        )
        .foreachBatch(body)
        .option("repark.cdc.sink", "sc.mb4.silver_t2")
        .option("checkpointLocation", "/tmp/mb4-fb-t2")
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination() is None
    assert body.schema == [("id", "bigint", True), ("k", "string", True)]
    assert body.batches == [(0, [[1, "k1"], [2, "k0"]]), (1, [[3, "k1"]])]
    assert len(body.batches) == 2
    assert query.isActive is False
    assert query.exception() is None


def test_foreach_door_replays_mb0_r1_resume(
    spark: ReparkSession, stream_table: str, tmp_path: Path
) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0')")
    _append(spark, stream_table, "(3, 'k1')")
    _make_sink(spark, "sc.mb4.silver_r1")
    body = _Collect()
    checkpoint = str(tmp_path / "fb-r1")
    for ids in ("(4, 'k0'), (5, 'k1')", None):
        query = (
            _stream_writer(spark, stream_table)
            .foreachBatch(body)
            .option("repark.cdc.sink", "sc.mb4.silver_r1")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        assert query.awaitTermination() is None
        if ids is not None:
            _append(spark, stream_table, ids)
    query = (
        _stream_writer(spark, stream_table)
        .foreachBatch(body)
        .option("repark.cdc.sink", "sc.mb4.silver_r1")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination() is None
    assert body.schema == [("id", "bigint", True), ("k", "string", True)]
    assert body.batches == [
        (0, [[1, "k1"], [2, "k0"], [3, "k1"]]),
        (1, [[4, "k0"], [5, "k1"]]),
    ]


def test_public_doors_replay_mb0_r1_resume(
    spark: ReparkSession, stream_table: str, tmp_path: Path
) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0')")
    _append(spark, stream_table, "(3, 'k1')")
    _make_sink(spark, "sc.mb4.silver_pubr1")
    body = _Collect()
    checkpoint = str(tmp_path / "fb-pubr1")
    for ids in ("(4, 'k0'), (5, 'k1')", None):
        query = (
            spark.readStream.format("iceberg")
            .load(stream_table)
            .writeStream.foreachBatch(body)
            .option("repark.cdc.sink", "sc.mb4.silver_pubr1")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        assert query.awaitTermination() is None
        if ids is not None:
            _append(spark, stream_table, ids)
    query = (
        spark.readStream.format("iceberg")
        .load(stream_table)
        .writeStream.foreachBatch(body)
        .option("repark.cdc.sink", "sc.mb4.silver_pubr1")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination() is None
    assert body.schema == [("id", "bigint", True), ("k", "string", True)]
    assert body.batches == [
        (0, [[1, "k1"], [2, "k0"], [3, "k1"]]),
        (1, [[4, "k0"], [5, "k1"]]),
    ]


def test_foreach_door_replays_mb0_w4_stamped(spark: ReparkSession, stream_table: str) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0')")
    sink = _make_sink(spark, "sc.mb4.silver_fbw4")
    body = _AppendTo(sink)
    checkpoint = "/tmp/mb4-fb-w4"
    queries: list[StreamingQuery] = []
    for step in range(2):
        query = (
            _stream_writer(spark, stream_table)
            .foreachBatch(body)
            .option("repark.cdc.sink", sink)
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        assert query.awaitTermination() is None
        queries.append(query)
        if step == 0:
            _append(spark, stream_table, "(3, 'k1'), (4, 'k0')")
    assert body.seen == [0, 1]
    assert _rows(spark, sink) == [[1, "k1"], [2, "k0"], [3, "k1"], [4, "k0"]]
    log = _snapshot_log(spark, sink)
    assert len(log) == 4
    writes = [summary for _, summary in log if "repark.cdc.epoch" not in summary]
    assert [summary["added-records"] for summary in writes] == ["2", "2"]
    stamps = [summary for _, summary in log if "repark.cdc.epoch" in summary]
    assert [summary["repark.cdc.epoch"] for summary in stamps] == ["0", "1"]
    assert stamps[0]["repark.cdc.query-id"] == queries[0].id
    assert stamps[1]["repark.cdc.query-id"] == queries[0].id
    assert stamps[0]["repark.cdc.run-id"] == queries[0].runId
    assert stamps[1]["repark.cdc.run-id"] == queries[1].runId
    assert all("spark.sql.streaming.epochId" not in summary for summary in stamps)


def test_foreach_door_raising_body_fails_mb0_w6_mbe16(
    spark: ReparkSession, stream_table: str
) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0')")
    _make_sink(spark, "sc.mb4.silver_fbw6")
    query = (
        _stream_writer(spark, stream_table)
        .foreachBatch(_boom)
        .option("repark.cdc.sink", "sc.mb4.silver_fbw6")
        .option("checkpointLocation", "/tmp/mb4-fb-w6")
        .trigger(availableNow=True)
        .start()
    )
    text = (
        f"[STREAM_FAILED] Query [id = {query.id}, runId = {query.runId}] "
        "terminated with exception: batch 0 failed: "
        "RuntimeError: mb4 injected failure at batch SQLSTATE: XXKST"
    )
    with pytest.raises(StreamingQueryException) as excinfo:
        query.awaitTermination()
    assert excinfo.value.getCondition() == "STREAM_FAILED"
    assert excinfo.value.getSqlState() == "XXKST"
    assert str(excinfo.value) == text
    assert query.isActive is False
    failure = query.exception()
    assert failure is not None
    assert type(failure) is StreamingQueryException
    assert str(failure) == text


def test_foreach_door_missing_checkpoint_refuses_w8_rule(
    spark: ReparkSession, stream_table: str
) -> None:
    with pytest.raises(AnalysisException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .foreachBatch(_AppendTo("sc.mb4.silver"))
            .option("repark.cdc.sink", "sc.mb4.silver")
            .trigger(availableNow=True)
            .start()
        )
    assert excinfo.value.getCondition() == "_LEGACY_ERROR_TEMP_1298"
    assert excinfo.value.getMessageParameters() == {}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == _W8_TEXT


def test_foreach_door_batch_actions_in_body_complete(
    spark: ReparkSession, stream_table: str
) -> None:
    _append(spark, stream_table, "(1, 'k1'), (2, 'k0'), (3, 'k1')")
    _make_sink(spark, "sc.mb4.silver_fbactions")
    body = _Actions()
    query = (
        _stream_writer(spark, stream_table)
        .foreachBatch(body)
        .option("repark.cdc.sink", "sc.mb4.silver_fbactions")
        .option("checkpointLocation", "/tmp/mb4-fb-actions")
        .trigger(availableNow=True)
        .start()
    )
    assert query.awaitTermination() is None
    assert body.seen == [(0, [[1, "k1"], [2, "k0"], [3, "k1"]])]
    assert body.counts == [(0, 3)]
    assert query.exception() is None


def test_foreach_door_missing_sink_refuses_mbe10(spark: ReparkSession, stream_table: str) -> None:
    with pytest.raises(IllegalArgumentException) as excinfo:
        (
            _stream_writer(spark, stream_table)
            .foreachBatch(_AppendTo("sc.mb4.silver"))
            .option("checkpointLocation", "/tmp/mb4-fb-nosink")
            .trigger(availableNow=True)
            .start()
        )
    assert excinfo.value.getCondition() == "REPARK_MICROBATCH.SINK_UNDECLARED"
    assert excinfo.value.getMessageParameters() == {}
    assert excinfo.value.getSqlState() is None
    assert str(excinfo.value) == (
        "[REPARK_MICROBATCH.SINK_UNDECLARED] no sink declared for this streaming query; "
        'pass .option("repark.cdc.sink", "<table>")'
    )
