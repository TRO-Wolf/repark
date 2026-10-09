from pathlib import Path

import pytest

from repark.errors import StreamingQueryException
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming import DataStreamReader, DataStreamWriter
from repark.spark.streaming.query import StreamingQuery, StreamingQueryManager


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-manager").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def stream_table(spark: ReparkSession, tmp_path: Path) -> str:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.mb4")
    spark.sql("CREATE TABLE sc.mb4.orders (id BIGINT, k STRING)")
    spark.sql("INSERT INTO sc.mb4.orders VALUES (1, 'k1'), (2, 'k0'), (3, 'k1')")
    return "sc.mb4.orders"


def _start_daemon(
    spark: ReparkSession, source: str, sink: str, checkpoint: str, name: str
) -> StreamingQuery:
    spark.sql(f"CREATE TABLE {sink} (id BIGINT, k STRING)")
    return (
        DataStreamWriter(DataStreamReader(spark).format("iceberg").load(source))
        .option("checkpointLocation", checkpoint)
        .toTable(sink, queryName=name)
    )


def _boom(frame: DataFrame, batch_id: int) -> None:
    raise RuntimeError("mb4 injected failure at batch")


def test_manager_active_lists_running_and_drops_stopped(
    spark: ReparkSession, stream_table: str
) -> None:
    manager = StreamingQueryManager(spark)
    assert manager.active == []
    query = _start_daemon(spark, stream_table, "sc.mb4.silver_mgr1", "/tmp/mb4-mgr-1", "mgr1")
    try:
        assert query.isActive is True
        active = manager.active
        assert [found.id for found in active] == [query.id]
        assert [found.runId for found in active] == [query.runId]
    finally:
        query.stop()
    assert manager.active == []


def test_manager_get_returns_the_query_by_id(spark: ReparkSession, stream_table: str) -> None:
    manager = StreamingQueryManager(spark)
    query = _start_daemon(spark, stream_table, "sc.mb4.silver_mgr2", "/tmp/mb4-mgr-2", "mgr2")
    try:
        found = manager.get(query.id)
        assert found is not None
        assert found.id == query.id
        assert found.runId == query.runId
        assert found.name == "mgr2"
    finally:
        query.stop()
    assert manager.get(query.id) is None


def test_manager_await_any_termination_reports_one_of_two_stops(
    spark: ReparkSession, stream_table: str
) -> None:
    manager = StreamingQueryManager(spark)
    first = _start_daemon(spark, stream_table, "sc.mb4.silver_mgr3a", "/tmp/mb4-mgr-3a", "mgr3a")
    second = _start_daemon(spark, stream_table, "sc.mb4.silver_mgr3b", "/tmp/mb4-mgr-3b", "mgr3b")
    try:
        assert manager.awaitAnyTermination(0) is False
        first.stop()
        assert manager.awaitAnyTermination() is None
        assert manager.awaitAnyTermination(30) is True
        assert second.isActive is True
    finally:
        first.stop()
        second.stop()


def test_manager_reset_terminated_clears_the_record(
    spark: ReparkSession, stream_table: str
) -> None:
    manager = StreamingQueryManager(spark)
    first = _start_daemon(spark, stream_table, "sc.mb4.silver_mgr4a", "/tmp/mb4-mgr-4a", "mgr4a")
    second = _start_daemon(spark, stream_table, "sc.mb4.silver_mgr4b", "/tmp/mb4-mgr-4b", "mgr4b")
    try:
        first.stop()
        assert manager.awaitAnyTermination() is None
        assert manager.resetTerminated() is None
        assert manager.awaitAnyTermination(0) is False
        second.stop()
        assert manager.awaitAnyTermination(0) is True
    finally:
        first.stop()
        second.stop()


def test_manager_await_any_termination_raises_the_failed_query_error(
    spark: ReparkSession, stream_table: str
) -> None:
    spark.sql("CREATE TABLE sc.mb4.silver_mgr5 (id BIGINT, k STRING)")
    query = (
        DataStreamWriter(DataStreamReader(spark).format("iceberg").load(stream_table))
        .foreachBatch(_boom)
        .option("repark.cdc.sink", "sc.mb4.silver_mgr5")
        .option("checkpointLocation", "/tmp/mb4-mgr-5")
        .trigger(availableNow=True)
        .start()
    )
    with pytest.raises(StreamingQueryException) as excinfo:
        query.awaitTermination()
    failure = query.exception()
    assert failure is not None
    assert str(failure) == str(excinfo.value)
    manager = StreamingQueryManager(spark)
    with pytest.raises(StreamingQueryException) as anyinfo:
        manager.awaitAnyTermination()
    assert anyinfo.value.getCondition() == "STREAM_FAILED"
    assert anyinfo.value.getSqlState() == "XXKST"
    assert str(anyinfo.value) == str(failure)
