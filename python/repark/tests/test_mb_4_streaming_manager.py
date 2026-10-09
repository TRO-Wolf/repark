import sys
from functools import partial
from pathlib import Path

import pytest

from repark.errors import RecoveryRequiredException, StreamingQueryException
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


def _replace_sink_table(spark: ReparkSession, sink: str, frame: DataFrame, batch_id: int) -> None:
    spark.sql(f"DROP TABLE {sink}")
    spark.sql(f"CREATE TABLE {sink} (id BIGINT, k STRING)")


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


def test_spark_stop_stops_a_running_query_and_second_stop_is_quiet(
    spark: ReparkSession, stream_table: str
) -> None:
    query = _start_daemon(spark, stream_table, "sc.mb4.silver_stop9c", "/tmp/mb4-stop-9c", "stop9c")
    assert query.isActive is True
    assert spark.stop() is None
    assert query.isActive is False
    assert spark.stop() is None


def test_spark_stop_never_raises_for_a_failed_query(
    spark: ReparkSession, stream_table: str
) -> None:
    spark.sql("CREATE TABLE sc.mb4.silver_stop9b (id BIGINT, k STRING)")
    query = (
        DataStreamWriter(DataStreamReader(spark).format("iceberg").load(stream_table))
        .foreachBatch(_boom)
        .option("repark.cdc.sink", "sc.mb4.silver_stop9b")
        .option("checkpointLocation", "/tmp/mb4-stop-9b")
        .trigger(availableNow=True)
        .start()
    )
    with pytest.raises(StreamingQueryException) as excinfo:
        query.awaitTermination()
    assert not isinstance(excinfo.value, RecoveryRequiredException)
    assert spark.stop() is None
    assert query.isActive is False
    assert spark.stop() is None


def test_spark_stop_raises_first_recovery_after_releasing(
    spark: ReparkSession, stream_table: str, tmp_path: Path
) -> None:
    sink = "sc.mb4.silver_stop9a"
    spark.sql(f"CREATE TABLE {sink} (id BIGINT, k STRING)")
    query = (
        DataStreamWriter(DataStreamReader(spark).format("iceberg").load(stream_table))
        .foreachBatch(partial(_replace_sink_table, spark, sink))
        .option("repark.cdc.sink", sink)
        .option("checkpointLocation", "/tmp/mb4-stop-9a")
        .trigger(availableNow=True)
        .start()
    )
    with pytest.raises(RecoveryRequiredException):
        query.awaitTermination()
    assert isinstance(query.exception(), RecoveryRequiredException)
    module = tmp_path / "mb4_stop9a_mod.py"
    module.write_text("MARKER = 1\n", encoding="utf-8")
    spark.addArtifact(str(module), pyfile=True)
    warehouse = spark._alive_token["auto_catalog_warehouse"]
    warehouse_path = warehouse.name
    artifact_dir = spark._alive_token["artifact_dir"]
    with pytest.raises(RecoveryRequiredException):
        spark.stop()
    assert spark._inner is None
    assert "auto_catalog_warehouse" not in spark._alive_token
    assert "artifact_dir" not in spark._alive_token
    assert not Path(warehouse_path).exists()
    assert not Path(artifact_dir).exists()
    assert artifact_dir not in sys.path
    assert spark.stop() is None
