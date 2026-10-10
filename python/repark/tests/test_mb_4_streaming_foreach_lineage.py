import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from repark import _native
from repark.errors import (
    PySparkException,
    RecoveryRequiredException,
    StreamingQueryException,
)
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming.query import StreamingQuery

_SOURCE = "sc.ln.src"
_SINK = "sc.ln.snk"
_UNSTAMPED_WRITE = "REPARK_MICROBATCH.UNSTAMPED_SINK_WRITE"
_SERIALIZABLE = (
    "'write.merge.isolation-level' = 'serializable', "
    "'write.update.isolation-level' = 'serializable', "
    "'write.delete.isolation-level' = 'serializable'"
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-foreach-lineage").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    try:
        session.stop()
    except RecoveryRequiredException:
        session.stop()


@pytest.fixture
def tables(spark: ReparkSession, tmp_path: Path) -> Path:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.ln")
    spark.sql(f"CREATE TABLE {_SOURCE} (id BIGINT, k STRING)")
    spark.sql(f"CREATE TABLE {_SINK} (id BIGINT, k STRING) TBLPROPERTIES ({_SERIALIZABLE})")
    spark.sql(f"INSERT INTO {_SINK} VALUES (90, 'seed')")
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (1, 'a'), (2, 'b')")
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (3, 'c')")
    return tmp_path


def _start(spark: ReparkSession, body: object, checkpoint: Path) -> StreamingQuery:
    return (
        spark.readStream.option("streaming-max-files-per-micro-batch", "1")
        .table(_SOURCE)
        .writeStream.foreachBatch(body)
        .option("repark.cdc.sink", _SINK)
        .option("checkpointLocation", str(checkpoint / "ck"))
        .queryName("ln")
        .trigger(availableNow=True)
        .start()
    )


def _ids(spark: ReparkSession) -> list[int]:
    return sorted(row[0] for row in spark.sql(f"SELECT id FROM {_SINK}").collect())


def _log(spark: ReparkSession) -> list[tuple[str, str | None]]:
    rows = spark.sql(
        f"SELECT snapshot_id, operation, summary FROM {_SINK}.snapshots ORDER BY committed_at"
    ).collect()
    return [(row.operation, dict(row.summary).get("repark.cdc.epoch")) for row in rows]


def _unstamped_ids(spark: ReparkSession) -> list[int]:
    rows = spark.sql(
        f"SELECT snapshot_id, summary FROM {_SINK}.snapshots ORDER BY committed_at"
    ).collect()
    return [row.snapshot_id for row in rows if "repark.cdc.epoch" not in dict(row.summary)]


def _recovery(query: StreamingQuery) -> RecoveryRequiredException:
    with pytest.raises(RecoveryRequiredException) as excinfo:
        query.awaitTermination()
    return excinfo.value


def _insert(spark: ReparkSession, frame: DataFrame) -> None:
    spark.sql(f"INSERT INTO {_SINK} VALUES (70, 'stray')")


def _append(spark: ReparkSession, frame: DataFrame) -> None:
    frame.writeTo(_SINK).append()


def _overwrite(spark: ReparkSession, frame: DataFrame) -> None:
    spark.sql(f"INSERT OVERWRITE {_SINK} VALUES (70, 'stray')")


def _await_coroutine(spark: ReparkSession, frame: DataFrame) -> None:
    asyncio.run(asyncio.to_thread(_insert, spark, frame))


class _Stray:
    def __init__(self, spark: ReparkSession, route: str, then: str, at: int = 1) -> None:
        self.spark = spark
        self.route = route
        self.then = then
        self.at = at
        self.calls = 0

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        self.calls += 1
        if batch_id != self.at:
            frame.writeTo(_SINK).append()
            return
        getattr(self, f"_{self.route}")(frame)
        if self.then == "raise":
            raise ValueError("mb4 body failed between its two sink writes")
        frame.writeTo(_SINK).append()

    def _thread_insert(self, frame: DataFrame) -> None:
        self._joined(_insert, frame)

    def _thread_append(self, frame: DataFrame) -> None:
        self._joined(_append, frame)

    def _thread_overwrite(self, frame: DataFrame) -> None:
        self._joined(_overwrite, frame)

    def _pool_insert(self, frame: DataFrame) -> None:
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(_insert, self.spark, frame).result()

    def _to_thread_insert(self, frame: DataFrame) -> None:
        _await_coroutine(self.spark, frame)

    def _joined(self, write: object, frame: DataFrame) -> None:
        writer = threading.Thread(target=write, args=(self.spark, frame))
        writer.start()
        writer.join()


_THREAD_ROUTES = [
    ("thread_insert", "append"),
    ("thread_append", "append"),
    ("thread_overwrite", "overwrite"),
    ("pool_insert", "append"),
    ("to_thread_insert", "append"),
]


@pytest.mark.parametrize(("route", "operation"), _THREAD_ROUTES)
def test_thread_route_then_raise_ends_recovery_required_and_the_restart_refuses(
    spark: ReparkSession, tables: Path, route: str, operation: str
) -> None:
    body = _Stray(spark, route, "raise")
    failure = _recovery(_start(spark, body, tables))
    stray = _unstamped_ids(spark)[-1]
    assert f"sink advanced to snapshot {stray} ({operation}) without a stamp" in str(failure)
    assert failure.epoch == 1
    assert isinstance(failure.__cause__, ValueError)
    assert body.calls == 2
    rows, log = _ids(spark), _log(spark)
    for _ in range(2):
        resumed = _Stray(spark, route, "append")
        refused = _recovery(_start(spark, resumed, tables))
        assert f"sink advanced to snapshot {stray} ({operation}) without a stamp" in str(refused)
        assert resumed.calls == 0
        assert _ids(spark) == rows
        assert _log(spark) == log


@pytest.mark.parametrize(("route", "operation"), _THREAD_ROUTES)
def test_thread_route_beside_the_stamped_write_ends_recovery_required(
    spark: ReparkSession, tables: Path, route: str, operation: str
) -> None:
    body = _Stray(spark, route, "append")
    failure = _recovery(_start(spark, body, tables))
    stray = _unstamped_ids(spark)[-1]
    assert f"sink advanced to snapshot {stray} ({operation}) without a stamp" in str(failure)
    assert failure.epoch == 1
    assert failure.__cause__ is None
    assert _log(spark)[-2:] == [(operation, None), ("append", "1")]
    rows = _ids(spark)
    resumed = _Stray(spark, route, "append", at=9)
    restart = _start(spark, resumed, tables)
    assert restart.awaitTermination() is None
    assert resumed.calls == 0
    assert _ids(spark) == rows


def test_main_thread_statement_while_a_body_runs_ends_recovery_required(
    spark: ReparkSession, tables: Path
) -> None:
    entered, release = threading.Event(), threading.Event()

    class _Blocked:
        def __call__(self, frame: DataFrame, batch_id: int) -> None:
            if batch_id == 1:
                entered.set()
                assert release.wait(60)
            frame.writeTo(_SINK).append()

    query = _start(spark, _Blocked(), tables)
    assert entered.wait(60)
    spark.sql(f"INSERT INTO {_SINK} VALUES (70, 'main')")
    release.set()
    failure = _recovery(query)
    stray = _unstamped_ids(spark)[-1]
    assert f"sink advanced to snapshot {stray} (append) without a stamp" in str(failure)


_EXPLAINED = [
    f"EXPLAIN ANALYZE INSERT INTO {_SINK} SELECT * FROM ln_batch",
    f"EXPLAIN ANALYZE INSERT INTO {_SINK} VALUES (70, 'stray')",
    f"EXPLAIN ANALYZE UPDATE {_SINK} SET k = 'seen' WHERE id = 90",
    f"EXPLAIN ANALYZE DELETE FROM {_SINK} WHERE id = 90",
]


@pytest.mark.parametrize("statement", _EXPLAINED)
@pytest.mark.parametrize("then", ["raise", "append"])
def test_explain_analyze_of_a_sink_write_refuses_before_it_lands(
    spark: ReparkSession, tables: Path, statement: str, then: str
) -> None:
    seen: list[str] = []

    def body(frame: DataFrame, batch_id: int) -> None:
        frame.createOrReplaceTempView("ln_batch")
        try:
            spark.sql(statement).collect()
        except Exception as error:
            seen.append(str(error))
            if then == "raise":
                raise
        frame.writeTo(_SINK).append()

    query = _start(spark, body, tables)
    if then == "raise":
        with pytest.raises(StreamingQueryException) as excinfo:
            query.awaitTermination()
        assert f"[{_UNSTAMPED_WRITE}] epoch 0: this commit to the declared sink" in str(
            excinfo.value
        )
        assert _ids(spark) == [90]
        assert _log(spark) == [("append", None)]
    else:
        assert query.awaitTermination() is None
        assert len(seen) == 2
        assert _ids(spark) == [1, 2, 3, 90]
        assert _log(spark) == [("append", None), ("append", "0"), ("append", "1")]
    assert all("cannot carry the epoch stamp" in text for text in seen)


def test_plain_explain_of_a_sink_write_passes(spark: ReparkSession, tables: Path) -> None:
    def body(frame: DataFrame, batch_id: int) -> None:
        spark.sql(f"EXPLAIN INSERT INTO {_SINK} VALUES (70, 'stray')").collect()
        frame.writeTo(_SINK).append()

    query = _start(spark, body, tables)
    assert query.awaitTermination() is None
    assert _ids(spark) == [1, 2, 3, 90]


def _failed_first_append(tables: Path, frame: DataFrame) -> str:
    metadata = tables / "wh" / "ln" / "snk" / "metadata"
    metadata.chmod(0o555)
    try:
        with pytest.raises(PySparkException) as excinfo:
            frame.writeTo(_SINK).append()
    finally:
        metadata.chmod(0o755)
    return str(excinfo.value)


def test_retry_after_a_failed_stamped_write_is_the_stamped_commit(
    spark: ReparkSession, tables: Path
) -> None:
    first: list[str] = []

    def body(frame: DataFrame, batch_id: int) -> None:
        if batch_id == 0:
            first.append(_failed_first_append(tables, frame))
        frame.writeTo(_SINK).append()

    query = _start(spark, body, tables)
    assert query.awaitTermination() is None
    assert len(first) == 1
    assert "already stamped" not in first[0]
    assert _ids(spark) == [1, 2, 3, 90]
    assert _log(spark) == [("append", None), ("append", "0"), ("append", "1")]


@pytest.mark.parametrize(
    "statement",
    [
        f"ALTER TABLE {_SINK} SET TBLPROPERTIES ('after-failed-claim' = 'landed')",
        f"INSERT OVERWRITE {_SINK} VALUES (100, 'ow')",
    ],
)
def test_unstampable_commit_after_a_failed_stamped_write_still_refuses(
    spark: ReparkSession, tables: Path, statement: str
) -> None:
    def body(frame: DataFrame, batch_id: int) -> None:
        _failed_first_append(tables, frame)
        spark.sql(statement)

    with pytest.raises(StreamingQueryException) as excinfo:
        _start(spark, body, tables).awaitTermination()
    assert f"[{_UNSTAMPED_WRITE}] epoch 0: this commit to the declared sink" in str(excinfo.value)
    assert _ids(spark) == [90]
    assert _log(spark) == [("append", None)]
    properties = {row[0] for row in spark.sql(f"SHOW TBLPROPERTIES {_SINK}").collect()}
    assert "after-failed-claim" not in properties


def test_body_exception_is_the_cause_of_the_query_exception(
    spark: ReparkSession, tables: Path
) -> None:
    raised = ValueError("mb4 boom-cause")

    def body(frame: DataFrame, batch_id: int) -> None:
        raise raised

    query = _start(spark, body, tables)
    with pytest.raises(StreamingQueryException) as excinfo:
        query.awaitTermination()
    assert excinfo.value.__cause__ is raised
    assert query.exception().__cause__ is raised
    assert "batch 0 failed: ValueError: mb4 boom-cause" in str(excinfo.value)
    with pytest.raises(StreamingQueryException) as anyinfo:
        spark.streams.awaitAnyTermination()
    assert anyinfo.value.__cause__ is raised


def test_sink_dropped_and_recreated_after_the_stamped_write_ends_recovery_required(
    spark: ReparkSession, tables: Path
) -> None:
    def body(frame: DataFrame, batch_id: int) -> None:
        frame.writeTo(_SINK).append()
        spark.sql(f"DROP TABLE {_SINK}")
        spark.sql(f"CREATE TABLE {_SINK} (id BIGINT, k STRING)")

    failure = _recovery(_start(spark, body, tables))
    assert "the table under the sink's name was replaced" in str(failure)
    assert _ids(spark) == []


def test_foreign_insert_between_runs_refuses_the_restart(
    spark: ReparkSession, tables: Path
) -> None:
    body = _Stray(spark, "thread_insert", "append", at=9)
    assert _start(spark, body, tables).awaitTermination() is None
    spark.sql(f"INSERT INTO {_SINK} VALUES (99, 'foreign')")
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (4, 'd')")
    foreign = _unstamped_ids(spark)[-1]
    resumed = _Stray(spark, "thread_insert", "append", at=9)
    refused = _recovery(_start(spark, resumed, tables))
    assert f"sink advanced to snapshot {foreign} (append) without a stamp" in str(refused)
    assert resumed.calls == 0
    spark.sql(f"CALL sc.system.rollback_to_snapshot('ln.snk', {_stamped_head(spark)})")
    recovered = _Stray(spark, "thread_insert", "append", at=9)
    assert _start(spark, recovered, tables).awaitTermination() is None
    assert recovered.calls == 1
    assert _ids(spark) == [1, 2, 3, 4, 90]


def _stamped_head(spark: ReparkSession) -> int:
    rows = spark.sql(
        f"SELECT snapshot_id, summary FROM {_SINK}.snapshots ORDER BY committed_at"
    ).collect()
    return [row.snapshot_id for row in rows if "repark.cdc.epoch" in dict(row.summary)][-1]


_REMEDIES = (
    "Its rows are in the sink. A restart refuses while an unstamped snapshot sits above the "
    "newest stamped batch; to clear it, either roll the sink back to its newest stamped "
    "snapshot (to the head the query first started on, if no batch is stamped yet), or start "
    "the query under a new name"
)


@pytest.mark.parametrize("route", ["thread_insert", "thread_append"])
def test_stray_at_the_first_batch_refuses_the_first_restart_from_the_mark(
    spark: ReparkSession, tables: Path, route: str
) -> None:
    body = _Stray(spark, route, "raise", at=0)
    failure = _recovery(_start(spark, body, tables))
    stray = _unstamped_ids(spark)[-1]
    text = f"sink advanced to snapshot {stray} (append) without a stamp"
    assert text in str(failure)
    assert _REMEDIES in str(failure)
    assert failure.epoch == 0
    rows, log = _ids(spark), _log(spark)
    for _ in range(2):
        resumed = _Stray(spark, route, "append", at=9)
        refused = _recovery(_start(spark, resumed, tables))
        assert text in str(refused)
        assert _REMEDIES in str(refused)
        assert resumed.calls == 0
        assert _ids(spark) == rows
        assert _log(spark) == log
    seed = spark.sql(f"SELECT snapshot_id FROM {_SINK}.snapshots ORDER BY committed_at").collect()[
        0
    ][0]
    spark.sql(f"CALL sc.system.rollback_to_snapshot('ln.snk', {seed})")
    recovered = _Stray(spark, route, "append", at=9)
    assert _start(spark, recovered, tables).awaitTermination() is None
    assert recovered.calls == 2
    assert _ids(spark) == [1, 2, 3, 90]


def _offsets(spark: ReparkSession) -> list[str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {_SINK}").collect()
    return [row[1] for row in rows if row[0].startswith("repark.cdc.offsets.")]


def _raise_before_any_write(frame: DataFrame, batch_id: int) -> None:
    raise ValueError("mb4 body failed before its sink write")


def test_first_start_writes_the_mark_into_the_offsets_property(
    spark: ReparkSession, tables: Path
) -> None:
    seed = spark.sql(f"SELECT snapshot_id FROM {_SINK}.snapshots").collect()[0][0]
    assert _offsets(spark) == []
    with pytest.raises(StreamingQueryException):
        _start(spark, _raise_before_any_write, tables).awaitTermination()
    mark = f'{{"format-version":1,"pending-epoch":0,"starting-head":{seed}}}'
    assert _offsets(spark) == [mark]
    assert _log(spark) == [("append", None)]
    healthy = _Stray(spark, "thread_insert", "append", at=9)
    assert _start(spark, healthy, tables).awaitTermination() is None
    values = _offsets(spark)
    assert len(values) == 1
    assert "pending-epoch" not in values[0]
    assert _ids(spark) == [1, 2, 3, 90]


def test_table_door_writes_no_mark(spark: ReparkSession, tables: Path) -> None:
    query = (
        spark.readStream.table(_SOURCE)
        .writeStream.option("checkpointLocation", str(tables / "ck"))
        .queryName("ln")
        .trigger(availableNow=True)
        .toTable(_SINK)
    )
    assert query.awaitTermination() is None
    values = _offsets(spark)
    assert len(values) == 1
    assert "pending-epoch" not in values[0]
    assert _log(spark) == [("append", None), ("append", "0")]
