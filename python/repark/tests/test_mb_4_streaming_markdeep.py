import random
import re
import threading
import time
from pathlib import Path

import pytest

from repark import _native
from repark.errors import RecoveryRequiredException, StreamingQueryException
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming.query import StreamingQuery

_DELETE = "DELETE FROM {sink} WHERE id < 2"
_ATTEMPTS = 6


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-markdeep").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    try:
        session.stop()
    except RecoveryRequiredException:
        session.stop()


class _Boom:
    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        raise RuntimeError("before any write")


class _Raced:
    def __init__(self, source: str, sink: str, name: str, outcome: str) -> None:
        self.source = source
        self.sink = sink
        self.name = name
        self.outcome = outcome


def _start(
    spark: ReparkSession, door: str, source: str, sink: str, name: str, root: Path
) -> StreamingQuery:
    writer = (
        spark.readStream.option("streaming-max-files-per-micro-batch", "1")
        .table(source)
        .writeStream.option("checkpointLocation", str(root / f"ck{name}"))
        .queryName(name)
        .trigger(availableNow=True)
    )
    if door == "foreach":
        return writer.foreachBatch(_Boom()).option("repark.cdc.sink", sink).start()
    return writer.toTable(sink)


def _shape(spark: ReparkSession, sink: str) -> str:
    rows = spark.sql(
        f"SELECT snapshot_id, parent_id, operation, summary FROM {sink}.snapshots"
    ).collect()
    by_id = {row.snapshot_id: row for row in rows}
    head_rows = spark.sql(f"SELECT snapshot_id FROM {sink}.refs WHERE name = 'main'").collect()
    head = head_rows[0][0] if head_rows else None
    line = []
    while head is not None and head in by_id:
        line.append(by_id[head])
        head = by_id[head].parent_id
    letters = ""
    for row in reversed(line):
        summary = dict(row.summary)
        if "repark.cdc.epoch" in summary:
            letters += "T" if "spark.sql.streaming.queryId" in summary else "S"
        else:
            letters += {"append": "f", "delete": "d", "overwrite": "o", "replace": "c"}.get(
                row.operation, "?"
            )
    return letters


def _snapshots(spark: ReparkSession, sink: str) -> list[tuple[int, str, str | None]]:
    rows = spark.sql(f"SELECT snapshot_id, operation, summary FROM {sink}.snapshots").collect()
    return [
        (row.snapshot_id, row.operation, dict(row.summary).get("repark.cdc.epoch")) for row in rows
    ]


def _stray_landed(before: set[int], after: list[tuple[int, str, str | None]]) -> bool:
    return any(epoch is None for snapshot, _, epoch in after if snapshot not in before)


def _delete_effect(spark: ReparkSession, sink: str) -> bool:
    ids = [row[0] for row in spark.sql(f"SELECT id FROM {sink}").collect()]
    return len({0, 1} - set(ids)) > 0


def _run_foreign(spark: ReparkSession, sink: str, delay: float, outcome: list[str]) -> None:
    time.sleep(delay)
    try:
        spark.sql(_DELETE.format(sink=sink))
    except Exception as failed:
        outcome.append(f"{type(failed).__name__}: {failed}")
    else:
        outcome.append("ok")


def _race_one(
    spark: ReparkSession, tmp_path: Path, tag: str, delay: float, attempt: int
) -> _Raced | None:
    source = f"sc.md.src{tag}{attempt}"
    sink = f"sc.md.snk{tag}{attempt}"
    name = f"md{tag}{attempt}"
    spark.sql(f"CREATE TABLE {sink} (id BIGINT, k STRING)")
    spark.sql(f"CREATE TABLE {source} (id BIGINT, k STRING)")
    low = 0
    for _ in range(4):
        spark.sql(f"INSERT INTO {source} VALUES ({low}, 'a'), ({low + 1}, 'b')")
        low += 2
    with pytest.raises(StreamingQueryException):
        _start(spark, "foreach", source, sink, name, tmp_path).awaitTermination()
    properties = spark.sql(f"SHOW TBLPROPERTIES {sink}").collect()
    assert any("pending-epoch" in str(row) for row in properties)
    before = {snapshot for snapshot, _, _ in _snapshots(spark, sink)}
    outcome = "ok"
    query = _start(spark, "table", source, sink, name, tmp_path)
    racer_outcome: list[str] = []
    racer = threading.Thread(target=_run_foreign, args=(spark, sink, delay, racer_outcome))
    racer.start()
    try:
        assert query.awaitTermination() is None
    except (RecoveryRequiredException, StreamingQueryException) as failed:
        outcome = f"{type(failed).__name__}: {failed}"
    finally:
        racer.join()
    if racer_outcome != ["ok"]:
        return None
    if outcome != "ok":
        assert "append fence" in outcome or "recovery required for query" in outcome
    if not _stray_landed(before, _snapshots(spark, sink)):
        return None
    assert _delete_effect(spark, sink)
    assert re.search(r"[fdoc]+[ST]", _shape(spark, sink)) is None
    return _Raced(source, sink, name, outcome)


def _until_landed(spark: ReparkSession, tmp_path: Path, tag: str, seed: int) -> _Raced:
    delays = [random.Random(seed + attempt).uniform(0.0, 0.08) for attempt in range(_ATTEMPTS)]
    for attempt, delay in enumerate(delays):
        raced = _race_one(spark, tmp_path, tag, delay, attempt)
        if raced is not None:
            return raced
    pytest.fail(f"the foreign delete never landed during the toTable run in {_ATTEMPTS} attempts")


@pytest.mark.parametrize("door", ["foreach", "table"])
@pytest.mark.parametrize("expire", [False, True])
def test_a_mark_only_name_never_restarts_silently_after_a_raced_table_run(
    spark: ReparkSession, tmp_path: Path, door: str, expire: bool
) -> None:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.md")
    raced = _until_landed(spark, tmp_path, f"{door}{expire}", 20261011)
    if expire:
        spark.sql(
            f"CALL sc.system.expire_snapshots(table => '{raced.sink[3:]}', "
            "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)"
        )
    low = max(row[0] for row in spark.sql(f"SELECT id FROM {raced.source}").collect()) + 1
    spark.sql(f"INSERT INTO {raced.source} VALUES ({low}, 'a'), ({low + 1}, 'b')")
    with pytest.raises(RecoveryRequiredException) as excinfo:
        _start(spark, door, raced.source, raced.sink, raced.name, tmp_path).awaitTermination()
    text = str(excinfo.value).split(" SQLSTATE")[0]
    assert "recovery required for query" in text
