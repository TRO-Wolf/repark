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

_FOREIGN = {
    "delete_delivered": "DELETE FROM {sink} WHERE id < 2",
    "dup_delivered": "INSERT INTO {sink} SELECT * FROM {sink} WHERE id < 2",
    "insert": "INSERT INTO {sink} VALUES (700000, 'stray')",
}
_ATTEMPTS = 6
_ROLL_BACK = re.compile(
    r"roll the sink back to snapshot (\d+) \(CALL system\.rollback_to_snapshot\)"
)
_SAME_NAME = "and start the query again."
_ABOVE = "Every start refuses while that snapshot sits above the newest stamped batch."


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-door-switch").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    try:
        session.stop()
    except RecoveryRequiredException:
        session.stop()


class _Append:
    def __init__(self, spark: ReparkSession, sink: str) -> None:
        self.spark = spark
        self.sink = sink

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        frame.writeTo(self.sink).append()


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
        return writer.foreachBatch(_Append(spark, sink)).option("repark.cdc.sink", sink).start()
    return writer.toTable(sink)


def _snapshots(spark: ReparkSession, sink: str) -> list[tuple[int, str, str | None]]:
    rows = spark.sql(f"SELECT snapshot_id, operation, summary FROM {sink}.snapshots").collect()
    return [
        (row.snapshot_id, row.operation, dict(row.summary).get("repark.cdc.epoch")) for row in rows
    ]


def _stray_landed(before: set[int], after: list[tuple[int, str, str | None]]) -> bool:
    return any(epoch is None for snapshot, _, epoch in after if snapshot not in before)


def _foreign_effect(spark: ReparkSession, sink: str, kind: str) -> bool:
    ids = [row[0] for row in spark.sql(f"SELECT id FROM {sink}").collect()]
    if kind == "insert":
        return 700000 in ids
    if kind == "dup_delivered":
        return len(ids) != len(set(ids))
    return len({0, 1} - set(ids)) > 0


def _run_foreign(
    spark: ReparkSession, sink: str, kind: str, delay: float, outcome: list[str]
) -> None:
    time.sleep(delay)
    try:
        spark.sql(_FOREIGN[kind].format(sink=sink))
    except Exception as failed:
        outcome.append(f"{type(failed).__name__}: {failed}")
    else:
        outcome.append("ok")


def _race_one(
    spark: ReparkSession,
    tmp_path: Path,
    tag: str,
    kind: str,
    delay: float,
    attempt: int,
) -> _Raced | None:
    source = f"sc.ds.src{tag}{attempt}"
    sink = f"sc.ds.snk{tag}{attempt}"
    name = f"dw{tag}{attempt}"
    spark.sql(f"CREATE TABLE {sink} (id BIGINT, k STRING)")
    spark.sql(f"CREATE TABLE {source} (id BIGINT, k STRING)")
    low = 0
    for _ in range(2):
        spark.sql(f"INSERT INTO {source} VALUES ({low}, 'a'), ({low + 1}, 'b')")
        low += 2
    first = _start(spark, "foreach", source, sink, name, tmp_path)
    assert first.awaitTermination() is None
    for _ in range(3):
        spark.sql(f"INSERT INTO {source} VALUES ({low}, 'a'), ({low + 1}, 'b')")
        low += 2
    before = {snapshot for snapshot, _, _ in _snapshots(spark, sink)}
    outcome = "ok"
    query = _start(spark, "table", source, sink, name, tmp_path)
    racer_outcome: list[str] = []
    racer = threading.Thread(target=_run_foreign, args=(spark, sink, kind, delay, racer_outcome))
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
    assert _foreign_effect(spark, sink, kind)
    return _Raced(source, sink, name, outcome)


def _until_landed(spark: ReparkSession, tmp_path: Path, tag: str, kind: str, seed: int) -> _Raced:
    delays = [random.Random(seed + attempt).uniform(0.0, 0.05) for attempt in range(_ATTEMPTS)]
    for attempt, delay in enumerate(delays):
        raced = _race_one(spark, tmp_path, tag, kind, delay, attempt)
        if raced is not None:
            return raced
    pytest.fail(f"the foreign {kind} never landed during the toTable run in {_ATTEMPTS} attempts")


def _ids(spark: ReparkSession, table: str) -> list[int]:
    return sorted(row[0] for row in spark.sql(f"SELECT id FROM {table}").collect())


@pytest.mark.parametrize("kind", sorted(_FOREIGN))
@pytest.mark.parametrize("door", ["foreach", "table"])
def test_a_raced_totable_batch_under_a_foreach_name_is_loud_and_its_remedy_is_exact(
    spark: ReparkSession, tmp_path: Path, kind: str, door: str
) -> None:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.ds")
    raced = _until_landed(spark, tmp_path, f"{kind}{door}a", kind, 20261011)
    with pytest.raises(RecoveryRequiredException) as excinfo:
        _start(spark, door, raced.source, raced.sink, raced.name, tmp_path).awaitTermination()
    text = str(excinfo.value).split(" SQLSTATE")[0]
    assert _ABOVE in text
    assert _SAME_NAME in text
    target = int(_ROLL_BACK.search(text).group(1))
    short = raced.sink[3:]
    spark.sql(f"CALL sc.system.rollback_to_snapshot('{short}', {target})")
    rerun = _start(spark, door, raced.source, raced.sink, raced.name, tmp_path)
    assert rerun.awaitTermination() is None, raced.outcome
    assert _ids(spark, raced.sink) == _ids(spark, raced.source)


@pytest.mark.parametrize("kind", sorted(_FOREIGN))
@pytest.mark.parametrize("door", ["foreach", "table"])
def test_a_raced_totable_batch_under_a_foreach_name_is_loud_after_expiry_to_one(
    spark: ReparkSession, tmp_path: Path, kind: str, door: str
) -> None:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.ds")
    raced = _until_landed(spark, tmp_path, f"{kind}{door}b", kind, 20261012)
    short = raced.sink[3:]
    spark.sql(
        f"CALL sc.system.expire_snapshots(table => '{short}', "
        "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)"
    )
    with pytest.raises(RecoveryRequiredException) as excinfo:
        _start(spark, door, raced.source, raced.sink, raced.name, tmp_path).awaitTermination()
    text = str(excinfo.value).split(" SQLSTATE")[0]
    assert "recovery required for query" in text
    assert "stamped snapshot expired" in text or "without a stamp" in text
