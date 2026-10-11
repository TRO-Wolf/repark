import re
import threading
from pathlib import Path

import pytest

from repark import _native
from repark.errors import RecoveryRequiredException
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming.query import StreamingQuery

_SOURCE = "sc.rm.src"
_SINK = "sc.rm.snk"
_START_AFTER = "repark.cdc.start-after-snapshot-id"
_ROLL_BACK = re.compile(
    r"roll the sink back to snapshot (\d+) \(CALL system\.rollback_to_snapshot\)"
)
_KEEP = re.compile(
    r"To keep its rows, start the query under a new name"
    rf"(?: with the reader option {re.escape(_START_AFTER)}=(\d+))?[.;]"
)
_SAME_NAME = "and start the query again."
_ABOVE = "Every start refuses while that snapshot sits above the newest stamped batch."
_EMPTY_START = (
    "The query first started on an empty sink, so there is no snapshot to roll back to: "
    "discarding its rows is a repair for the operator."
)
_NO_NEW_NAME = (
    "A new query name is not offered: the newest stamped batch ends inside a source snapshot, "
    "so no start-after position continues exactly from it."
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-remedies").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    try:
        session.stop()
    except RecoveryRequiredException:
        session.stop()


def _tables(spark: ReparkSession, root: Path, seeded: bool) -> Path:
    spark.register_memory_catalog("sc", str(root / "wh"))
    spark.sql("CREATE NAMESPACE sc.rm")
    spark.sql(f"CREATE TABLE {_SOURCE} (id BIGINT, k STRING)")
    spark.sql(f"CREATE TABLE {_SINK} (id BIGINT, k STRING)")
    if seeded:
        spark.sql(f"INSERT INTO {_SINK} VALUES (90, 'seed')")
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (1, 'a'), (2, 'b')")
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (3, 'c')")
    return root


@pytest.fixture
def tables(spark: ReparkSession, tmp_path: Path) -> Path:
    return _tables(spark, tmp_path, seeded=True)


@pytest.fixture
def empty_sink(spark: ReparkSession, tmp_path: Path) -> Path:
    return _tables(spark, tmp_path, seeded=False)


class _Body:
    def __init__(self, spark: ReparkSession, stray: str | None = None, at: int = 1) -> None:
        self.spark = spark
        self.stray = stray
        self.at = at
        self.calls = 0

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        self.calls += 1
        if self.stray is not None and batch_id == self.at:
            writer = threading.Thread(target=self._write_twice, args=(frame,))
            writer.start()
            writer.join()
            if self.stray == "alone":
                raise ValueError("mb4 body failed after its helper thread wrote the sink")
        frame.writeTo(_SINK).append()

    def _write_twice(self, frame: DataFrame) -> None:
        frame.writeTo(_SINK).append()


def _start(
    spark: ReparkSession,
    body: object,
    root: Path,
    name: str = "rm",
    start_after: str | None = None,
) -> StreamingQuery:
    reader = spark.readStream.option("streaming-max-files-per-micro-batch", "1")
    if start_after is not None:
        reader = reader.option(_START_AFTER, start_after)
    return (
        reader.table(_SOURCE)
        .writeStream.foreachBatch(body)
        .option("repark.cdc.sink", _SINK)
        .option("checkpointLocation", str(root / "ck"))
        .queryName(name)
        .trigger(availableNow=True)
        .start()
    )


def _ids(spark: ReparkSession) -> list[int]:
    return sorted(row[0] for row in spark.sql(f"SELECT id FROM {_SINK}").collect())


def _snapshots(spark: ReparkSession, table: str) -> list[tuple[int, str | None]]:
    rows = spark.sql(
        f"SELECT snapshot_id, summary FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
    return [(row.snapshot_id, dict(row.summary).get("repark.cdc.epoch")) for row in rows]


def _stamped(spark: ReparkSession, epoch: int) -> int:
    return next(snapshot for snapshot, at in _snapshots(spark, _SINK) if at == str(epoch))


def _refused(spark: ReparkSession, root: Path, stray: int) -> str:
    texts = []
    for _ in range(2):
        rows = _ids(spark)
        body = _Body(spark)
        with pytest.raises(RecoveryRequiredException) as excinfo:
            _start(spark, body, root).awaitTermination()
        assert body.calls == 0
        assert _ids(spark) == rows
        texts.append(str(excinfo.value).split(" SQLSTATE")[0])
    assert texts[0] == texts[1]
    assert f"sink advanced to snapshot {stray} (append) without a stamp" in texts[0]
    assert "Its rows are in the sink." in texts[0]
    return texts[0]


def _roll_back(spark: ReparkSession, text: str) -> int:
    target = int(_ROLL_BACK.search(text).group(1))
    spark.sql(f"CALL sc.system.rollback_to_snapshot('rm.snk', {target})")
    return target


def _foreign_insert_after_two_batches(spark: ReparkSession, root: Path) -> int:
    assert _start(spark, _Body(spark), root).awaitTermination() is None
    spark.sql(f"INSERT INTO {_SINK} VALUES (99, 'foreign')")
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (4, 'd')")
    return _snapshots(spark, _SINK)[-1][0]


def test_stray_above_the_newest_stamp_and_the_rollback_remedy_is_exact(
    spark: ReparkSession, tables: Path
) -> None:
    stray = _foreign_insert_after_two_batches(spark, tables)
    text = _refused(spark, tables, stray)
    assert _ABOVE in text
    assert _SAME_NAME in text
    assert _roll_back(spark, text) == _stamped(spark, 1)
    resumed = _Body(spark)
    assert _start(spark, resumed, tables).awaitTermination() is None
    assert resumed.calls == 1
    assert _ids(spark) == [1, 2, 3, 4, 90]


def test_stray_above_the_newest_stamp_and_the_new_name_remedy_is_exact(
    spark: ReparkSession, tables: Path
) -> None:
    stray = _foreign_insert_after_two_batches(spark, tables)
    text = _refused(spark, tables, stray)
    keep = _KEEP.search(text)
    assert keep.group(1) == str(_snapshots(spark, _SOURCE)[1][0])
    assert "a new name without that option delivers every stamped batch again" in text
    renamed = _Body(spark)
    restart = _start(spark, renamed, tables, name="rm2", start_after=keep.group(1))
    assert restart.awaitTermination() is None
    assert renamed.calls == 1
    assert _ids(spark) == [1, 2, 3, 4, 90, 99]


def _stray_before_any_stamp(spark: ReparkSession, root: Path) -> int:
    with pytest.raises(RecoveryRequiredException):
        _start(spark, _Body(spark, "alone", at=0), root).awaitTermination()
    snapshots = _snapshots(spark, _SINK)
    assert snapshots[-1][1] is None
    return snapshots[-1][0]


def test_stray_before_any_stamp_and_the_rollback_remedy_is_exact(
    spark: ReparkSession, tables: Path
) -> None:
    seed = _snapshots(spark, _SINK)[0][0]
    stray = _stray_before_any_stamp(spark, tables)
    text = _refused(spark, tables, stray)
    assert _ABOVE in text
    assert _SAME_NAME in text
    assert _roll_back(spark, text) == seed
    resumed = _Body(spark)
    assert _start(spark, resumed, tables).awaitTermination() is None
    assert resumed.calls == 2
    assert _ids(spark) == [1, 2, 3, 90]


def test_stray_before_any_stamp_and_the_new_name_remedy_is_exact(
    spark: ReparkSession, tables: Path
) -> None:
    stray = _stray_before_any_stamp(spark, tables)
    text = _refused(spark, tables, stray)
    assert _KEEP.search(text).group(1) is None
    renamed = _Body(spark)
    assert _start(spark, renamed, tables, name="rm2").awaitTermination() is None
    assert renamed.calls == 2
    assert _ids(spark) == [1, 1, 2, 2, 3, 90]


def test_stray_before_any_stamp_on_an_empty_sink_offers_only_the_new_name(
    spark: ReparkSession, empty_sink: Path
) -> None:
    stray = _stray_before_any_stamp(spark, empty_sink)
    text = _refused(spark, empty_sink, stray)
    assert _EMPTY_START in text
    assert _ROLL_BACK.search(text) is None
    assert _KEEP.search(text).group(1) is None
    renamed = _Body(spark)
    assert _start(spark, renamed, empty_sink, name="rm2").awaitTermination() is None
    assert renamed.calls == 2
    assert _ids(spark) == [1, 1, 2, 2, 3]


def test_a_batch_that_ends_inside_a_source_snapshot_is_not_offered_a_new_name(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.rm")
    spark.sql(f"CREATE TABLE {_SOURCE} (id BIGINT, k STRING) PARTITIONED BY (k)")
    spark.sql(f"CREATE TABLE {_SINK} (id BIGINT, k STRING)")
    spark.sql(f"INSERT INTO {_SINK} VALUES (90, 'seed')")
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    def first_batch_only(frame: DataFrame, batch_id: int) -> None:
        if batch_id == 1:
            raise ValueError("mb4 body stopped after its first batch")
        frame.writeTo(_SINK).append()

    with pytest.raises(Exception, match="mb4 body stopped after its first batch"):
        _start(spark, first_batch_only, tmp_path).awaitTermination()
    assert len(_ids(spark)) == 2
    spark.sql(f"INSERT INTO {_SINK} VALUES (99, 'foreign')")
    text = _refused(spark, tmp_path, _snapshots(spark, _SINK)[-1][0])
    assert _NO_NEW_NAME in text
    assert _KEEP.search(text) is None
    assert _roll_back(spark, text) == _stamped(spark, 0)
    resumed = _Body(spark)
    assert _start(spark, resumed, tmp_path).awaitTermination() is None
    assert resumed.calls == 2
    assert _ids(spark) == [1, 2, 3, 90]
