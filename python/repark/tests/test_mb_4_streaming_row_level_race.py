import random
import threading
import time
from pathlib import Path

import pytest

from repark import _native
from repark.errors import RecoveryRequiredException
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession

_SERIALIZABLE = (
    "'write.merge.isolation-level' = 'serializable', "
    "'write.update.isolation-level' = 'serializable', "
    "'write.delete.isolation-level' = 'serializable'"
)
_SEED = ", ".join(f"({-number}, 'seed')" for number in range(1, 10))
_STATEMENTS = {
    "update": "UPDATE {sink} SET k = concat(k, 'u') WHERE id = -1",
    "delete": "DELETE FROM {sink} WHERE id = -({epoch} + 2)",
    "merge_update": (
        "MERGE INTO {sink} t USING (SELECT -1 AS id, 'm' AS k) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET k = concat(t.k, 'm')"
    ),
    "merge_delete": (
        "MERGE INTO {sink} t USING (SELECT -({epoch} + 2) AS id) s ON t.id = s.id "
        "WHEN MATCHED THEN DELETE"
    ),
}
_ATTEMPTS = 12


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-row-level-race").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    try:
        session.stop()
    except RecoveryRequiredException:
        session.stop()


class _Racing:
    def __init__(self, spark: ReparkSession, sink: str, statement: str, delay: float) -> None:
        self.spark = spark
        self.sink = sink
        self.statement = statement
        self.delay = delay

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        racer = threading.Thread(target=self._foreign, args=(batch_id,))
        racer.start()
        try:
            self.spark.sql(self.statement.format(sink=self.sink, epoch=batch_id))
        finally:
            racer.join()

    def _foreign(self, batch_id: int) -> None:
        time.sleep(self.delay)
        self.spark.sql(f"INSERT INTO {self.sink} VALUES ({700000 + batch_id}, 'stray')")


def _strays_under_a_stamp(spark: ReparkSession, sink: str) -> list[int]:
    rows = spark.sql(f"SELECT snapshot_id, parent_id, summary FROM {sink}.snapshots").collect()
    stamped = {row.snapshot_id for row in rows if "repark.cdc.epoch" in dict(row.summary)}
    born = {row.snapshot_id: dict(row.summary).get("mb4.seed") for row in rows}
    return [
        row.parent_id
        for row in rows
        if row.snapshot_id in stamped
        and row.parent_id is not None
        and row.parent_id not in stamped
        and born[row.parent_id] is None
    ]


@pytest.mark.parametrize("kind", sorted(_STATEMENTS))
def test_a_foreign_insert_racing_a_row_level_body_never_ends_under_its_stamp(
    spark: ReparkSession, tmp_path: Path, kind: str
) -> None:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.rr")
    spark.sql("CREATE TABLE sc.rr.src (id BIGINT, k STRING)")
    for commit in range(4):
        spark.sql(f"INSERT INTO sc.rr.src VALUES ({commit}, 'a')")
    delays = random.Random(20261010).sample([step * 0.004 for step in range(_ATTEMPTS)], _ATTEMPTS)
    under: dict[int, list[int]] = {}
    refused = 0
    for attempt, delay in enumerate(delays):
        sink = f"sc.rr.snk{attempt}"
        spark.sql(f"CREATE TABLE {sink} (id BIGINT, k STRING) TBLPROPERTIES ({_SERIALIZABLE})")
        spark.conf.set("spark.sql.iceberg.snapshot-property.mb4.seed", "yes")
        spark.sql(f"INSERT INTO {sink} VALUES {_SEED}")
        spark.conf.unset("spark.sql.iceberg.snapshot-property.mb4.seed")
        query = (
            spark.readStream.option("streaming-max-files-per-micro-batch", "1")
            .table("sc.rr.src")
            .writeStream.foreachBatch(_Racing(spark, sink, _STATEMENTS[kind], delay))
            .option("repark.cdc.sink", sink)
            .option("checkpointLocation", str(tmp_path / f"ck{attempt}"))
            .queryName(f"rr{attempt}")
            .trigger(availableNow=True)
            .start()
        )
        try:
            query.awaitTermination()
        except RecoveryRequiredException:
            refused += 1
        found = _strays_under_a_stamp(spark, sink)
        if found:
            under[attempt] = found
    assert under == {}
    assert refused == _ATTEMPTS
