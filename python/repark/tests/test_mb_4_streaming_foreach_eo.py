import json
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

import pytest

from repark import _native
from repark.errors import RecoveryRequiredException, StreamingQueryException
from repark.spark.dataframe import DataFrame
from repark.spark.functions import col
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming.query import StreamingQuery

_SOURCE = "sc.eo.src"
_SINK = "sc.eo.snk"
_SIDE = "sc.eo.side"
_TWICE = "REPARK_MICROBATCH.SINK_COMMITTED_TWICE"
_UNSTAMPED_WRITE = "REPARK_MICROBATCH.UNSTAMPED_SINK_WRITE"

_KILL_HARNESS = textwrap.dedent(
    """
    import glob, json, os, re, sys, threading
    from repark import _native
    from repark.spark.session.session_core import ReparkSession

    root, mode, arg = sys.argv[1], sys.argv[2], int(sys.argv[3])
    spark = ReparkSession.builder.appName("mb4-eo-kill").getOrCreate()
    _native._streaming_tests_allow_local_catalog(spark._ensure_alive())
    spark.register_memory_catalog("sc", root + "/wh")
    spark.sql("CREATE NAMESPACE sc.eo")

    def newest(table):
        files = glob.glob(f"{root}/wh/eo/{table}/metadata/*.metadata.json")
        def version(path):
            name = os.path.basename(path)
            found = re.match(r"v(\\d+)\\.", name) or re.match(r"(\\d+)-", name)
            return int(found.group(1))
        def whole(path):
            try:
                with open(path, encoding="utf-8") as handle:
                    json.load(handle)
            except ValueError:
                return False
            return True
        return max((path for path in files if whole(path)), key=version)

    def metadata_files():
        return len(glob.glob(f"{root}/wh/eo/snk/metadata/*.metadata.json"))

    if mode == "init":
        spark.sql("CREATE TABLE sc.eo.src (id BIGINT, k STRING)")
        spark.sql("CREATE TABLE sc.eo.snk (id BIGINT, k STRING)")
        for commit in range(arg):
            low = commit * 4
            values = ",".join(f"({i},'k{i % 3}')" for i in range(low, low + 4))
            spark.sql("INSERT INTO sc.eo.src VALUES " + values)
        spark.stop()
        sys.exit(0)

    for table in ("src", "snk"):
        spark.sql(
            f"CALL sc.system.register_table(table => 'eo.{table}', "
            f"metadata_file => '{newest(table)}')"
        )

    def report():
        source = sorted(row[0] for row in spark.table("sc.eo.src").collect())
        sink = sorted(row[0] for row in spark.table("sc.eo.snk").collect())
        summaries = [
            row[0] for row in spark.sql("SELECT summary FROM sc.eo.snk.snapshots").collect()
        ]
        epochs = sorted(
            int(summary["repark.cdc.epoch"])
            for summary in summaries
            if "repark.cdc.epoch" in summary
        )
        answer = {"source": source, "sink": sink, "epochs": epochs}
        print("REPORT " + json.dumps(answer), flush=True)

    if mode == "report":
        report()
        spark.stop()
        sys.exit(0)

    def body(frame, epoch):
        frame.writeTo("sc.eo.snk").append()
        if mode == "exit_after_write" and epoch == arg:
            os._exit(42)

    def watch(base):
        while metadata_files() < base + arg + 1:
            pass
        os._exit(43)

    if mode == "kill_on_commit":
        threading.Thread(target=watch, args=(metadata_files(),), daemon=True).start()
    if mode == "kill_after_ms":
        threading.Timer(arg / 1000.0, lambda: os._exit(44)).start()
    query = (
        spark.readStream.option("streaming-max-files-per-micro-batch", "1")
        .table("sc.eo.src")
        .writeStream.foreachBatch(body)
        .option("repark.cdc.sink", "sc.eo.snk")
        .option("checkpointLocation", root + "/ck")
        .queryName("eo")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(120)
    report()
    spark.stop()
    os._exit(0)
    """
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-foreach-eo").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    session.stop()


@pytest.fixture
def tables(spark: ReparkSession, tmp_path: Path) -> Path:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.eo")
    for table in (_SOURCE, _SINK, _SIDE):
        spark.sql(f"CREATE TABLE {table} (id BIGINT, k STRING)")
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
        .queryName("eo")
        .trigger(availableNow=True)
        .start()
    )


def _ids(spark: ReparkSession, table: str) -> list[int]:
    return sorted(row[0] for row in spark.sql(f"SELECT id FROM {table}").collect())


def _log(spark: ReparkSession, table: str) -> list[tuple[str, str | None, str | None]]:
    rows = spark.sql(
        f"SELECT operation, summary FROM {table}.snapshots ORDER BY committed_at"
    ).collect()
    return [
        (
            row.operation,
            dict(row.summary).get("repark.cdc.epoch"),
            dict(row.summary).get("added-records"),
        )
        for row in rows
    ]


def _failure(query: StreamingQuery) -> StreamingQueryException:
    with pytest.raises(StreamingQueryException) as excinfo:
        query.awaitTermination()
    return excinfo.value


class _Body:
    def __init__(self, spark: ReparkSession, shape: str, raise_at: int | None = None) -> None:
        self.spark = spark
        self.shape = shape
        self.raise_at = raise_at
        self.seen: list[int] = []
        self.errors: list[str] = []

    def __call__(self, frame: DataFrame, batch_id: int) -> None:
        self.seen.append(batch_id)
        getattr(self, f"_{self.shape}")(frame)
        if self.raise_at == batch_id:
            self.raise_at = None
            raise RuntimeError("mb4 body failed after its sink write")

    def _append(self, frame: DataFrame) -> None:
        frame.writeTo(_SINK).append()

    def _insert_into(self, frame: DataFrame) -> None:
        frame.createOrReplaceTempView("eo_batch")
        self.spark.sql(f"INSERT INTO {_SINK} SELECT * FROM eo_batch")

    def _append_twice(self, frame: DataFrame) -> None:
        frame.writeTo(_SINK).append()
        frame.writeTo(_SINK).append()

    def _merge(self, frame: DataFrame) -> None:
        frame.createOrReplaceTempView("eo_batch")
        self.spark.sql(
            f"MERGE INTO {_SINK} t USING eo_batch s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET k = s.k WHEN NOT MATCHED THEN INSERT *"
        )

    def _overwrite(self, frame: DataFrame) -> None:
        frame.writeTo(_SINK).overwritePartitions()

    def _insert_overwrite(self, frame: DataFrame) -> None:
        frame.createOrReplaceTempView("eo_batch")
        self.spark.sql(f"INSERT OVERWRITE {_SINK} SELECT * FROM eo_batch")

    def _alter(self, frame: DataFrame) -> None:
        self.spark.sql(f"ALTER TABLE {_SINK} SET TBLPROPERTIES ('eo.touched' = 'yes')")

    def _overwrite_where(self, frame: DataFrame) -> None:
        frame.writeTo(_SINK).overwrite(col("id") == 90)

    def _create_or_replace(self, frame: DataFrame) -> None:
        frame.writeTo(_SINK).createOrReplace()

    def _truncate(self, frame: DataFrame) -> None:
        self.spark.sql(f"TRUNCATE TABLE {_SINK}")

    def _delete_all(self, frame: DataFrame) -> None:
        self.spark.sql(f"DELETE FROM {_SINK}")

    def _delete_no_match(self, frame: DataFrame) -> None:
        self.spark.sql(f"DELETE FROM {_SINK} WHERE id = 12345")

    def _rollback(self, frame: DataFrame) -> None:
        first = self.spark.sql(
            f"SELECT snapshot_id FROM {_SINK}.snapshots ORDER BY committed_at"
        ).collect()[0][0]
        self.spark.sql(f"CALL sc.system.rollback_to_snapshot('eo.snk', {first})")

    def _expire(self, frame: DataFrame) -> None:
        self.spark.sql(
            "CALL sc.system.expire_snapshots(table => 'eo.snk', "
            "older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1)"
        )

    def _rewrite(self, frame: DataFrame) -> None:
        self.spark.sql(
            "CALL sc.system.rewrite_data_files(table => 'eo.snk', "
            "options => map('rewrite-all', 'true'))"
        )

    def _update(self, frame: DataFrame) -> None:
        self.spark.sql(f"UPDATE {_SINK} SET k = 'seen' WHERE id = 90")

    def _delete_row(self, frame: DataFrame) -> None:
        self.spark.sql(f"DELETE FROM {_SINK} WHERE id = {93 - len(self.seen)}")

    def _nothing(self, frame: DataFrame) -> None:
        frame.count()

    def _side(self, frame: DataFrame) -> None:
        frame.writeTo(_SIDE).append()
        frame.writeTo(_SINK).append()

    def _swallowed_second(self, frame: DataFrame) -> None:
        frame.writeTo(_SINK).append()
        try:
            frame.writeTo(_SINK).append()
        except Exception as error:
            self.errors.append(str(error))

    def _other_thread(self, frame: DataFrame) -> None:
        rows = [tuple(row) for row in frame.collect()]
        values = ", ".join(f"({row[0]}, '{row[1]}')" for row in rows)
        writer = threading.Thread(
            target=self.spark.sql, args=(f"INSERT INTO {_SINK} VALUES {values}",)
        )
        writer.start()
        writer.join()


def test_body_append_carries_the_epoch_stamp_in_its_own_snapshot(
    spark: ReparkSession, tables: Path
) -> None:
    query = _start(spark, _Body(spark, "append"), tables)
    assert query.awaitTermination() is None
    assert _ids(spark, _SINK) == [1, 2, 3]
    assert _log(spark, _SINK) == [("append", "0", "2"), ("append", "1", "1")]


def test_body_insert_into_carries_the_epoch_stamp(spark: ReparkSession, tables: Path) -> None:
    query = _start(spark, _Body(spark, "insert_into"), tables)
    assert query.awaitTermination() is None
    assert _ids(spark, _SINK) == [1, 2, 3]
    assert _log(spark, _SINK) == [("append", "0", "2"), ("append", "1", "1")]


def test_write_then_raise_restarts_without_a_duplicate(spark: ReparkSession, tables: Path) -> None:
    body = _Body(spark, "append", raise_at=0)
    failure = _failure(_start(spark, body, tables))
    assert "mb4 body failed after its sink write" in str(failure)
    assert _ids(spark, _SINK) == [1, 2]
    assert _log(spark, _SINK) == [("append", "0", "2")]
    restart = _start(spark, body, tables)
    assert restart.awaitTermination() is None
    assert body.seen == [0, 1]
    assert _ids(spark, _SINK) == [1, 2, 3]
    assert _log(spark, _SINK) == [("append", "0", "2"), ("append", "1", "1")]


def test_second_append_in_one_epoch_refuses_mbe13(spark: ReparkSession, tables: Path) -> None:
    failure = _failure(_start(spark, _Body(spark, "append_twice"), tables))
    assert failure.getCondition() == "STREAM_FAILED"
    assert f"[{_TWICE}] epoch 0 already stamped the sink" in str(failure)
    assert _ids(spark, _SINK) == [1, 2]
    assert _log(spark, _SINK) == [("append", "0", "2")]


def test_swallowed_second_append_leaves_one_commit_per_epoch(
    spark: ReparkSession, tables: Path
) -> None:
    body = _Body(spark, "swallowed_second")
    query = _start(spark, body, tables)
    assert query.awaitTermination() is None
    assert len(body.errors) == 2
    assert all("already stamped the sink" in error for error in body.errors)
    assert _ids(spark, _SINK) == [1, 2, 3]
    assert _log(spark, _SINK) == [("append", "0", "2"), ("append", "1", "1")]


def test_merge_body_is_stamped_under_serializable_isolation(
    spark: ReparkSession, tables: Path
) -> None:
    spark.sql(
        f"ALTER TABLE {_SINK} SET TBLPROPERTIES ('write.merge.isolation-level' = 'serializable')"
    )
    body = _Body(spark, "merge", raise_at=1)
    failure = _failure(_start(spark, body, tables))
    assert "mb4 body failed after its sink write" in str(failure)
    restart = _start(spark, body, tables)
    assert restart.awaitTermination() is None
    assert body.seen == [0, 1]
    assert _ids(spark, _SINK) == [1, 2, 3]
    assert [epoch for _, epoch, _ in _log(spark, _SINK)] == ["0", "1"]


_UNSTAMPABLE = [
    "overwrite",
    "insert_overwrite",
    "overwrite_where",
    "create_or_replace",
    "truncate",
    "delete_all",
    "delete_no_match",
    "alter",
    "rollback",
    "expire",
    "rewrite",
]


@pytest.mark.parametrize("shape", _UNSTAMPABLE)
def test_unstampable_sink_write_refuses_before_it_commits(
    spark: ReparkSession, tables: Path, shape: str
) -> None:
    spark.sql(f"INSERT INTO {_SINK} VALUES (90, 'kept')")
    spark.sql(f"INSERT INTO {_SINK} VALUES (91, 'kept')")
    before = _log(spark, _SINK)
    properties = spark.sql(f"SHOW TBLPROPERTIES {_SINK}").collect()
    failure = _failure(_start(spark, _Body(spark, shape), tables))
    assert failure.getCondition() == "STREAM_FAILED"
    assert f"[{_UNSTAMPED_WRITE}] epoch 0: this commit to the declared sink" in str(failure)
    assert _ids(spark, _SINK) == [90, 91]
    assert _log(spark, _SINK) == before
    assert spark.sql(f"SHOW TBLPROPERTIES {_SINK}").collect() == properties


@pytest.mark.parametrize(
    ("shape", "level", "rows"),
    [
        ("update", "write.update.isolation-level", [[90, "seen"], [91, "kept"], [92, "kept"]]),
        ("delete_row", "write.delete.isolation-level", [[90, "kept"]]),
    ],
)
def test_row_level_statement_is_stamped_under_serializable_isolation(
    spark: ReparkSession, tables: Path, shape: str, level: str, rows: list[list[object]]
) -> None:
    spark.sql(f"ALTER TABLE {_SINK} SET TBLPROPERTIES ('{level}' = 'serializable')")
    spark.sql(f"INSERT INTO {_SINK} VALUES (90, 'kept'), (91, 'kept'), (92, 'kept')")
    query = _start(spark, _Body(spark, shape), tables)
    assert query.awaitTermination() is None
    found = sorted(list(row) for row in spark.sql(f"SELECT * FROM {_SINK}").collect())
    assert found == rows
    assert [(operation, epoch) for operation, epoch, _ in _log(spark, _SINK)] == [
        ("append", None),
        ("overwrite", "0"),
        ("overwrite", "1"),
    ]


def test_body_without_a_sink_write_gets_the_stamp_only_commit(
    spark: ReparkSession, tables: Path
) -> None:
    query = _start(spark, _Body(spark, "nothing"), tables)
    assert query.awaitTermination() is None
    assert _ids(spark, _SINK) == []
    assert _log(spark, _SINK) == [("append", "0", None), ("append", "1", None)]


def test_second_table_is_a_side_output_and_the_sink_stays_exact(
    spark: ReparkSession, tables: Path
) -> None:
    body = _Body(spark, "side", raise_at=0)
    _failure(_start(spark, body, tables))
    restart = _start(spark, body, tables)
    assert restart.awaitTermination() is None
    assert _ids(spark, _SINK) == [1, 2, 3]
    assert _ids(spark, _SIDE) == [1, 2, 3]
    assert [epoch for _, epoch, _ in _log(spark, _SIDE)] == [None, None]
    assert [epoch for _, epoch, _ in _log(spark, _SINK)] == ["0", "1"]


def test_sink_write_from_another_thread_ends_recovery_required(
    spark: ReparkSession, tables: Path
) -> None:
    query = _start(spark, _Body(spark, "other_thread"), tables)
    with pytest.raises(RecoveryRequiredException) as excinfo:
        query.awaitTermination()
    assert "without a stamp; a commit bypassed the batch scope" in str(excinfo.value)
    assert _ids(spark, _SINK) == [1, 2]
    assert _log(spark, _SINK) == [("append", None, "2")]
    with pytest.raises(RecoveryRequiredException):
        spark.stop()


def _harness(tmp_path: Path, mode: str, arg: int) -> tuple[int, dict[str, list[int]] | None]:
    script = tmp_path / "eo_kill_harness.py"
    if not script.exists():
        script.write_text(_KILL_HARNESS, encoding="utf-8")
    done = subprocess.run(
        [sys.executable, str(script), str(tmp_path), mode, str(arg)],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    reports = [line for line in done.stdout.splitlines() if line.startswith("REPORT ")]
    return done.returncode, json.loads(reports[-1][7:]) if reports else None


def _assert_exact(report: dict[str, list[int]] | None) -> None:
    assert report is not None
    assert report["sink"] == report["source"]
    assert report["epochs"] == list(range(len(report["epochs"])))


def test_exit_after_the_sink_write_restarts_without_a_duplicate(tmp_path: Path) -> None:
    assert _harness(tmp_path, "init", 6)[0] == 0
    assert _harness(tmp_path, "exit_after_write", 2)[0] == 42
    code, report = _harness(tmp_path, "run", 0)
    assert code == 0
    _assert_exact(report)
    assert len(report["sink"]) == 24


def test_kills_at_sink_commits_restart_without_a_duplicate(tmp_path: Path) -> None:
    assert _harness(tmp_path, "init", 12)[0] == 0
    for commits in (0, 1, 0, 2, 1):
        assert _harness(tmp_path, "kill_on_commit", commits)[0] == 43
    code, report = _harness(tmp_path, "run", 0)
    assert code == 0
    _assert_exact(report)
    assert len(report["sink"]) == 48


def test_random_kills_restart_without_a_duplicate(tmp_path: Path) -> None:
    assert _harness(tmp_path, "init", 16)[0] == 0
    for delay in (350, 900, 520, 1300, 700, 1100):
        assert _harness(tmp_path, "kill_after_ms", delay)[0] in (0, 44)
    code, report = _harness(tmp_path, "run", 0)
    assert code == 0
    _assert_exact(report)
    assert len(report["sink"]) == 64
