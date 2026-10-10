from pathlib import Path

import pytest

from repark import _native
from repark.errors import PySparkException, StreamingQueryException
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession

_SOURCE = "sc.gt.src"
_SINK = "sc.gt.snk"
_QUERY_ID_KEY = "repark.cdc.query-id"
_FORGED = "11111111-2222-3333-4444-555555555555"
_CONF = f"spark.sql.iceberg.snapshot-property.{_QUERY_ID_KEY}"
_RESERVED = f"snapshot property {_QUERY_ID_KEY} is reserved for a streaming query's commit stamp"
_NESTED_NS = (
    "Cannot write incompatible data for the table `sc`.`gt`.`snk`: Cannot safely cast "
    '`st`.`v` to "TIMESTAMP_NS". A nested timestamp_ns leaf is not writable yet: omit the '
    "column `st` or supply NULL for it."
)
_NANOSECOND = "CAST('2024-01-01 00:00:00.123456789' AS TIMESTAMP_NS)"
_ENC_1 = "carries property 'encryption.key-id'"


@pytest.fixture
def spark() -> ReparkSession:
    session = (
        ReparkSession.builder.appName("pytest-mb-4-streaming-sink-gates")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    session.stop()


@pytest.fixture
def tables(spark: ReparkSession, tmp_path: Path) -> Path:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.gt")
    spark.sql(f"CREATE TABLE {_SOURCE} (id BIGINT, k STRING)")
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (1, 'a'), (2, 'b')")
    return tmp_path


def _files(root: Path) -> list[str]:
    table = root / "wh" / "gt" / "snk"
    return sorted(str(path.relative_to(table)) for path in table.rglob("*") if path.is_file())


def _summaries(spark: ReparkSession) -> list[dict[str, str]]:
    rows = spark.sql(f"SELECT summary FROM {_SINK}.snapshots ORDER BY committed_at").collect()
    return [dict(row[0]) for row in rows]


def _offsets(spark: ReparkSession) -> list[str]:
    rows = spark.sql(f"SHOW TBLPROPERTIES {_SINK}").collect()
    return [row[1] for row in rows if row[0].startswith("repark.cdc.offsets.")]


def test_table_door_refuses_a_nested_timestamp_ns_leaf_as_the_batch_write_does(
    spark: ReparkSession, tables: Path
) -> None:
    spark.sql(
        f"CREATE TABLE {_SINK} (id BIGINT, st STRUCT<v: timestamp_ns, n: INT>) "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    projection = ["id", f"named_struct('v', {_NANOSECOND}, 'n', 1) AS st"]
    with pytest.raises(PySparkException) as batch:
        spark.table(_SOURCE).selectExpr(*projection).writeTo(_SINK).append()
    assert _NESTED_NS in str(batch.value)
    before = _files(tables)
    query = (
        spark.readStream.table(_SOURCE)
        .selectExpr(*projection)
        .writeStream.option("checkpointLocation", str(tables / "ck"))
        .queryName("gt")
        .trigger(availableNow=True)
        .toTable(_SINK)
    )
    with pytest.raises(StreamingQueryException) as stream:
        query.awaitTermination()
    assert _NESTED_NS in str(stream.value)
    assert _files(tables) == before
    assert _summaries(spark) == []


_STATEMENTS = [
    f"INSERT INTO {_SINK} VALUES (3, 'c')",
    f"INSERT INTO {_SINK} SELECT * FROM {_SOURCE}",
    f"INSERT OVERWRITE {_SINK} VALUES (3, 'c')",
    f"DELETE FROM {_SINK} WHERE id = 8",
    f"UPDATE {_SINK} SET k = 'z' WHERE id = 8",
    f"MERGE INTO {_SINK} d USING {_SOURCE} s ON d.id = s.id "
    "WHEN MATCHED THEN UPDATE SET k = s.k WHEN NOT MATCHED THEN INSERT *",
    f"CREATE OR REPLACE TABLE {_SINK} AS SELECT 5 AS id, 'r' AS k",
]


@pytest.mark.parametrize("statement", _STATEMENTS)
def test_statement_under_a_reserved_summary_key_in_the_session_conf_is_refused_by_name(
    spark: ReparkSession, tables: Path, statement: str
) -> None:
    spark.sql(f"CREATE TABLE {_SINK} (id BIGINT, k STRING)")
    spark.sql(f"INSERT INTO {_SINK} VALUES (8, 'seed'), (9, 'seed')")
    before = _summaries(spark)
    spark.conf.set(_CONF, _FORGED)
    try:
        with pytest.raises(PySparkException) as excinfo:
            spark.sql(statement)
    finally:
        spark.conf.unset(_CONF)
    assert _RESERVED in str(excinfo.value)
    assert _summaries(spark) == before
    assert sorted(row[0] for row in spark.table(_SINK).collect()) == [8, 9]


def _append(frame: DataFrame) -> None:
    frame.writeTo(_SINK).option(f"snapshot-property.{_QUERY_ID_KEY}", _FORGED).append()


def _overwrite_partitions(frame: DataFrame) -> None:
    writer = frame.writeTo(_SINK).option(f"snapshot-property.{_QUERY_ID_KEY}", _FORGED)
    writer.overwritePartitions()


def _create_or_replace(frame: DataFrame) -> None:
    frame.writeTo(_SINK).option(f"snapshot-property.{_QUERY_ID_KEY}", _FORGED).createOrReplace()


def _save_as_table(frame: DataFrame) -> None:
    writer = frame.write.format("iceberg").mode("append")
    writer.option(f"snapshot-property.{_QUERY_ID_KEY}", _FORGED).saveAsTable(_SINK)


def _epoch_key(frame: DataFrame) -> None:
    frame.writeTo(_SINK).option("snapshot-property.REPARK.CDC.epoch", "4").append()


@pytest.mark.parametrize(
    ("write", "key"),
    [
        (_append, _QUERY_ID_KEY),
        (_overwrite_partitions, _QUERY_ID_KEY),
        (_create_or_replace, _QUERY_ID_KEY),
        (_save_as_table, _QUERY_ID_KEY),
        (_epoch_key, "REPARK.CDC.epoch"),
    ],
)
def test_writer_option_carrying_a_reserved_summary_key_is_refused_by_name(
    spark: ReparkSession, tables: Path, write: object, key: str
) -> None:
    spark.sql(f"CREATE TABLE {_SINK} (id BIGINT, k STRING)")
    spark.sql(f"INSERT INTO {_SINK} VALUES (8, 'seed'), (9, 'seed')")
    before = _summaries(spark)
    with pytest.raises(PySparkException) as excinfo:
        write(spark.table(_SOURCE))
    assert f"snapshot property {key} is reserved for a streaming query's commit stamp" in str(
        excinfo.value
    )
    assert _summaries(spark) == before
    assert sorted(row[0] for row in spark.table(_SINK).collect()) == [8, 9]


def test_a_forged_stamp_cannot_hide_a_foreign_write_from_the_restart(
    spark: ReparkSession, tables: Path
) -> None:
    spark.sql(f"CREATE TABLE {_SINK} (id BIGINT, k STRING)")

    def body(frame: DataFrame, batch_id: int) -> None:
        frame.writeTo(_SINK).append()

    def start() -> object:
        return (
            spark.readStream.table(_SOURCE)
            .writeStream.foreachBatch(body)
            .option("repark.cdc.sink", _SINK)
            .option("checkpointLocation", str(tables / "ck"))
            .queryName("gt")
            .trigger(availableNow=True)
            .start()
        )

    assert start().awaitTermination() is None
    with pytest.raises(PySparkException):
        _append(spark.createDataFrame([(70, "forged")], "id BIGINT, k STRING"))
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (3, 'c')")
    assert start().awaitTermination() is None
    assert sorted(row[0] for row in spark.table(_SINK).collect()) == [1, 2, 3]


@pytest.mark.parametrize("writes", ["append", "nothing"])
def test_foreach_door_refuses_a_keyed_sink_before_it_writes_its_mark(
    spark: ReparkSession, tables: Path, writes: str
) -> None:
    spark.sql(
        f"CREATE TABLE {_SINK} (id BIGINT, k STRING) TBLPROPERTIES ('encryption.key-id' = 'k1')"
    )
    before = _files(tables)
    calls: list[int] = []

    def body(frame: DataFrame, batch_id: int) -> None:
        calls.append(batch_id)
        if writes == "append":
            frame.writeTo(_SINK).append()

    query = (
        spark.readStream.table(_SOURCE)
        .writeStream.foreachBatch(body)
        .option("repark.cdc.sink", _SINK)
        .option("checkpointLocation", str(tables / "ck"))
        .queryName("gt")
        .trigger(availableNow=True)
        .start()
    )
    with pytest.raises(StreamingQueryException) as excinfo:
        query.awaitTermination()
    assert _ENC_1 in str(excinfo.value)
    assert "batch 0 failed" not in str(excinfo.value)
    assert calls == []
    assert _offsets(spark) == []
    assert _files(tables) == before
