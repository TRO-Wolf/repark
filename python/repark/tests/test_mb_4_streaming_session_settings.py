from pathlib import Path

import pyarrow.parquet as pq
import pytest

from repark import _native
from repark.spark.dataframe import DataFrame
from repark.spark.session.session_core import ReparkSession
from repark.spark.streaming.query import StreamingQuery

_SOURCE = "sc.tz.src"
_ZONED = [
    "id",
    "CAST(ts AS STRING) AS s",
    "hour(ts) AS h",
    "CAST(ts AS DATE) AS d",
    "CAST(ts AS TIMESTAMP_NTZ) AS n",
    "date_format(ts, 'yyyy-MM-dd HH:mm') AS f",
]
_ZONED_DDL = "(id BIGINT, s STRING, h INT, d DATE, n TIMESTAMP_NTZ, f STRING)"
_WRAPPED = ["id", "CAST(2147483647 AS INT) + CAST(id AS INT) AS v"]
_WRAPPED_DDL = "(id BIGINT, v INT)"
_CASED = ["id AS ID", "CAST(id AS STRING) AS V"]
_CASED_DDL = "(id BIGINT, v STRING)"


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-mb-4-streaming-session-settings").getOrCreate()
    _native._streaming_tests_allow_local_catalog(session._ensure_alive())
    yield session
    session.stop()


@pytest.fixture
def tables(spark: ReparkSession, tmp_path: Path) -> Path:
    spark.register_memory_catalog("sc", str(tmp_path / "wh"))
    spark.sql("CREATE NAMESPACE sc.tz")
    spark.sql(f"CREATE TABLE {_SOURCE} (id BIGINT, ts TIMESTAMP)")
    return tmp_path


def _stored(root: Path, table: str) -> list[dict[str, object]]:
    files = sorted((root / "wh" / "tz" / table / "data").glob("**/*.parquet"))
    rows = [row for file in files for row in pq.read_table(file).to_pylist()]
    return sorted(rows, key=lambda row: row["id"])


def _stream(spark: ReparkSession, projection: list[str], root: Path, name: str) -> object:
    return (
        spark.readStream.table(_SOURCE)
        .selectExpr(*projection)
        .writeStream.option("checkpointLocation", str(root / f"ck-{name}"))
        .queryName(name)
        .trigger(availableNow=True)
    )


def _three_doors(
    spark: ReparkSession, root: Path, projection: list[str], ddl: str
) -> tuple[list[tuple[object, ...]], list[tuple[object, ...]]]:
    for table in ("batch", "st", "fb"):
        spark.sql(f"CREATE TABLE sc.tz.{table} {ddl}")
    spark.table(_SOURCE).selectExpr(*projection).writeTo("sc.tz.batch").append()
    table_door: StreamingQuery = _stream(spark, projection, root, "st").toTable("sc.tz.st")
    assert table_door.awaitTermination() is None
    collected: list[tuple[object, ...]] = []

    def body(frame: DataFrame, batch_id: int) -> None:
        collected.extend(tuple(row) for row in frame.collect())
        frame.writeTo("sc.tz.fb").append()

    foreach: StreamingQuery = (
        _stream(spark, projection, root, "fb")
        .foreachBatch(body)
        .option("repark.cdc.sink", "sc.tz.fb")
        .start()
    )
    assert foreach.awaitTermination() is None
    batch = [tuple(row) for row in spark.table("sc.tz.batch").collect()]
    return batch, collected


@pytest.mark.parametrize("zone", ["America/New_York", "Asia/Tokyo"])
@pytest.mark.parametrize("how", ["builder", "conf"])
def test_both_doors_store_what_the_batch_write_stores_under_a_session_zone(
    tmp_path: Path, zone: str, how: str
) -> None:
    builder = ReparkSession.builder.appName("pytest-mb-4-streaming-session-zone")
    if how == "builder":
        builder = builder.config("spark.sql.session.timeZone", zone)
    spark = builder.getOrCreate()
    try:
        if how == "conf":
            spark.conf.set("spark.sql.session.timeZone", zone)
        assert spark.conf.get("spark.sql.session.timeZone") == zone
        _native._streaming_tests_allow_local_catalog(spark._ensure_alive())
        spark.register_memory_catalog("sc", str(tmp_path / "wh"))
        spark.sql("CREATE NAMESPACE sc.tz")
        spark.sql(f"CREATE TABLE {_SOURCE} (id BIGINT, ts TIMESTAMP)")
        spark.sql(f"INSERT INTO {_SOURCE} VALUES (1, TIMESTAMP '2024-01-01 00:30:00')")
        batch, collected = _three_doors(spark, tmp_path, _ZONED, _ZONED_DDL)
    finally:
        spark.stop()
    stored = _stored(tmp_path, "batch")
    assert [(row["s"], row["h"], str(row["d"]), row["f"]) for row in stored] == [
        ("2024-01-01 00:30:00", 0, "2024-01-01", "2024-01-01 00:30")
    ]
    assert str(stored[0]["n"]) == "2024-01-01 00:30:00"
    assert _stored(tmp_path, "st") == stored
    assert _stored(tmp_path, "fb") == stored
    assert collected == batch


def test_both_doors_follow_ansi_mode_set_after_the_session_started(
    spark: ReparkSession, tables: Path
) -> None:
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (1, TIMESTAMP '2024-01-01 00:30:00')")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    batch, collected = _three_doors(spark, tables, _WRAPPED, _WRAPPED_DDL)
    stored = _stored(tables, "batch")
    assert stored == [{"id": 1, "v": -2147483648}]
    assert _stored(tables, "st") == stored
    assert _stored(tables, "fb") == stored
    assert collected == batch


@pytest.mark.parametrize("case_sensitive", ["true", "false"])
def test_both_doors_match_sink_columns_as_the_batch_write_does(
    spark: ReparkSession, tables: Path, case_sensitive: str
) -> None:
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (1, TIMESTAMP '2024-01-01 00:30:00')")
    spark.conf.set("spark.sql.caseSensitive", case_sensitive)
    batch, collected = _three_doors(spark, tables, _CASED, _CASED_DDL)
    stored = _stored(tables, "batch")
    assert stored == [{"id": 1, "v": "1"}]
    assert _stored(tables, "st") == stored
    assert _stored(tables, "fb") == stored
    assert collected == batch


def test_current_timestamp_in_a_streaming_plan_is_the_batch_time(
    spark: ReparkSession, tables: Path
) -> None:
    spark.sql(f"INSERT INTO {_SOURCE} VALUES (1, TIMESTAMP '2024-01-01 00:30:00')")
    projection = ["id", "unix_timestamp(current_timestamp()) AS v"]
    started = spark.sql("SELECT unix_timestamp(current_timestamp())").collect()[0][0]
    _three_doors(spark, tables, projection, "(id BIGINT, v BIGINT)")
    ended = spark.sql("SELECT unix_timestamp(current_timestamp())").collect()[0][0]
    for table in ("batch", "st", "fb"):
        stored = _stored(tables, table)
        assert len(stored) == 1
        assert started <= stored[0]["v"] <= ended
