"""Max over a view of a cast modulo column answers Spark's value and type.

pins: cast-view-agg-nullability-1/C-001, C-002, C-003, C-004
"""

from __future__ import annotations

import datetime
from pathlib import Path

import pyarrow as pa
import pytest

from repark.spark.dataframe import DataFrame
from repark.spark.session import ReparkSession
from repark.spark.types import LongType, TimestampType

VIEW_SQL = "SELECT CAST(946684800 + (id * 1577) % 3000000000 AS TIMESTAMP) AS ts FROM range(10)"
VIEW = "cast_view_agg_nullability_1"
FULL_EPOCH = 946698993
FULL_WALL_UTC = datetime.datetime(2000, 1, 1, 3, 56, 33, tzinfo=datetime.UTC)
FIRST_WALL_UTC = datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
MINIMAL_WALL_UTC = datetime.datetime(1970, 1, 1, 0, 0, 2, tzinfo=datetime.UTC)


@pytest.fixture
def spark() -> ReparkSession:
    """Per-test UTC session; plays with the autouse active-session isolate."""
    return (
        ReparkSession.builder.master("local[1]")
        .appName("test_cast_view_agg_nullability_1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _arrow_table(frame: DataFrame) -> pa.Table:
    table: pa.Table = frame.toArrow()
    return table


def test_full_repro_answers_spark_value_and_type(spark: ReparkSession) -> None:
    spark.sql(VIEW_SQL).createOrReplaceTempView(VIEW)
    frame = spark.sql(f"SELECT max(ts) AS m, CAST(max(ts) AS BIGINT) AS e FROM {VIEW}")
    table = _arrow_table(frame)
    assert table.schema.field("m").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("e").type == pa.int64()
    rows = table.to_pylist()
    assert rows[0]["e"] == FULL_EPOCH
    assert rows[0]["m"] == FULL_WALL_UTC
    assert isinstance(frame.schema["m"].dataType, TimestampType)
    assert frame.schema["m"].nullable is True


def test_minimal_modulo_cast_view_max_answers(spark: ReparkSession) -> None:
    spark.sql("SELECT CAST(id % 3 AS TIMESTAMP) AS ts FROM range(10)").createOrReplaceTempView(VIEW)
    table = _arrow_table(spark.sql(f"SELECT max(ts) AS m FROM {VIEW}"))
    assert table.schema.field("m").type == pa.timestamp("us", tz="UTC")
    assert table.to_pylist() == [{"m": MINIMAL_WALL_UTC}]


def test_min_and_count_over_the_column_answer(spark: ReparkSession) -> None:
    spark.sql(VIEW_SQL).createOrReplaceTempView(VIEW)
    min_table = _arrow_table(spark.sql(f"SELECT min(ts) AS m FROM {VIEW}"))
    assert min_table.to_pylist() == [{"m": FIRST_WALL_UTC}]
    count_table = _arrow_table(spark.sql(f"SELECT count(ts) AS c FROM {VIEW}"))
    assert count_table.schema.field("c").type == pa.int64()
    assert count_table.to_pylist() == [{"c": 10}]


def test_count_star_over_the_same_view_answers_ten(spark: ReparkSession) -> None:
    spark.sql(VIEW_SQL).createOrReplaceTempView(VIEW)
    table = _arrow_table(spark.sql(f"SELECT count(*) AS c FROM {VIEW}"))
    assert table.schema.field("c").type == pa.int64()
    assert table.to_pylist() == [{"c": 10}]


def test_plain_select_over_the_view_answers_ten_rows(spark: ReparkSession) -> None:
    spark.sql(VIEW_SQL).createOrReplaceTempView(VIEW)
    table = _arrow_table(spark.sql(f"SELECT ts FROM {VIEW} ORDER BY ts"))
    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    rows = table.to_pylist()
    assert len(rows) == 10
    assert rows[0] == {"ts": FIRST_WALL_UTC}
    assert rows[-1] == {"ts": FULL_WALL_UTC}


def test_view_column_stays_nullable_like_spark(spark: ReparkSession) -> None:
    spark.sql(VIEW_SQL).createOrReplaceTempView(VIEW)
    field = spark.table(VIEW).schema["ts"]
    assert isinstance(field.dataType, TimestampType)
    assert field.nullable is True


def test_count_star_over_plain_cast_and_plain_modulo_views(spark: ReparkSession) -> None:
    spark.sql("SELECT CAST(id AS TIMESTAMP) AS ts FROM range(10)").createOrReplaceTempView(VIEW)
    first = _arrow_table(spark.sql(f"SELECT count(*) AS c FROM {VIEW}")).to_pylist()
    spark.sql("SELECT (id * 2) % 7 AS x FROM range(10)").createOrReplaceTempView(VIEW)
    second = _arrow_table(spark.sql(f"SELECT count(*) AS c FROM {VIEW}")).to_pylist()
    assert first == [{"c": 10}]
    assert second == [{"c": 10}]


def test_parquet_roundtrip_max_matches_the_direct_answer(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql(VIEW_SQL).write.parquet(str(tmp_path / "view_parquet"))
    back = spark.read.parquet(str(tmp_path / "view_parquet"))
    table = _arrow_table(back.selectExpr("max(ts) AS m", "CAST(max(ts) AS BIGINT) AS e"))
    assert table.schema.field("m").type == pa.timestamp("us", tz="UTC")
    assert table.to_pylist() == [{"m": FULL_WALL_UTC, "e": FULL_EPOCH}]
    assert isinstance(back.schema["ts"].dataType, TimestampType)


def test_max_without_the_view_is_unchanged(spark: ReparkSession) -> None:
    table = _arrow_table(
        spark.sql(
            "SELECT max(CAST(946684800 + (id * 1577) % 3000000000 AS TIMESTAMP)) AS m"
            " FROM range(10)"
        )
    )
    assert table.schema.field("m").type == pa.timestamp("us", tz="UTC")
    assert table.to_pylist() == [{"m": FULL_WALL_UTC}]


def test_max_over_arithmetic_without_the_cast_is_unchanged(spark: ReparkSession) -> None:
    spark.sql(
        "SELECT 946684800 + (id * 1577) % 3000000000 AS x FROM range(10)"
    ).createOrReplaceTempView(VIEW)
    table = _arrow_table(spark.sql(f"SELECT max(x) AS m FROM {VIEW}"))
    assert table.schema.field("m").type == pa.int64()
    assert table.to_pylist() == [{"m": FULL_EPOCH}]
    assert isinstance(spark.table(VIEW).schema["x"].dataType, LongType)
