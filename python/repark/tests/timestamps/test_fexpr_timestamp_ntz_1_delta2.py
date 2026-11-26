from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from repark import ReparkSession
from repark.spark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("test_fexpr_timestamp_ntz_1_delta2")

NEW_YORK = "America/New_York"
ANSI_MODES = (True, False)
WALL = dt.datetime(2024, 1, 1, 2, 0)
TIME_ONLY = ("02:00:00", "T02:00:00", "02:00:00.123456", "T02:00:00+02:00", "02:00:00Z")
BRACKETED = (
    "2024-01-01 02:00:00[UTC]",
    "2024-01-01 02:00:00[INVALID_ZONE]",
    "2024-01-01 02:00:00[America/New_York]",
    "2024-01-01 02:00:00[not a zone!]",
    "2024-01-01T02:00:00Z[UTC]",
    "2024-01-01T02:00:00-05:00[America/New_York]",
    "2024-01-01 02:00:00 [UTC]",
    "2024-01-01 02:00:00[]",
    "2024-01-01[UTC]",
)
NTZ_DOORS = ("sql", "selectExpr", "expr", "column", "column_expr", "lit")
LTZ_NULLABLE = (
    "CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ)",
    "'2024-01-01 02:00:00'::timestamp_ltz",
    "CAST('2024-01-01T02:00:00Z' AS TIMESTAMP_LTZ)",
    "CAST('2024-01-01T02:00:00+05:30' AS TIMESTAMP_LTZ)",
    "CAST('  2024-01-01 02:00:00 ' AS TIMESTAMP_LTZ)",
    "COALESCE(CAST(NULL AS TIMESTAMP_NTZ), CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ))",
    "COALESCE(CAST(NULL AS TIMESTAMP_LTZ), CAST('2024-01-01 02:00:00' AS TIMESTAMP_LTZ))",
)
LTZ_NOT_NULLABLE = (
    "TIMESTAMP_LTZ '2024-01-01 02:00:00'",
    "TIMESTAMP_LTZ '2024-01-01 02:00:00Z'",
    "TIMESTAMP '2024-01-01 02:00:00Z'",
    "COALESCE(NULL, TIMESTAMP_LTZ '2024-01-01 02:00:00')",
)
EXPR_DOORS = ("select", "withColumn", "selectExpr", "sql")


def _session(
    ansi: bool, timestamp_type: str = "TIMESTAMP_LTZ", warehouse: Path | None = None
) -> ReparkSession:
    _reset_active_session_for_tests()
    builder = (
        ReparkSession.builder.appName("fexpr-ntz-delta2")
        .config("spark.sql.session.timeZone", NEW_YORK)
        .config("spark.sql.ansi.enabled", "true" if ansi else "false")
        .config("spark.sql.timestampType", timestamp_type)
    )
    if warehouse is not None:
        builder = (
            builder.config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        )
    return builder.getOrCreate()


@pytest.fixture(autouse=True)
def _clean_active_session() -> Iterator[None]:
    _reset_active_session_for_tests()
    yield
    _reset_active_session_for_tests()


def _quoted(text: str) -> str:
    return "'" + text + "'"


def _ntz_frame(spark: ReparkSession, door: str, text: str, try_cast: bool) -> Any:
    cast = "TRY_CAST" if try_cast else "CAST"
    source = spark.createDataFrame([(text,)], "s string")
    builders: dict[str, Callable[[], Any]] = {
        "sql": lambda: spark.sql(f"SELECT {cast}({_quoted(text)} AS TIMESTAMP_NTZ) AS v"),
        "selectExpr": lambda: spark.range(1).selectExpr(
            f"{cast}({_quoted(text)} AS TIMESTAMP_NTZ) AS v"
        ),
        "expr": lambda: spark.range(1).select(
            sf.expr(f"{cast}({_quoted(text)} AS TIMESTAMP_NTZ)").alias("v")
        ),
        "column": lambda: source.select(
            (
                sf.col("s").try_cast("timestamp_ntz")
                if try_cast
                else sf.col("s").cast("timestamp_ntz")
            ).alias("v")
        ),
        "column_expr": lambda: source.select(sf.expr(f"{cast}(s AS TIMESTAMP_NTZ)").alias("v")),
        "lit": lambda: spark.range(1).select(
            (
                sf.lit(text).try_cast("timestamp_ntz")
                if try_cast
                else sf.lit(text).cast("timestamp_ntz")
            ).alias("v")
        ),
    }
    return builders[door]()


def _assert_refused_or_null(spark: ReparkSession, door: str, text: str, ansi: bool) -> None:
    assert _ntz_frame(spark, door, text, try_cast=True).collect()[0][0] is None, (door, text)
    if ansi:
        with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
            _ntz_frame(spark, door, text, try_cast=False).collect()
    else:
        assert _ntz_frame(spark, door, text, try_cast=False).collect()[0][0] is None, (door, text)


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("door", NTZ_DOORS)
def test_a_time_only_string_is_no_timestamp_ntz(door: str, ansi: bool) -> None:
    spark = _session(ansi)
    for text in TIME_ONLY:
        _assert_refused_or_null(spark, door, text, ansi)


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("door", NTZ_DOORS)
def test_a_bracketed_zone_suffix_is_no_timestamp_ntz(door: str, ansi: bool) -> None:
    spark = _session(ansi)
    for text in BRACKETED:
        _assert_refused_or_null(spark, door, text, ansi)


@pytest.mark.parametrize("text", ("02:00:00", "T02:00:00", "2024-01-01 02:00:00[UTC]"))
def test_a_typed_timestamp_ntz_literal_refuses_time_only_and_brackets(text: str) -> None:
    spark = _session(True)
    with pytest.raises(Exception, match="INVALID_TYPED_LITERAL"):
        spark.sql(f"SELECT TIMESTAMP_NTZ {_quoted(text)} AS v").collect()


@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_time_only_and_bracketed_writes_store_null_or_refuse(ansi: bool, tmp_path: Path) -> None:
    spark = _session(ansi, warehouse=tmp_path / "wh")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
    for index, text in enumerate(("02:00:00", "T02:00:00", "2024-01-01 02:00:00[UTC]")):
        select = f"SELECT 1 AS id, {{}}({_quoted(text)} AS TIMESTAMP_NTZ) AS v"
        spark.sql(f"CREATE TABLE ice.db.t{index} USING iceberg AS " + select.format("TRY_CAST"))
        assert [tuple(row) for row in spark.table(f"ice.db.t{index}").collect()] == [(1, None)], (
            text
        )
        cast_sql = f"CREATE TABLE ice.db.c{index} USING iceberg AS " + select.format("CAST")
        if ansi:
            with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
                spark.sql(cast_sql)
        else:
            spark.sql(cast_sql)
            assert [tuple(row) for row in spark.table(f"ice.db.c{index}").collect()] == [
                (1, None)
            ], text
    path = tmp_path / "bracketed"
    frame = spark.range(1).select(
        sf.col("id").cast("int").alias("id"),
        sf.expr("CAST('2024-01-01 02:00:00[INVALID_ZONE]' AS TIMESTAMP_NTZ)").alias("v"),
    )
    if ansi:
        with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
            frame.write.parquet(str(path))
        assert not list(path.glob("*.parquet"))
    else:
        frame.write.parquet(str(path))
        assert [tuple(row) for row in spark.read.parquet(str(path)).collect()] == [(0, None)]
    column = tmp_path / "time_only_column"
    source = spark.createDataFrame(
        [(1, "02:00:00"), (2, "2024-01-01 02:00:00")], "id int, s string"
    )
    source.select("id", sf.col("s").try_cast("timestamp_ntz").alias("v")).write.parquet(str(column))
    stored = sorted(tuple(row) for row in spark.read.parquet(str(column)).collect())
    assert stored == [(1, None), (2, WALL)]


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("target", ("TIMESTAMP", "TIMESTAMP_LTZ"))
def test_a_time_only_string_is_still_todays_ltz_instant(target: str, ansi: bool) -> None:
    spark = _session(ansi)
    before = dt.datetime.now(ZoneInfo(NEW_YORK)).date()
    for text in ("02:00:00", "T02:00:00"):
        for cast in ("CAST", "TRY_CAST"):
            fragment = f"{cast}({_quoted(text)} AS {target})"
            frames = (
                spark.sql(f"SELECT {fragment} AS v"),
                spark.range(1).selectExpr(f"{fragment} AS v"),
                spark.range(1).select(sf.expr(fragment).alias("v")),
            )
            for frame in frames:
                assert frame.dtypes == [("v", "timestamp")], fragment
                value = frame.collect()[0][0]
                after = dt.datetime.now(ZoneInfo(NEW_YORK)).date()
                assert value.time() == dt.time(2, 0), fragment
                assert value.date() in {before, after}, fragment


def _expr_frame(spark: ReparkSession, door: str, fragment: str) -> Any:
    if door == "select":
        return spark.range(1).select(sf.expr(fragment).alias("v"))
    if door == "withColumn":
        return spark.range(1).withColumn("v", sf.expr(fragment)).select("v")
    if door == "selectExpr":
        return spark.range(1).selectExpr(f"{fragment} AS v")
    return spark.sql(f"SELECT {fragment} AS v")


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("door", EXPR_DOORS)
def test_explicit_ltz_and_ntz_string_casts_are_nullable(door: str, ansi: bool) -> None:
    spark = _session(ansi)
    for fragment in (*LTZ_NULLABLE, "CAST('02:00:00' AS TIMESTAMP_LTZ)"):
        frame = _expr_frame(spark, door, fragment)
        assert frame.dtypes == [("v", "timestamp")], fragment
        assert frame.schema.fields[0].nullable, fragment
    for fragment in (
        "CAST('2024-01-01 02:00:00' AS TIMESTAMP_NTZ)",
        "'2024-01-01 02:00:00'::timestamp_ntz",
    ):
        frame = _expr_frame(spark, door, fragment)
        assert frame.dtypes == [("v", "timestamp_ntz")], fragment
        assert frame.schema.fields[0].nullable, fragment
        assert frame.collect()[0][0] == WALL, fragment
    assert _expr_frame(spark, door, LTZ_NULLABLE[0]).collect()[0][0] == WALL


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("door", EXPR_DOORS)
def test_typed_timestamp_literals_stay_not_nullable(door: str, ansi: bool) -> None:
    spark = _session(ansi)
    for fragment in (*LTZ_NOT_NULLABLE, "TIMESTAMP_NTZ '2024-01-01 02:00:00'"):
        frame = _expr_frame(spark, door, fragment)
        assert not frame.schema.fields[0].nullable, fragment
    assert _expr_frame(spark, door, LTZ_NOT_NULLABLE[0]).collect()[0][0] == WALL
