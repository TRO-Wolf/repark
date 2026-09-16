"""Runtime zone spellings that only Java `ZoneId` accepts — SET-ANSI-RUNTIME-1 P1.

Oracle: `fixtures-batch5.json` S5-tz-* cells (PySpark 4.1.2 echo) plus the wall
clocks Spark's offsets imply (`+5` is UTC+5, `GMT+8` is UTC+8, `Z` is UTC).
Ruling R-17c-4: the snapshot carries the raw Spark-visible text (echo) and a
canonical companion (Arrow `Tz` / Python `ZoneInfo`-convertible) that every
value-bearing consumer uses.

Every spelling sets the zone at runtime and then asserts VALUE-bearing answers
(`from_unixtime(0)`, its string cast, its hour) on both doors, the
`createDataFrame` naive-datetime path (instant on the Arrow path, wall on
`collect`), and the raw-text echo (`conf.get`, `current_timezone()`). The two
seconds spellings have no Arrow form and refuse at the SET (R-17c-4).
"""

from __future__ import annotations

import datetime
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import IllegalArgumentException

ZONE_KEY = "spark.sql.session.timeZone"

EPOCH_WALL = {
    "+5": "1970-01-01 05:00:00",
    "GMT+8": "1970-01-01 08:00:00",
    "gmt+8": "1970-01-01 08:00:00",
    "UT+3": "1970-01-01 03:00:00",
    "Z": "1970-01-01 00:00:00",
    "z": "1970-01-01 00:00:00",
}

EPOCH_HOUR = {
    "+5": 5,
    "GMT+8": 8,
    "gmt+8": 8,
    "UT+3": 3,
    "Z": 0,
    "z": 0,
}

NAIVE_NOON = datetime.datetime(2024, 6, 1, 12, 0, 0)

NAIVE_INSTANT_UTC = {
    "+5": datetime.datetime(2024, 6, 1, 7, 0, 0, tzinfo=datetime.UTC),
    "GMT+8": datetime.datetime(2024, 6, 1, 4, 0, 0, tzinfo=datetime.UTC),
    "gmt+8": datetime.datetime(2024, 6, 1, 4, 0, 0, tzinfo=datetime.UTC),
    "UT+3": datetime.datetime(2024, 6, 1, 9, 0, 0, tzinfo=datetime.UTC),
    "Z": datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=datetime.UTC),
    "z": datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=datetime.UTC),
}


def _session() -> ReparkSession:
    """One session on the S16 oracle basis: zone UTC, ANSI on, both explicit."""
    return (
        ReparkSession.builder.appName("runtime-zone-spellings-1")
        .config(ZONE_KEY, "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


def _arrow(frame: Any) -> pa.Table:
    """Materialize a frame on the Arrow path."""
    return frame.to_arrow()


def test_sql_door_value_queries_follow_java_spellings() -> None:
    """`from_unixtime(0)` and its cast/hour follow each Java-only spelling — SQL door."""
    for raw, wall in EPOCH_WALL.items():
        spark = _session()
        spark.conf.set(ZONE_KEY, raw)
        assert spark.conf.get(ZONE_KEY) == raw
        table = _arrow(spark.sql("SELECT current_timezone() AS tz"))
        assert table.to_pylist() == [{"tz": raw}]
        table = _arrow(
            spark.sql(
                "SELECT from_unixtime(0) AS tick, "
                "CAST(from_unixtime(0) AS STRING) AS render, "
                "hour(CAST(from_unixtime(0) AS TIMESTAMP)) AS tick_hour"
            )
        )
        assert table.to_pylist() == [{"tick": wall, "render": wall, "tick_hour": EPOCH_HOUR[raw]}]
        spark.stop()


def test_f_api_value_queries_follow_java_spellings() -> None:
    """`F.from_unixtime` and friends follow each Java-only spelling — F-API door."""
    for raw, wall in EPOCH_WALL.items():
        spark = _session()
        spark.conf.set(ZONE_KEY, raw)
        table = _arrow(
            spark.sql("SELECT 1 AS anchor").select(
                F.from_unixtime(F.lit(0)).alias("tick"),
                F.from_unixtime(F.lit(0)).cast("string").alias("render"),
                F.hour(F.from_unixtime(F.lit(0)).cast("timestamp")).alias("tick_hour"),
            )
        )
        assert table.to_pylist() == [{"tick": wall, "render": wall, "tick_hour": EPOCH_HOUR[raw]}]
        spark.stop()


def test_select_expr_binds_the_zone_at_analysis() -> None:
    """`selectExpr("current_timezone()")` after a SET answers the new zone.

    The expression parses at call time, so it reads the live carrier — the Rust-reachable
    path for the P2 hand-off to run 17a (`F.current_timezone()` still binds a Python
    literal in `functions*.py` instead of this UDF).
    """
    spark = _session()
    spark.conf.set(ZONE_KEY, "+5")
    table = _arrow(spark.sql("SELECT 1 AS anchor").selectExpr("current_timezone() AS tz"))
    assert table.to_pylist() == [{"tz": "+5"}]
    spark.stop()


def test_create_dataframe_naive_datetime_follows_java_spellings() -> None:
    """A naive datetime localises in the runtime zone: instant on Arrow, wall on collect."""
    for raw, instant in NAIVE_INSTANT_UTC.items():
        spark = _session()
        spark.conf.set(ZONE_KEY, raw)
        frame = spark.createDataFrame([(NAIVE_NOON,)], ["ts"])
        table = _arrow(frame.select("ts"))
        field_type = table.schema.field("ts").type
        assert pa.types.is_timestamp(field_type) and field_type.tz is not None
        assert table.column("ts").to_pylist() == [instant]
        assert [row.ts for row in frame.select("ts").collect()] == [NAIVE_NOON]
        spark.stop()


def test_seconds_spellings_refuse_at_the_set() -> None:
    """`+05:30:30` / `+18:00:00` validate as Java but have no Arrow form: refuse, store nothing."""
    for raw in ("+05:30:30", "+18:00:00"):
        spark = _session()
        with pytest.raises(IllegalArgumentException) as caught:
            spark.conf.set(ZONE_KEY, raw)
        assert "[INVALID_CONF_VALUE.TIME_ZONE]" in str(caught.value)
        assert spark.conf.get(ZONE_KEY) == "UTC"
        table = _arrow(spark.sql("SELECT current_timezone() AS tz"))
        assert table.to_pylist() == [{"tz": "UTC"}]
        spark.stop()
