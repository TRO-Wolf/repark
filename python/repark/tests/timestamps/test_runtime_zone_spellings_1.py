"""Zone spellings against the batch-17 oracle — SET-ANSI-RUNTIME-1 review follow-up.

Oracle: `/tmp/oc-worker/rc-oracle/fixtures-batch17-zone-spellings.json` (PySpark 4.1.2,
builder zone UTC, ANSI on; per spelling: `conf.set`, then `conf.get`,
`current_timezone()`, `CAST(from_unixtime(0) AS STRING)` and
`CAST(TIMESTAMP'2024-01-01 00:00:00' AS STRING)`). 15 cells; every Spark refusal
carries `IllegalArgumentException` with the RAW zone in the message.

Ruling R-17c-6: Spark's acceptance set is the specification in both directions —
`ZoneId` matching is case-sensitive (`GMT+8` yes, `gmt+8` no; `Z` yes, `z` no),
surrounding whitespace is not trimmed (padded refuses), seconds-precision offsets are
accepted (`+05:30:30`, `+18:00:00`). `+05:30:30` has no Arrow form and stays a refusal
as DECLARED divergence (registry SET-ANSI-RUNTIME-4: Spark accepts, RePark refuses);
`+18:00:00` canonicalises to `+18:00`.
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

OK_CELLS: list[tuple[str, str, int]] = [
    ("+5", "1970-01-01 05:00:00", 5),
    ("+05", "1970-01-01 05:00:00", 5),
    ("+0530", "1970-01-01 05:30:00", 5),
    ("+18:00", "1970-01-01 18:00:00", 18),
    ("+18:00:00", "1970-01-01 18:00:00", 18),
    ("GMT+8", "1970-01-01 08:00:00", 8),
    ("UT+3", "1970-01-01 03:00:00", 3),
    ("Z", "1970-01-01 00:00:00", 0),
    ("Asia/Tokyo", "1970-01-01 09:00:00", 9),
]

REFUSAL_CELLS: list[str] = ["+18:01", "gmt+8", "z", "Not/AZone"]

PADDED_ZONE = "  Asia/Tokyo  "

TS_CAST_WALL = "2024-01-01 00:00:00"

NAIVE_NOON = datetime.datetime(2024, 6, 1, 12, 0, 0)

NAIVE_INSTANT_UTC = {
    "+5": datetime.datetime(2024, 6, 1, 7, 0, 0, tzinfo=datetime.UTC),
    "GMT+8": datetime.datetime(2024, 6, 1, 4, 0, 0, tzinfo=datetime.UTC),
    "UT+3": datetime.datetime(2024, 6, 1, 9, 0, 0, tzinfo=datetime.UTC),
    "Z": datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=datetime.UTC),
    "+18:00:00": datetime.datetime(2024, 5, 31, 18, 0, 0, tzinfo=datetime.UTC),
}


def _session() -> ReparkSession:
    """One session on the batch-17 oracle basis: zone UTC, ANSI on, both explicit."""
    return (
        ReparkSession.builder.appName("runtime-zone-spellings-1")
        .config(ZONE_KEY, "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


def _arrow(frame: Any) -> pa.Table:
    """Materialize a frame on the Arrow path."""
    return frame.to_arrow()


def _assert_values(spark: ReparkSession, raw: str, wall: str, hour: int) -> None:
    """The batch-17 value columns on the SQL door plus the F-API door."""
    assert spark.conf.get(ZONE_KEY) == raw
    table = _arrow(
        spark.sql(
            "SELECT current_timezone() AS tz, "
            "CAST(from_unixtime(0) AS STRING) AS tick, "
            "CAST(TIMESTAMP'2024-01-01 00:00:00' AS STRING) AS stable, "
            "hour(CAST(from_unixtime(0) AS TIMESTAMP)) AS tick_hour"
        )
    )
    assert table.to_pylist() == [
        {"tz": raw, "tick": wall, "stable": TS_CAST_WALL, "tick_hour": hour}
    ]
    table = _arrow(
        spark.sql("SELECT 1 AS anchor").select(
            F.from_unixtime(F.lit(0)).cast("string").alias("tick"),
            F.hour(F.from_unixtime(F.lit(0)).cast("timestamp")).alias("tick_hour"),
        )
    )
    assert table.to_pylist() == [{"tick": wall, "tick_hour": hour}]


def test_batch17_ok_cells_apply_on_both_doors() -> None:
    """Every oracle OK cell: `conf.set` and SQL `SET` apply; echo is raw; values match."""
    for raw, wall, hour in OK_CELLS:
        spark = _session()
        spark.conf.set(ZONE_KEY, raw)
        _assert_values(spark, raw, wall, hour)
        spark.stop()
        spark = _session()
        spark.sql(f"SET {ZONE_KEY} = {raw}")
        _assert_values(spark, raw, wall, hour)
        spark.stop()


def test_batch17_refusal_cells_refuse_on_both_doors() -> None:
    """Every oracle RAISE cell: both carriers refuse, message echoes raw, nothing stored."""
    for raw in REFUSAL_CELLS:
        spark = _session()
        with pytest.raises(IllegalArgumentException) as caught:
            spark.conf.set(ZONE_KEY, raw)
        assert "[INVALID_CONF_VALUE.TIME_ZONE]" in str(caught.value)
        assert raw in str(caught.value)
        assert spark.conf.get(ZONE_KEY) == "UTC"
        table = _arrow(spark.sql("SELECT current_timezone() AS tz"))
        assert table.to_pylist() == [{"tz": "UTC"}]
        spark.stop()
        spark = _session()
        with pytest.raises(IllegalArgumentException) as caught_sql:
            spark.sql(f"SET {ZONE_KEY} = {raw}")
        assert "[INVALID_CONF_VALUE.TIME_ZONE]" in str(caught_sql.value)
        assert spark.conf.get(ZONE_KEY) == "UTC"
        spark.stop()


def test_batch17_padded_zone_refuses_on_both_doors() -> None:
    """Surrounding whitespace is not trimmed: padded refuses with the raw text echoed."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.conf.set(ZONE_KEY, PADDED_ZONE)
    assert "[INVALID_CONF_VALUE.TIME_ZONE]" in str(caught.value)
    assert PADDED_ZONE in str(caught.value)
    assert spark.conf.get(ZONE_KEY) == "UTC"
    spark.stop()
    spark = _session()
    with pytest.raises(IllegalArgumentException):
        spark.sql(f"SET {ZONE_KEY} = '{PADDED_ZONE}'")
    assert spark.conf.get(ZONE_KEY) == "UTC"
    spark.stop()


def test_batch17_nonzero_seconds_is_declared_divergence() -> None:
    """`+05:30:30`: Spark accepts, RePark refuses — registry SET-ANSI-RUNTIME-4.

    `+05:30:30` has no Arrow `Tz` form and the engine consumers are out of this unit's
    fence; the refusal at the SET (nothing stored) is the documented divergence, never
    a silent accept-then-explode. `+18:00:00` needs no divergence: it canonicalises to
    `+18:00` (covered in the OK cells).
    """
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.conf.set(ZONE_KEY, "+05:30:30")
    assert "[INVALID_CONF_VALUE.TIME_ZONE]" in str(caught.value)
    assert spark.conf.get(ZONE_KEY) == "UTC"
    table = _arrow(spark.sql("SELECT current_timezone() AS tz"))
    assert table.to_pylist() == [{"tz": "UTC"}]
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
