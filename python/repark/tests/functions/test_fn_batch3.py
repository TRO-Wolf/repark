"""R-FN-BATCH3 — datetime / Java-pattern format pins + loud census."""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark.functions import (
    add_months,
    date_part,
    date_trunc,
    dayofyear,
    extract,
    format_number,
    hour,
    last_day,
    lit,
    minute,
    next_day,
    quarter,
    second,
    timestamp_millis,
    timestamp_seconds,
    to_date,
    to_timestamp,
    try_to_timestamp,
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-batch3").getOrCreate()
    yield session
    session.stop()


def test_datetime_batch3_ok_pins(spark: ReparkSession) -> None:
    frame = spark.sql(
        "SELECT DATE '2020-02-01' AS d, "
        "TIMESTAMP '2020-06-15 15:30:45' AS t, "
        "DATE '2024-01-01' AS d0"
    )
    table = frame.select(
        last_day("d").alias("ld"),
        next_day("d0", "Monday").alias("next_d"),
        dayofyear("d").alias("doy"),
        quarter("d").alias("q"),
        add_months("d", 1).alias("am"),
        date_trunc("month", "t").alias("dt"),
        hour("t").alias("h"),
        minute("t").alias("mi"),
        second("t").alias("s"),
        date_part(lit("year"), "d").alias("dp"),
        extract(lit("month"), "d").alias("ex"),
        timestamp_seconds(lit(0)).alias("ts0"),
        timestamp_millis(lit(0)).alias("tm0"),
    ).to_arrow()
    row = table.to_pylist()[0]
    assert row["ld"].isoformat() == "2020-02-29"
    assert row["next_d"].isoformat() == "2024-01-08"
    assert row["doy"] == 32
    assert row["q"] == 1
    assert row["am"].isoformat() == "2020-03-01"
    assert row["h"] == 15
    assert row["mi"] == 30
    assert row["s"] == 45
    assert row["dp"] == 2020
    assert row["ex"] == 2
    assert pa.types.is_integer(table.schema.field("h").type) or pa.types.is_floating(
        table.schema.field("h").type
    )


def test_datetime_null_case(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT CAST(NULL AS DATE) AS d")
    val = frame.select(last_day("d").alias("v")).to_arrow().to_pylist()[0]["v"]
    assert val is None


def test_java_datetime_patterns_parse(spark: ReparkSession) -> None:
    """FNP-11B step 2 answers Java patterns (pins: fnp-11b/C-002)."""
    frame = spark.sql("SELECT '31/12/2016 10:30' AS s, '2020-01-02' AS t")
    table = frame.select(
        to_date("s", "dd/MM/yyyy HH:mm").alias("d"),
        to_timestamp("t", "yyyy-MM-dd").cast("string").alias("ts"),
    ).to_arrow()
    row = table.to_pylist()[0]
    assert row["d"].isoformat() == "2016-12-31"
    assert row["ts"] == "2020-01-02 00:00:00"
    quoted = (
        spark.sql("SELECT '2016-12-31T10:30:00' AS u")
        .select(
            to_timestamp("u", "yyyy-MM-dd'T'HH:mm:ss").cast("string").alias("q"),
        )
        .to_arrow()
        .to_pylist()[0]["q"]
    )
    assert quoted == "2016-12-31 10:30:00"


def test_batch3_try_to_timestamp_answers(spark: ReparkSession) -> None:
    """FNP-11B step 3 answers timestamps and NULLs (pins: fnp-11b/C-002)."""
    frame = spark.sql("SELECT '2024-06-15 12:00:00' AS s, 'garbage' AS g")
    table = frame.select(
        try_to_timestamp("s").cast("string").alias("ok"),
        try_to_timestamp("g").alias("bad"),
        try_to_timestamp("s", lit("yyyy-MM-dd HH:mm:ss")).cast("string").alias("fmt"),
    ).to_arrow()
    row = table.to_pylist()[0]
    assert row["ok"] == "2024-06-15 12:00:00"
    assert row["bad"] is None
    assert row["fmt"] == "2024-06-15 12:00:00"


def test_batch3_format_number_answers(spark: ReparkSession) -> None:
    frame = spark.sql(
        "SELECT * FROM VALUES (CAST(2.5 AS DOUBLE)), (CAST(NULL AS DOUBLE)), "
        "(CAST(0.125 AS DOUBLE)) AS t(d)"
    )
    table = frame.select(format_number("d", 2).alias("v")).to_arrow()
    assert table.column("v").to_pylist() == ["2.50", None, "0.12"]
    assert table.schema.field("v").type == pa.string()
    assert table.schema.field("v").nullable is True
    # FNP-3: to_utc_timestamp / from_utc_timestamp ship (datafusion-spark kernels).
    # Behavior + the zone round trip: test_fnp3_destubbed.py.
