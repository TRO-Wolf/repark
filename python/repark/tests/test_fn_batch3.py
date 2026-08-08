"""R-FN-BATCH3 — datetime / format wrappers + Chrono≠Java refusal pins."""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException
from repark.functions import (
    add_months,
    date_part,
    date_trunc,
    dayofyear,
    extract,
    format_number,
    from_utc_timestamp,
    hour,
    last_day,
    lit,
    make_timestamp,
    minute,
    next_day,
    quarter,
    second,
    timestamp_millis,
    timestamp_seconds,
    to_date,
    to_timestamp,
    to_utc_timestamp,
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
        date_part("year", "d").alias("dp"),
        extract("month", "d").alias("ex"),
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


def test_chrono_java_format_refusal(spark: ReparkSession) -> None:
    """U4 Chrono≠Java rule STANDS for format-pattern args (W3 greylight)."""
    with pytest.raises(UnsupportedOperationException, match="format"):
        to_timestamp(lit("2020-01-02"), format="yyyy-MM-dd")
    with pytest.raises(UnsupportedOperationException, match="format"):
        to_date(lit("2020-01-02"), format="yyyy-MM-dd")


def test_batch3_loud_unsupported(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException, match="format_number"):
        format_number("x", 2)
    with pytest.raises(UnsupportedOperationException, match="try_to_timestamp"):
        try_to_timestamp("x")
    with pytest.raises(UnsupportedOperationException, match="to_utc_timestamp"):
        to_utc_timestamp("t", "UTC")
    with pytest.raises(UnsupportedOperationException, match="from_utc_timestamp"):
        from_utc_timestamp("t", "UTC")
    with pytest.raises(UnsupportedOperationException, match="make_timestamp"):
        make_timestamp(2020, 1, 2, 3, 4, 5)
