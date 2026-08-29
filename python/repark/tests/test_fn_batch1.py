"""R-FN-BATCH1 — top scalar function wrappers (value + Arrow type + null case)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException
from repark.spark.functions import (
    ceil,
    concat_ws,
    current_date,
    exp,
    floor,
    from_unixtime,
    greatest,
    initcap,
    instr,
    isnan,
    isnull,
    least,
    length,
    lit,
    log,
    log10,
    lower,
    lpad,
    ltrim,
    md5,
    nanvl,
    pow,
    regexp_replace,
    round,
    rpad,
    rtrim,
    signum,
    split,
    sqrt,
    to_date,
    trim,
    upper,
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-batch1").getOrCreate()
    yield session
    session.stop()


def test_string_functions(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT ' AbC ' AS s")
    row = frame.select(
        lower("s").alias("lo"),
        upper("s").alias("up"),
        trim("s").alias("tr"),
        ltrim("s").alias("lt"),
        rtrim("s").alias("rt"),
        length("s").alias("ln"),
        initcap("s").alias("ic"),
        lpad("s", 8, "0").alias("lp"),
        rpad(trim("s"), 5, "x").alias("rp"),
        instr("s", "b").alias("ix"),
        concat_ws("-", trim("s"), lit("z")).alias("cw"),
        regexp_replace("s", "b", "X").alias("rr"),
    ).to_arrow()
    data = row.to_pylist()[0]
    assert data["lo"] == " abc "
    assert data["up"] == " ABC "
    assert data["tr"] == "AbC"
    assert data["ln"] == 5
    assert data["ix"] == 3
    assert data["cw"] == "AbC-z"
    assert "X" in data["rr"]
    assert row.schema.field("ln").type in (pa.int32(), pa.int64(), pa.uint32(), pa.uint64())


def test_math_functions(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT -3.7 AS x, 4.0 AS y")
    table = frame.select(
        floor("x").alias("fl"),
        ceil("x").alias("ce"),
        round("x", 0).alias("rd"),
        sqrt("y").alias("sq"),
        signum("x").alias("sg"),
        exp(lit(0)).alias("ex"),
        pow(lit(2), lit(3)).alias("pw"),
        log(lit(10.0)).alias("lg"),
        log10(lit(10.0)).alias("l10"),
    ).to_arrow()
    row = table.to_pylist()[0]
    assert row["fl"] == -4.0 or row["fl"] == -4
    assert row["ce"] == -3.0 or row["ce"] == -3
    assert row["sq"] == 2.0
    assert row["sg"] == -1.0
    assert row["ex"] == 1.0
    assert row["pw"] == 8.0
    # Spark F.log is natural log; F.log10 is base-10.
    assert abs(row["lg"] - 2.302585092994046) < 1e-9
    assert abs(row["l10"] - 1.0) < 1e-9
    assert pa.types.is_floating(table.schema.field("lg").type)


def test_null_and_nan(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT CAST(NULL AS VARCHAR) AS s, CAST('NaN' AS DOUBLE) AS x")
    row = (
        frame.select(
            isnull("s").alias("isn"),
            isnan("x").alias("ina"),
            nanvl("x", lit(0.0)).alias("nv"),
            md5(lit("a")).alias("m"),
        )
        .to_arrow()
        .to_pylist()[0]
    )
    assert row["isn"] is True
    assert row["ina"] is True
    assert row["nv"] == 0.0
    assert isinstance(row["m"], str) and len(row["m"]) == 32


def test_greatest_least_dates(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS a, 2 AS b")
    row = frame.select(
        greatest("a", "b").alias("g"),
        least("a", "b").alias("l"),
        current_date().alias("d"),
        to_date(lit("2020-01-02")).alias("td"),
    ).to_arrow()
    data = row.to_pylist()[0]
    assert data["g"] == 2
    assert data["l"] == 1
    assert data["td"].isoformat() == "2020-01-02"
    # ColumnOrName: bare str is column name, not a literal.
    col_date = (
        spark.sql("SELECT '2020-01-02' AS d")
        .select(to_date("d").alias("td"))
        .to_arrow()
        .to_pylist()[0]["td"]
    )
    assert col_date.isoformat() == "2020-01-02"
    # Spark from_unixtime → string.
    ts_table = frame.select(from_unixtime(lit(0)).alias("t")).to_arrow()
    assert pa.types.is_string(ts_table.schema.field("t").type) or pa.types.is_large_string(
        ts_table.schema.field("t").type
    )
    assert ts_table.to_pylist()[0]["t"] == "1970-01-01 00:00:00"


def test_unsupported_loud(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException, match="split"):
        split("s", ",")
    # FNP-3: datediff ships — Spark's older spelling of date_diff, same engine arm.
    # Behavior: test_fnp3_destubbed.py.


def test_null_arg_propagation_lower(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT CAST(NULL AS VARCHAR) AS s")
    val = frame.select(lower("s").alias("v")).to_arrow().to_pylist()[0]["v"]
    assert val is None
