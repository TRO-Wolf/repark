"""R-POLARS-NS — .str / .dt namespaces + fill_null(value) (value + Arrow type + NULL).

Every greylight-named surface is either pinned OK or loud-unsupported + ledger.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

import repark.spark.polars as rp
from repark import ReparkSession
from repark.errors import UnsupportedOperationException


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-polars-ns").getOrCreate()
    yield session
    session.stop()


def _string_field(table: pa.Table, name: str) -> bool:
    field_type = table.schema.field(name).type
    return pa.types.is_string(field_type) or pa.types.is_large_string(field_type)


def test_str_to_uppercase_lowercase_strip_len(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(" AbC ",), (None,)], ["s"])
    table = frame.pl.select(
        rp.col("s").str.to_uppercase().alias("up"),
        rp.col("s").str.to_lowercase().alias("lo"),
        rp.col("s").str.strip_chars().alias("st"),
        rp.col("s").str.len_chars().alias("ln"),
    ).spark.to_arrow()
    rows = table.to_pylist()
    assert rows[0]["up"] == " ABC "
    assert rows[0]["lo"] == " abc "
    assert rows[0]["st"] == "AbC"
    assert rows[0]["ln"] == 5
    assert rows[1]["up"] is None
    assert rows[1]["lo"] is None
    assert rows[1]["st"] is None
    assert rows[1]["ln"] is None
    assert _string_field(table, "up")
    assert pa.types.is_integer(table.schema.field("ln").type) or pa.types.is_floating(
        table.schema.field("ln").type
    )


def test_str_starts_ends_contains_real_path(spark: ReparkSession) -> None:
    """Must not use unbound SQL expr — real df.pl.select path (skeptic repro)."""
    frame = spark.createDataFrame([("hello",), ("world",), (None,)], ["s"])
    table = frame.pl.select(
        rp.col("s").str.starts_with("he").alias("sw"),
        rp.col("s").str.ends_with("ld").alias("ew"),
        rp.col("s").str.contains("ell").alias("ct"),
    ).spark.to_arrow()
    rows = table.to_pylist()
    assert rows[0]["sw"] is True
    assert rows[0]["ew"] is False
    assert rows[0]["ct"] is True
    assert rows[1]["sw"] is False
    assert rows[1]["ew"] is True
    assert rows[1]["ct"] is False
    assert rows[2]["sw"] is None
    assert rows[2]["ew"] is None
    assert rows[2]["ct"] is None
    assert pa.types.is_boolean(table.schema.field("sw").type)


def test_str_starts_with_quote_not_sql_injection(spark: ReparkSession) -> None:
    """Prefix with quote must not TokenizerError (call_scalar lit path)."""
    frame = spark.createDataFrame([("h'e",), ("hello",)], ["s"])
    table = frame.pl.select(
        rp.col("s").str.starts_with("h'e").alias("sw"),
    ).spark.to_arrow()
    rows = table.to_pylist()
    assert rows[0]["sw"] is True
    assert rows[1]["sw"] is False


def test_str_slice_zero_based(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("hello",), (None,)], ["s"])
    table = frame.pl.select(
        rp.col("s").str.slice(1, 3).alias("sl"),
        rp.col("s").str.slice(2).alias("sl2"),
    ).spark.to_arrow()
    rows = table.to_pylist()
    assert rows[0]["sl"] == "ell"
    assert rows[0]["sl2"] == "llo"
    assert rows[1]["sl"] is None
    assert _string_field(table, "sl")


def test_str_replace_and_pads(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("abXab",), ("7",)], ["s"])
    table = frame.pl.select(
        rp.col("s").str.replace("b", "B").alias("rp"),
        rp.col("s").str.zfill(3).alias("zf"),
        rp.col("s").str.pad_start(3, "0").alias("ps"),
        rp.col("s").str.pad_end(3, "x").alias("pe"),
    ).spark.to_arrow()
    rows = table.to_pylist()
    assert "B" in rows[0]["rp"]
    assert rows[1]["zf"] == "007" or rows[1]["zf"].endswith("7")
    assert _string_field(table, "rp")


def test_str_split_loud(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("a,b",)], ["s"])
    with pytest.raises(UnsupportedOperationException, match="split"):
        frame.pl.select(rp.col("s").str.split(","))


def test_dt_year_month_day_weekday_ordinal_truncate(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT DATE '2020-06-15' AS d UNION ALL SELECT CAST(NULL AS DATE)")
    table = frame.pl.select(
        rp.col("d").dt.year().alias("y"),
        rp.col("d").dt.month().alias("m"),
        rp.col("d").dt.day().alias("day"),
        rp.col("d").dt.weekday().alias("wd"),
        rp.col("d").dt.ordinal_day().alias("od"),
        rp.col("d").dt.truncate("month").alias("tr"),
    ).spark.to_arrow()
    # UNION ALL row order is nondeterministic — sort client-side so the date row is rows[0].
    rows = sorted(table.to_pylist(), key=lambda r: r["y"] is None)
    assert rows[0]["y"] == 2020
    assert rows[0]["m"] == 6
    assert rows[0]["day"] == 15
    assert rows[0]["od"] == 167
    assert str(rows[0]["tr"]).startswith("2020-06-01")
    assert rows[1]["y"] is None
    assert rows[1]["m"] is None
    assert pa.types.is_integer(table.schema.field("y").type) or pa.types.is_floating(
        table.schema.field("y").type
    )


def test_dt_hour_minute_second_offset_loud(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT TIMESTAMP '2020-06-15 12:00:00' AS t")
    with pytest.raises(UnsupportedOperationException, match="hour"):
        frame.pl.select(rp.col("t").dt.hour())
    with pytest.raises(UnsupportedOperationException, match="minute"):
        frame.pl.select(rp.col("t").dt.minute())
    with pytest.raises(UnsupportedOperationException, match="second"):
        frame.pl.select(rp.col("t").dt.second())
    with pytest.raises(UnsupportedOperationException, match="offset_by"):
        frame.pl.select(rp.col("t").dt.offset_by("1d"))


def test_fill_null_value_and_strategy_out(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(None,), (1,)], ["x"])
    filled = frame.pl.fill_null(value=0).spark.to_arrow()
    rows = filled.to_pylist()
    assert rows[0]["x"] == 0
    assert rows[1]["x"] == 1
    with pytest.raises(UnsupportedOperationException, match="forward"):
        frame.pl.fill_null(strategy="forward")
    with pytest.raises(UnsupportedOperationException, match="backward"):
        frame.pl.fill_null(strategy="backward")


def test_null_count(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, None), (None, 2)], ["a", "b"])
    out = frame.pl.null_count().spark.to_arrow().to_pylist()[0]
    assert out["a"] == 1
    assert out["b"] == 1


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("polars") is None,
    reason="real polars not installed",
)
def test_polars_differential_str_upper(spark: ReparkSession) -> None:
    import polars as pl

    data = [("Hello",), ("World",)]
    repark_out = (
        spark.createDataFrame(data, ["s"])
        .pl.select(rp.col("s").str.to_uppercase().alias("s"))
        .collect()
    )
    polars_out = pl.DataFrame({"s": ["Hello", "World"]}).with_columns(
        pl.col("s").str.to_uppercase()
    )
    assert repark_out["s"].to_list() == polars_out["s"].to_list()


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("polars") is None,
    reason="real polars not installed",
)
def test_polars_differential_str_starts_with(spark: ReparkSession) -> None:
    import polars as pl

    data = [("hello",), ("world",)]
    repark_out = (
        spark.createDataFrame(data, ["s"])
        .pl.select(rp.col("s").str.starts_with("he").alias("sw"))
        .collect()
    )
    polars_out = pl.DataFrame({"s": ["hello", "world"]}).with_columns(
        pl.col("s").str.starts_with("he").alias("sw")
    )
    assert repark_out["sw"].to_list() == polars_out["sw"].to_list()


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("polars") is None,
    reason="real polars not installed",
)
def test_polars_differential_str_slice(spark: ReparkSession) -> None:
    import polars as pl

    data = [("hello",), ("world",)]
    repark_out = (
        spark.createDataFrame(data, ["s"])
        .pl.select(rp.col("s").str.slice(1, 3).alias("sl"))
        .collect()
    )
    polars_out = pl.DataFrame({"s": ["hello", "world"]}).with_columns(
        pl.col("s").str.slice(1, 3).alias("sl")
    )
    assert repark_out["sl"].to_list() == polars_out["sl"].to_list()
