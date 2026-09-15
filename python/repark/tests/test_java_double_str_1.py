"""Spark-door DOUBLE/FLOAT stringify answers Java Double.toString (BL-7 FIXED)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-java-double-str-1").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _spark_text(spark: ReparkSession, expression: str) -> pa.Table:
    return _table(spark.sql(f"SELECT {expression} AS s"))


def test_spark_door_cast_double_stringify_matches_java(spark: ReparkSession) -> None:
    """Every BL7/JD double cell stringifies with Java text, typed string."""
    cases: list[tuple[str, str]] = [
        ("Infinity", "Infinity"),
        ("-Infinity", "-Infinity"),
        ("NaN", "NaN"),
        ("1.0E7", "1.0E7"),
        ("1.0E6", "1000000.0"),
        ("123456789.0", "1.23456789E8"),
        ("1.0E-4", "1.0E-4"),
        ("0.001", "0.001"),
        ("1.0E-3", "0.001"),
        ("1.0E21", "1.0E21"),
        ("1.0E20", "1.0E20"),
        ("3.4E38", "3.4E38"),
        ("4.9E-324", "4.9E-324"),
        ("0.30000000000000004", "0.30000000000000004"),
        ("100.0", "100.0"),
        ("1.0", "1.0"),
        ("12.5", "12.5"),
        ("9999999.0", "9999999.0"),
    ]
    for raw, expected in cases:
        table = _spark_text(spark, f"CAST(CAST('{raw}' AS DOUBLE) AS STRING)")
        assert table.column("s").to_pylist() == [expected]
        assert table.schema.field("s").type == pa.string()


def test_spark_door_cast_negative_zero_paths(spark: ReparkSession) -> None:
    """String '-0.0' keeps its sign; decimal -0.0 casts to positive zero."""
    table = _spark_text(spark, "CAST(CAST('-0.0' AS DOUBLE) AS STRING)")
    assert table.column("s").to_pylist() == ["-0.0"]
    assert table.schema.field("s").type == pa.string()
    table = _spark_text(spark, "CAST(CAST(-0.0 AS DOUBLE) AS STRING)")
    assert table.column("s").to_pylist() == ["0.0"]
    assert table.schema.field("s").type == pa.string()


def test_spark_door_cast_float_stringify_matches_java(spark: ReparkSession) -> None:
    """Every JD-float cell stringifies with Java text, typed string."""
    cases: list[tuple[str, str]] = [
        ("1.0E10", "1.0E10"),
        ("0.1", "0.1"),
        ("1.0E7", "1.0E7"),
        ("123456.7", "123456.7"),
        ("1.0E-5", "1.0E-5"),
        ("Infinity", "Infinity"),
        ("-Infinity", "-Infinity"),
        ("NaN", "NaN"),
        ("-0.0", "-0.0"),
    ]
    for raw, expected in cases:
        table = _spark_text(spark, f"CAST(CAST('{raw}' AS FLOAT) AS STRING)")
        assert table.column("s").to_pylist() == [expected]
        assert table.schema.field("s").type == pa.string()


def test_spark_door_concat_coerces_double_with_java_text(spark: ReparkSession) -> None:
    """SQL and facade concat coerce doubles with Java text, typed string."""
    cases: list[tuple[str, str]] = [
        ("1.0E7", "1.0E7"),
        ("Infinity", "Infinity"),
        ("1.0E-5", "1.0E-5"),
        ("1.0E6", "1000000.0"),
    ]
    for raw, expected in cases:
        table = _table(spark.sql(f"SELECT concat(CAST('{raw}' AS DOUBLE), '') AS c"))
        assert table.column("c").to_pylist() == [expected]
        assert table.schema.field("c").type == pa.string()
    frame = spark.range(1).select(F.concat(F.lit(1.0e7), F.lit("")).alias("c"))
    assert _table(frame).column("c").to_pylist() == ["1.0E7"]
    assert _table(frame).schema.field("c").type == pa.string()
    frame = spark.range(1).select(F.concat(F.lit(float("inf")), F.lit("")).alias("c"))
    assert _table(frame).column("c").to_pylist() == ["Infinity"]
    assert _table(frame).schema.field("c").type == pa.string()


def test_facade_cast_and_select_expr_match_java(spark: ReparkSession) -> None:
    """Column cast and selectExpr stringify doubles and floats with Java text."""
    frame = spark.sql(
        "SELECT * FROM (VALUES (0, CAST('1.0E7' AS DOUBLE)), "
        "(1, CAST('Infinity' AS DOUBLE)), (2, CAST('1.0E-5' AS DOUBLE)), "
        "(3, CAST('1.0E6' AS DOUBLE))) AS v(n, d) ORDER BY n"
    )
    table = _table(frame.select(F.col("d").cast("string").alias("s")))
    assert table.column("s").to_pylist() == ["1.0E7", "Infinity", "1.0E-5", "1000000.0"]
    assert table.schema.field("s").type == pa.string()
    table = _table(frame.selectExpr("CAST(d AS STRING) AS s"))
    assert table.column("s").to_pylist() == ["1.0E7", "Infinity", "1.0E-5", "1000000.0"]
    assert table.schema.field("s").type == pa.string()
    floats = spark.sql(
        "SELECT * FROM (VALUES (0, CAST('1.0E10' AS FLOAT)), "
        "(1, CAST('0.1' AS FLOAT))) AS v(n, f) ORDER BY n"
    )
    table = _table(floats.select(F.col("f").cast("string").alias("s")))
    assert table.column("s").to_pylist() == ["1.0E10", "0.1"]
    assert table.schema.field("s").type == pa.string()


def test_spark_door_length_kernels_use_java_text(spark: ReparkSession) -> None:
    """octet_length/bit_length count Java text bytes, typed int32."""
    cases: list[tuple[str, int, int]] = [
        ("Infinity", 8, 64),
        ("-Infinity", 9, 72),
        ("NaN", 3, 24),
        ("1.0E7", 5, 40),
        ("1.0E6", 9, 72),
        ("123456789.0", 12, 96),
        ("1.0E-4", 6, 48),
        ("0.001", 5, 40),
    ]
    for raw, octets, bits in cases:
        table = _table(
            spark.sql(
                f"SELECT octet_length(CAST('{raw}' AS DOUBLE)) AS o, "
                f"bit_length(CAST('{raw}' AS DOUBLE)) AS b"
            )
        )
        assert table.column("o").to_pylist() == [octets]
        assert table.column("b").to_pylist() == [bits]
        assert table.schema.field("o").type == pa.int32()
        assert table.schema.field("b").type == pa.int32()
    table = _table(spark.sql("SELECT octet_length(CAST('1.0E10' AS FLOAT)) AS o"))
    assert table.column("o").to_pylist() == [6]
    assert table.schema.field("o").type == pa.int32()


def test_spark_door_null_float_stringify_is_null(spark: ReparkSession) -> None:
    """NULL doubles stay NULL through CAST, concat and the length kernels."""
    table = _table(spark.sql("SELECT CAST(CAST(NULL AS DOUBLE) AS STRING) AS s"))
    assert table.column("s").to_pylist() == [None]
    assert table.schema.field("s").type == pa.string()
    table = _table(spark.sql("SELECT concat(CAST(NULL AS DOUBLE), '') AS c"))
    assert table.column("c").to_pylist() == [None]
    assert table.schema.field("c").type == pa.string()
    table = _table(spark.sql("SELECT octet_length(CAST(NULL AS DOUBLE)) AS o"))
    assert table.column("o").to_pylist() == [None]
    assert table.schema.field("o").type == pa.int32()


def test_native_door_keeps_arrow_spelling() -> None:
    """The native ANSI door keeps Arrow float formatting, typed string."""
    import repark

    table = repark.sql("SELECT CAST(CAST('Infinity' AS DOUBLE) AS STRING) AS s").to_arrow()
    assert table.column("s").to_pylist() == ["inf"]
    assert table.schema.field("s").type == pa.string()
    table = repark.sql("SELECT CAST(CAST('1.0E7' AS DOUBLE) AS STRING) AS s").to_arrow()
    assert table.column("s").to_pylist() == ["10000000.0"]
    assert table.schema.field("s").type == pa.string()
