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


def test_spark_door_try_cast_stringify_matches_java(spark: ReparkSession) -> None:
    """TRY_CAST of doubles uses Java text, typed string, NULL stays NULL."""
    table = _table(
        spark.sql(
            "SELECT TRY_CAST(CAST('1.0E7' AS DOUBLE) AS STRING) AS a, "
            "TRY_CAST(CAST('Infinity' AS DOUBLE) AS STRING) AS b, "
            "TRY_CAST(CAST('0.001' AS DOUBLE) AS STRING) AS c"
        )
    )
    assert table.column("a").to_pylist() == ["1.0E7"]
    assert table.column("b").to_pylist() == ["Infinity"]
    assert table.column("c").to_pylist() == ["0.001"]
    assert table.schema.field("a").type == pa.string()
    frame = spark.sql(
        "SELECT * FROM (VALUES (0, CAST('1.0E7' AS DOUBLE)), "
        "(1, CAST('Infinity' AS DOUBLE)), (2, CAST('1.0E-5' AS DOUBLE)), "
        "(3, CAST('1.0E6' AS DOUBLE)), (4, CAST(NULL AS DOUBLE))) AS v(n, d) ORDER BY n"
    )
    table = _table(frame.select(F.col("d").try_cast("string").alias("s")))
    assert table.column("s").to_pylist() == ["1.0E7", "Infinity", "1.0E-5", "1000000.0", None]
    assert table.schema.field("s").type == pa.string()


def test_spark_door_array_join_uses_java_text(spark: ReparkSession) -> None:
    """array_join renders double/float elements with Java text, typed string."""
    table = _table(
        spark.sql(
            "SELECT array_join(array(CAST('1.0E7' AS DOUBLE), CAST('0.1' AS DOUBLE), "
            "CAST('1.0E-5' AS DOUBLE)), ',') AS a"
        )
    )
    assert table.column("a").to_pylist() == ["1.0E7,0.1,1.0E-5"]
    assert table.schema.field("a").type == pa.string()
    table = _table(
        spark.sql(
            "SELECT array_join(array(CAST('1.0E10' AS FLOAT), CAST('0.1' AS FLOAT)), ',') AS a"
        )
    )
    assert table.column("a").to_pylist() == ["1.0E10,0.1"]
    assert table.schema.field("a").type == pa.string()
    frame = spark.sql(
        "SELECT * FROM (VALUES (0, CAST('1.0E7' AS DOUBLE)), "
        "(1, CAST('0.1' AS DOUBLE)), (2, CAST('1.0E-5' AS DOUBLE))) AS v(n, d) ORDER BY n"
    )
    table = _table(frame.select(F.array_join(F.array(F.col("d"), F.col("d")), ",").alias("a")))
    assert table.column("a").to_pylist() == ["1.0E7,1.0E7", "0.1,0.1", "1.0E-5,1.0E-5"]
    assert table.schema.field("a").type == pa.string()


def test_spark_door_format_string_s_uses_java_text(spark: ReparkSession) -> None:
    """format_string/printf %s renders doubles with Java text, typed string."""
    table = _table(
        spark.sql(
            "SELECT format_string('%s', CAST('1.0E7' AS DOUBLE)) AS a, "
            "format_string('%s', CAST('0.1' AS DOUBLE)) AS b, "
            "printf('%s', CAST('1.0E-5' AS DOUBLE)) AS c"
        )
    )
    assert table.column("a").to_pylist() == ["1.0E7"]
    assert table.column("b").to_pylist() == ["0.1"]
    assert table.column("c").to_pylist() == ["1.0E-5"]
    assert table.schema.field("a").type == pa.string()
    frame = spark.range(1).select(
        F.format_string("%s", F.lit(1.0e7)).alias("a"),
        F.printf("%s", F.lit(0.1)).alias("b"),
    )
    assert _table(frame).column("a").to_pylist() == ["1.0E7"]
    assert _table(frame).column("b").to_pylist() == ["0.1"]


def test_spark_door_format_string_f_is_todays_answer(spark: ReparkSession) -> None:
    """%f rounds HALF_UP like Java Formatter (JAVA-DOUBLE-FD-1 FIXED)."""
    table = _table(
        spark.sql(
            "SELECT format_string('%f', CAST('1.0E7' AS DOUBLE)) AS a, "
            "format_string('%.2f', CAST('0.125' AS DOUBLE)) AS b"
        )
    )
    assert table.column("a").to_pylist() == ["10000000.000000"]
    assert table.column("b").to_pylist() == ["0.13"]


def test_spark_door_float_min_max(spark: ReparkSession) -> None:
    """Float.MIN_VALUE spells 1.4E-45 and Float.MAX_VALUE spells 3.4028235E38."""
    table = _table(
        spark.sql(
            "SELECT CAST(CAST('1.4E-45' AS FLOAT) AS STRING) AS a, "
            "CAST(CAST('3.4028235E38' AS FLOAT) AS STRING) AS b"
        )
    )
    assert table.column("a").to_pylist() == ["1.4E-45"]
    assert table.column("b").to_pylist() == ["3.4028235E38"]
    assert table.schema.field("a").type == pa.string()


def test_spark_door_string_double_comparison_is_numeric(spark: ReparkSession) -> None:
    """String-to-double comparison and IN coerce numerically, typed boolean."""
    table = _table(
        spark.sql(
            "SELECT '1.0E7' = CAST('1.0E7' AS DOUBLE) AS a, "
            "'10000000' = CAST('1.0E7' AS DOUBLE) AS b"
        )
    )
    assert table.column("a").to_pylist() == [True]
    assert table.column("b").to_pylist() == [True]
    assert table.schema.field("a").type == pa.bool_()
    table = _table(spark.sql("SELECT '1.0E7' IN (CAST('1.0E7' AS DOUBLE)) AS a"))
    assert table.column("a").to_pylist() == [True]
    assert table.schema.field("a").type == pa.bool_()


def test_spark_door_concat_ws_and_like_use_java_text(spark: ReparkSession) -> None:
    """concat_ws joins and LIKE matches the Java text, typed outputs."""
    table = _table(spark.sql("SELECT concat_ws('-', CAST('1.0E7' AS DOUBLE), 'a') AS a"))
    assert table.column("a").to_pylist() == ["1.0E7-a"]
    assert table.schema.field("a").type == pa.string()
    table = _table(spark.sql("SELECT CAST('1.0E7' AS DOUBLE) LIKE '1.0E7' AS a"))
    assert table.column("a").to_pylist() == [True]
    assert table.schema.field("a").type == pa.bool_()


def test_spark_door_cast_varchar_uses_java_text(spark: ReparkSession) -> None:
    """CAST to VARCHAR/CHAR renders the Java text, typed string."""
    table = _table(
        spark.sql(
            "SELECT CAST(CAST('1.0E7' AS DOUBLE) AS VARCHAR(10)) AS a, "
            "CAST(CAST('1.0E7' AS DOUBLE) AS CHAR(8)) AS b"
        )
    )
    assert table.column("a").to_pylist() == ["1.0E7"]
    assert table.column("b").to_pylist() == ["1.0E7"]
    assert table.schema.field("a").type == pa.string()


def test_spark_door_case_and_coalesce_mix_raise(spark: ReparkSession) -> None:
    """CASE/coalesce mixing a bad string with a double raise CAST_INVALID_INPUT."""
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        spark.sql(
            "SELECT CASE WHEN true THEN 'x' ELSE CAST('1.0E7' AS DOUBLE) END AS a, "
            "CASE WHEN false THEN 'x' ELSE CAST('1.0E7' AS DOUBLE) END AS b"
        ).to_arrow()
    with pytest.raises(Exception, match="CAST_INVALID_INPUT"):
        spark.sql(
            "SELECT coalesce('a', CAST('1.0E7' AS DOUBLE)) AS a, "
            "coalesce(NULL, CAST('1.0E7' AS DOUBLE), 'b') AS b"
        ).to_arrow()


def test_spark_door_decimal_to_json_negzero_shapes(spark: ReparkSession) -> None:
    """Decimals keep Arrow text, to_json renders Java doubles, -0.0 keeps sign."""
    table = _table(
        spark.sql(
            "SELECT CAST(CAST('10000000' AS DECIMAL(10,0)) AS STRING) AS a, "
            "CAST(CAST('0.10' AS DECIMAL(3,2)) AS STRING) AS b"
        )
    )
    assert table.column("a").to_pylist() == ["10000000"]
    assert table.column("b").to_pylist() == ["0.10"]
    table = _table(
        spark.sql(
            "SELECT to_json(named_struct('d', CAST('1.0E7' AS DOUBLE), "
            "'e', CAST('0.1' AS DOUBLE))) AS a"
        )
    )
    assert table.column("a").to_pylist() == ['{"d":1.0E7,"e":0.1}']
    table = _table(
        spark.sql(
            "SELECT CAST(CAST('-0.0' AS DOUBLE) AS STRING) AS a, "
            "concat(CAST('-0.0' AS DOUBLE), '') AS b"
        )
    )
    assert table.column("a").to_pylist() == ["-0.0"]
    assert table.column("b").to_pylist() == ["-0.0"]
    assert table.schema.field("b").type == pa.string()


def test_spark_door_jdk_longhand_cells(spark: ReparkSession) -> None:
    """Shortest-form cells where the JDK agrees pin the equality side."""
    table = _table(
        spark.sql(
            "SELECT CAST(CAST('2.0E-3' AS DOUBLE) AS STRING) AS a, "
            "CAST(CAST('5.0E-324' AS DOUBLE) AS STRING) AS b, "
            "CAST(CAST('2.2250738585072014E-308' AS DOUBLE) AS STRING) AS c, "
            "CAST(CAST('9.007199254740993E15' AS DOUBLE) AS STRING) AS d, "
            "CAST(CAST('1.1' AS DOUBLE) AS STRING) AS e"
        )
    )
    assert table.column("a").to_pylist() == ["0.002"]
    assert table.column("b").to_pylist() == ["4.9E-324"]
    assert table.column("c").to_pylist() == ["2.2250738585072014E-308"]
    assert table.column("d").to_pylist() == ["9.007199254740992E15"]
    assert table.column("e").to_pylist() == ["1.1"]
    assert table.schema.field("a").type == pa.string()


def test_spark_door_jdk_longhand_backlog(spark: ReparkSession) -> None:
    """JDK-longhand cells answer FloatingDecimal text (JAVA-DOUBLE-FD-1 FIXED)."""
    table = _table(
        spark.sql(
            "SELECT CAST(CAST('8.41E21' AS DOUBLE) AS STRING) AS a, "
            "CAST(CAST('1.0E23' AS DOUBLE) AS STRING) AS b"
        )
    )
    assert table.column("a").to_pylist() == ["8.409999999999999E21"]
    assert table.column("b").to_pylist() == ["9.999999999999999E22"]


def test_spark_door_array_join_null_shapes(spark: ReparkSession) -> None:
    """array_join null elements/rows/delimiters follow Spark, typed string."""
    table = _table(
        spark.sql(
            "SELECT array_join(array(CAST('1.0E7' AS DOUBLE), CAST(NULL AS DOUBLE), "
            "CAST('0.1' AS DOUBLE)), ',') AS a"
        )
    )
    assert table.column("a").to_pylist() == ["1.0E7,0.1"]
    assert table.schema.field("a").type == pa.string()
    table = _table(spark.sql("SELECT array_join(CAST(NULL AS ARRAY<DOUBLE>), ',') AS a"))
    assert table.column("a").to_pylist() == [None]
    table = _table(
        spark.sql("SELECT array_join(array(CAST('1.0E7' AS DOUBLE)), CAST(NULL AS STRING)) AS a")
    )
    assert table.column("a").to_pylist() == [None]
    table = _table(
        spark.sql(
            "SELECT array_join(array(CAST('1.0E7' AS DOUBLE), CAST(NULL AS DOUBLE)), "
            "',', 'N/A') AS a"
        )
    )
    assert table.column("a").to_pylist() == ["1.0E7,N/A"]
