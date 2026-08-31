"""FNP-7a/7b — twelve ``try_*`` inversions.

Oracle: live PySpark 4.1.2 (c26-oracle, 2026-08-31). Pins are value AND Arrow
type on ``toArrow()``. Three doors: Spark SQL, native ANSI ``repark.sql()``,
facade Column API.
"""

from __future__ import annotations

import datetime
from decimal import Decimal

import pytest

from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _spark():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("fnp7-try").getOrCreate()


def _arrow(frame):
    return frame.toArrow()


def _sql_arrow(sql: str):
    return _spark().sql(sql).toArrow()


def test_try_divide_by_zero_is_null_double() -> None:
    """pins: fnp-7-try-inversions/C-001"""
    spark = _spark()
    facade = _arrow(spark.range(1).select(F.try_divide(F.lit(1), F.lit(0)).alias("v")))
    door = _sql_arrow("SELECT try_divide(1, 0) AS v")
    for table in (facade, door):
        assert table.column("v").to_pylist() == [None]
        assert str(table.schema.field("v").type) == "double"


def test_try_divide_six_over_two() -> None:
    """pins: fnp-7-try-inversions/C-001"""
    table = _sql_arrow("SELECT try_divide(CAST(6 AS INT), CAST(2 AS INT)) AS v")
    assert table.column("v").to_pylist() == [3.0]
    assert str(table.schema.field("v").type) == "double"


def test_try_mod_by_zero_is_null() -> None:
    """pins: fnp-7-try-inversions/C-002"""
    spark = _spark()
    facade = _arrow(
        spark.range(1).select(F.try_mod(F.lit(7).cast("int"), F.lit(0).cast("int")).alias("v"))
    )
    door = _sql_arrow("SELECT try_mod(CAST(7 AS INT), CAST(0 AS INT)) AS v")
    for table in (facade, door):
        assert table.column("v").to_pylist() == [None]
        assert str(table.schema.field("v").type) == "int32"


def test_try_mod_seven_mod_three() -> None:
    """pins: fnp-7-try-inversions/C-002"""
    table = _sql_arrow("SELECT try_mod(CAST(7 AS INT), CAST(3 AS INT)) AS v")
    assert table.column("v").to_pylist() == [1]
    assert str(table.schema.field("v").type) == "int32"


def test_try_element_at_array_edges() -> None:
    """pins: fnp-7-try-inversions/C-003"""
    spark = _spark()
    frame = spark.sql("SELECT array(10, 20, 30) AS a")
    one = _arrow(frame.select(F.try_element_at("a", 1).alias("v")))
    assert one.column("v").to_pylist() == [10]
    neg = _arrow(frame.select(F.try_element_at("a", -1).alias("v")))
    assert neg.column("v").to_pylist() == [30]
    oob = _arrow(frame.select(F.try_element_at("a", 4).alias("v")))
    assert oob.column("v").to_pylist() == [None]
    door = _sql_arrow("SELECT try_element_at(array(10, 20, 30), 4) AS v")
    assert door.column("v").to_pylist() == [None]


def test_try_element_at_index_zero_raises() -> None:
    """pins: fnp-7-try-inversions/C-003"""
    with pytest.raises(PySparkException, match="INVALID_INDEX_OF_ZERO"):
        _sql_arrow("SELECT try_element_at(array(10, 20, 30), 0) AS v")


def test_try_element_at_map_miss_is_null() -> None:
    """pins: fnp-7-try-inversions/C-003"""
    table = _sql_arrow("SELECT try_element_at(map('a', 1, 'b', 2), 'z') AS v")
    assert table.column("v").to_pylist() == [None]


def test_try_to_date_malformed_and_null() -> None:
    """pins: fnp-7-try-inversions/C-004"""
    spark = _spark()
    bad = _arrow(spark.range(1).select(F.try_to_date(F.lit("not-a-date")).alias("v")))
    good = _arrow(spark.range(1).select(F.try_to_date(F.lit("2024-01-15")).alias("v")))
    null = _sql_arrow("SELECT try_to_date(CAST(NULL AS STRING)) AS v")
    assert bad.column("v").to_pylist() == [None]
    assert good.column("v").to_pylist() == [datetime.date(2024, 1, 15)]
    assert "date" in str(good.schema.field("v").type).lower()
    assert null.column("v").to_pylist() == [None]


def test_try_to_date_format_mismatch_is_null() -> None:
    """pins: fnp-7-try-inversions/C-004"""
    table = _sql_arrow("SELECT try_to_date('2024-01-15', 'dd/MM/yyyy') AS v")
    assert table.column("v").to_pylist() == [None]
    parsed = _sql_arrow("SELECT try_to_date('15/01/2024', 'dd/MM/yyyy') AS v")
    assert parsed.column("v").to_pylist() == [datetime.date(2024, 1, 15)]


def test_try_to_date_illegal_pattern_raises() -> None:
    """pins: fnp-7-try-inversions/C-004"""
    with pytest.raises(PySparkException, match="INVALID_DATETIME_PATTERN"):
        _sql_arrow("SELECT try_to_date('2024-01-15', 'not-a-format') AS v")


def test_try_to_number_match_and_mismatch() -> None:
    """pins: fnp-7-try-inversions/C-005"""
    spark = _spark()
    good = _arrow(spark.range(1).select(F.try_to_number(F.lit("123"), "999").alias("v")))
    assert good.column("v").to_pylist() == [Decimal("123")]
    money = _sql_arrow("SELECT try_to_number('$1,234.56', '$999,999.99') AS v")
    assert money.column("v").to_pylist() == [Decimal("1234.56")]
    bad = _sql_arrow("SELECT try_to_number('abc', '999') AS v")
    assert bad.column("v").to_pylist() == [None]


def test_try_to_number_bad_format_raises() -> None:
    """pins: fnp-7-try-inversions/C-005"""
    with pytest.raises(PySparkException, match="INVALID_FORMAT"):
        _sql_arrow("SELECT try_to_number('123', 'not-a-format') AS v")


def test_try_to_binary_hex_and_failure() -> None:
    """pins: fnp-7-try-inversions/C-006"""
    spark = _spark()
    hexed = _arrow(spark.range(1).select(F.try_to_binary(F.lit("616263"), "hex").alias("v")))
    assert hexed.column("v").to_pylist() == [b"abc"]
    default = _sql_arrow("SELECT try_to_binary('61') AS v")
    assert default.column("v").to_pylist() == [b"a"]
    bad = _sql_arrow("SELECT try_to_binary('zz', 'hex') AS v")
    assert bad.column("v").to_pylist() == [None]
    utf = _sql_arrow("SELECT try_to_binary('abc', 'utf-8') AS v")
    assert utf.column("v").to_pylist() == [b"abc"]


def test_try_to_time_matches_spark_unsupported() -> None:
    """pins: fnp-7-try-inversions/C-007"""
    with pytest.raises(PySparkException, match="UNSUPPORTED_TIME_TYPE"):
        _sql_arrow("SELECT try_to_time('12:34:56') AS v")
    with pytest.raises(PySparkException, match="UNSUPPORTED_TIME_TYPE"):
        _spark().range(1).select(F.try_to_time(F.lit("12:34:56")).alias("v")).toArrow()


def test_try_sum_and_overflow() -> None:
    """pins: fnp-7-try-inversions/C-008"""
    spark = _spark()
    frame = spark.sql("SELECT * FROM VALUES (1), (2), (3) AS t(v)")
    summed = _arrow(frame.select(F.try_sum("v").alias("v")))
    assert summed.column("v").to_pylist() == [6]
    assert str(summed.schema.field("v").type) == "int64"
    door = _sql_arrow("SELECT try_sum(v) AS v FROM VALUES (1), (2), (3) AS t(v)")
    assert door.column("v").to_pylist() == [6]
    overflow = _sql_arrow(
        "SELECT try_sum(v) AS v FROM VALUES "
        "(CAST(9223372036854775807 AS BIGINT)), (CAST(1 AS BIGINT)) AS t(v)"
    )
    assert overflow.column("v").to_pylist() == [None]


def test_try_add_overflow_int_and_smallint() -> None:
    """pins: fnp-7-try-inversions/C-009, C-014"""
    spark = _spark()
    facade = _arrow(
        spark.range(1).select(
            F.try_add(F.lit(2147483647).cast("int"), F.lit(1).cast("int")).alias("v")
        )
    )
    assert facade.column("v").to_pylist() == [None]
    assert str(facade.schema.field("v").type) == "int32"
    door = _sql_arrow("SELECT try_add(CAST(2147483647 AS INT), CAST(1 AS INT)) AS v")
    assert door.column("v").to_pylist() == [None]
    small = _sql_arrow("SELECT try_add(CAST(32767 AS SMALLINT), CAST(1 AS SMALLINT)) AS v")
    assert small.column("v").to_pylist() == [None]
    assert str(small.schema.field("v").type) == "int16"


def test_try_subtract_int_min() -> None:
    """pins: fnp-7-try-inversions/C-010"""
    table = _sql_arrow("SELECT try_subtract(CAST(-2147483648 AS INT), CAST(1 AS INT)) AS v")
    assert table.column("v").to_pylist() == [None]


def test_try_multiply_overflow() -> None:
    """pins: fnp-7-try-inversions/C-011"""
    table = _sql_arrow("SELECT try_multiply(CAST(2147483647 AS INT), CAST(2 AS INT)) AS v")
    assert table.column("v").to_pylist() == [None]


def test_try_avg_mean_and_type() -> None:
    """pins: fnp-7-try-inversions/C-012"""
    spark = _spark()
    frame = spark.sql("SELECT * FROM VALUES (1), (2), (3) AS t(v)")
    averaged = _arrow(frame.select(F.try_avg("v").alias("v")))
    assert averaged.column("v").to_pylist() == [2.0]
    assert str(averaged.schema.field("v").type) == "double"
    door = _sql_arrow("SELECT try_avg(v) AS v FROM VALUES (1), (2), (3) AS t(v)")
    assert door.column("v").to_pylist() == [2.0]


def test_try_add_interval_day() -> None:
    """pins: fnp-7-try-inversions/C-015"""
    table = _sql_arrow("SELECT try_add(INTERVAL 1 DAY, INTERVAL 1 DAY) AS v")
    values = table.column("v").to_pylist()
    assert len(values) == 1
    assert values[0] is not None


def test_try_names_are_present() -> None:
    """pins: fnp-7-try-inversions/C-013"""
    for name in (
        "try_add",
        "try_avg",
        "try_divide",
        "try_element_at",
        "try_mod",
        "try_multiply",
        "try_subtract",
        "try_sum",
        "try_to_binary",
        "try_to_date",
        "try_to_number",
        "try_to_time",
    ):
        assert hasattr(F, name)
