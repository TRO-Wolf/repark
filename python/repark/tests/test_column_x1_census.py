"""X1 — Column surface unblocks (between, pow, string predicates, bitwise, lit temporal)."""

from __future__ import annotations

import datetime
from enum import IntEnum

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.spark.column import Column
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import IntegerType, LongType


@pytest.fixture
def spark() -> ReparkSession:
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("test-column-x1").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def test_between_filters(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, 2, 3), (2, 1, 3), (4, 1, 4)], ["a", "b", "c"])
    rows = frame.filter(frame.a.between(frame.b, frame.c)).collect()
    assert [(row.a, row.b, row.c) for row in rows] == [(2, 1, 3), (4, 1, 4)]


def test_pow_and_rpow(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(2,), (3,)], ["x"])
    values = [row[0] for row in frame.select(frame.x**2).collect()]
    assert values == [4.0, 9.0]
    reflected = [row[0] for row in frame.select(2**frame.x).collect()]
    assert reflected == [4.0, 8.0]


def test_string_predicates_and_like(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([("abc",), ("xyz",)], ["s"])
    assert frame.filter(frame.s.startswith("a")).count() == 1
    assert frame.filter(frame.s.endswith("z")).count() == 1
    assert frame.filter(frame.s.contains("b")).count() == 1
    assert frame.filter(frame.s.like("a%")).count() == 1
    assert frame.filter(frame.s.ilike("A%")).count() == 1
    assert frame.filter(frame.s.rlike("^a")).count() == 1


def test_bitwise_and_eqnullsafe(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,), (3,)], ["x"])
    bits = [row[0] for row in frame.select(frame.x.bitwiseAND(1)).collect()]
    assert bits == [1, 1]
    nullsafe = spark.createDataFrame([(1, 1), (None, None)], ["a", "b"])
    assert nullsafe.filter(nullsafe.a.eqNullSafe(nullsafe.b)).count() == 2


def test_bitwise_or_xor_values(spark: ReparkSession) -> None:
    """bitwiseOR / bitwiseXOR Arrow values (octo C1)."""
    frame = spark.createDataFrame([(1,), (3,)], ["x"])
    assert [row[0] for row in frame.select(frame.x.bitwiseOR(2)).collect()] == [3, 3]
    assert [row[0] for row in frame.select(frame.x.bitwiseXOR(1)).collect()] == [0, 2]


def test_eqnullsafe_none_scalar_and_between_bounds(spark: ReparkSession) -> None:
    """eqNullSafe(None) + between inclusive/inverted (octo C2)."""
    frame = spark.createDataFrame([(1,), (None,)], ["a"])
    assert frame.filter(frame.a.eqNullSafe(None)).count() == 1
    assert frame.filter(frame.a.eqNullSafe(1)).count() == 1
    ids = spark.range(5)
    assert ids.filter(F.col("id").between(1, 3)).count() == 3
    assert ids.filter(F.col("id").between(2, 2)).count() == 1
    assert ids.filter(F.col("id").between(3, 1)).count() == 0


def test_reflected_bool_and(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(True,), (False,)], ["b"])
    # True & col — needs Column.__rand__
    assert isinstance(True & frame.b, Column)
    assert frame.filter(True & frame.b).count() == 1


def test_reflected_bool_or(spark: ReparkSession) -> None:
    """False | col / True | col — Column.__ror__ (octo C5)."""
    frame = spark.createDataFrame([(True,), (False,)], ["b"])
    assert isinstance(False | frame.b, Column)
    assert frame.filter(False | frame.b).count() == 1
    assert frame.filter(True | frame.b).count() == 2


def test_array_from_column_names(spark: ReparkSession) -> None:
    """F.array accepts column-name strings (octo C5)."""
    frame = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    assert list(frame.select(F.array("a", "b", "c").alias("x")).collect()[0][0]) == [1, 2, 3]


def test_cast_longtype(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,)], ["x"])
    casted = frame.select(frame.x.cast(LongType()))
    assert isinstance(casted, type(frame))
    assert casted.collect()[0][0] == 1


def test_lit_date_and_enum(spark: ReparkSession) -> None:
    class Color(IntEnum):
        RED = 1

    frame = spark.range(1).select(
        F.lit(datetime.date(2017, 11, 6)).alias("d"),
        F.lit(Color.RED).alias("e"),
        F.lit(datetime.datetime(2017, 11, 6, 12, 0, 0)).alias("ts"),
    )
    row = frame.collect()[0]
    assert row.d == datetime.date(2017, 11, 6)
    assert row.e == 1
    assert row.ts is not None


def test_lit_time_list_and_empty_array(spark: ReparkSession) -> None:
    """lit(time) / lit(list) / F.array() zero-arg (octo C1)."""
    frame = spark.range(1).select(
        F.lit(datetime.time(12, 30, 45)).alias("t"),
        F.lit([1, 2, 3]).alias("arr"),
        F.array().alias("empty"),
    )
    row = frame.collect()[0]
    assert row.t == datetime.time(12, 30, 45)
    assert list(row.arr) == [1, 2, 3]
    assert list(row.empty) == []


def test_hour_minute_second_on_time(spark: ReparkSession) -> None:
    """Apache test_hour|minute|second — extractors on lit(time) (octo C3)."""
    frame = spark.range(1).select(F.lit(datetime.time(12, 34, 56)).alias("time"))
    assert frame.select(F.hour(frame.time)).collect()[0][0] == 12
    assert frame.select(F.hour("time")).collect()[0][0] == 12
    assert frame.select(F.minute(frame.time)).collect()[0][0] == 34
    assert frame.select(F.second(frame.time)).collect()[0][0] == 56
    # Timestamp path still works.
    ts = spark.range(1).select(F.lit(datetime.datetime(2017, 11, 6, 15, 16, 17)).alias("ts"))
    assert ts.select(F.hour("ts")).collect()[0][0] == 15


def test_date_add_months_string_count_column(spark: ReparkSession) -> None:
    """date_add/add_months/date_sub accept column-name str counts (SPARK-37738 / octo C3/C4)."""
    day = datetime.date(2021, 12, 27)
    frame = spark.createDataFrame([(day, 2)], schema="date date, add int")
    assert frame.select(F.date_add(frame.date, "add")).collect()[0][0] == datetime.date(
        2021, 12, 29
    )
    assert frame.select(F.date_add(frame.date, 3)).collect()[0][0] == datetime.date(2021, 12, 30)
    month_day = datetime.date(2021, 11, 27)
    months = spark.createDataFrame([(month_day, 2)], schema="date date, add int")
    assert months.select(F.add_months(months.date, "add")).collect()[0][0] == datetime.date(
        2022, 1, 27
    )
    sub_frame = spark.createDataFrame([(day, 2)], schema="date date, sub int")
    assert sub_frame.select(F.date_sub(sub_frame.date, "sub")).collect()[0][0] == datetime.date(
        2021, 12, 25
    )
    assert sub_frame.select(F.date_sub(sub_frame.date, 3)).collect()[0][0] == datetime.date(
        2021, 12, 24
    )


def test_longtype_api_surface() -> None:
    """LongType typeName/simpleString/engine (X1 + octo C3)."""
    assert LongType().typeName() == "long"
    assert LongType().simpleString() == "bigint"
    assert LongType()._engine_type() == "long"
    assert LongType() == LongType()
    assert LongType() != IntegerType()


def test_lit_enum_list_value(spark: ReparkSession) -> None:
    """Enum whose .value is a sequence → array column (octo C7)."""
    from enum import Enum

    class Payload(Enum):
        PAIR = (1, 2)

    row = spark.range(1).select(F.lit(Payload.PAIR).alias("a")).collect()[0]
    assert list(row.a) == [1, 2]


def test_inverse_trig_domain(spark: ReparkSession) -> None:
    """acos/asin domain pins for Apache inverse_trig PASS (octo C7)."""
    import math

    frame = spark.createDataFrame([(0.0,), (1.0,), (-1.0,)], ["a"])
    acos_vals = [row[0] for row in frame.select(F.acos("a")).collect()]
    asin_vals = [row[0] for row in frame.select(F.asin("a")).collect()]
    assert abs(acos_vals[0] - math.pi / 2) < 1e-9
    assert abs(acos_vals[1] - 0.0) < 1e-9
    assert abs(acos_vals[2] - math.pi) < 1e-9
    assert abs(asin_vals[0] - 0.0) < 1e-9
    assert abs(asin_vals[1] - math.pi / 2) < 1e-9
    assert abs(asin_vals[2] + math.pi / 2) < 1e-9


def test_trig_and_dayname(spark: ReparkSession) -> None:
    import math

    frame = spark.createDataFrame([(0.0,), (math.pi / 2,)], ["a"])
    cos_rows = [row[0] for row in frame.select(F.cos(frame.a)).collect()]
    assert abs(cos_rows[0] - 1.0) < 1e-9
    assert abs(cos_rows[1] - 0.0) < 1e-9
    hypot_val = frame.select(F.hypot(frame.a, frame.a)).collect()[0][0]
    assert abs(hypot_val - 0.0) < 1e-9
    # Non-degenerate Euclidean norm (3-4-5) + Apache lit-second forms (octo C6).
    right = spark.createDataFrame([(3.0, 4.0)], ["a", "b"])
    assert abs(right.select(F.hypot("a", "b")).collect()[0][0] - 5.0) < 1e-9
    assert abs(right.select(F.hypot("a", 2)).collect()[0][0] - math.hypot(3.0, 2)) < 1e-9
    assert abs(right.select(F.hypot(right.a, 2)).collect()[0][0] - math.hypot(3.0, 2)) < 1e-9
    assert abs(right.select(F.pow(right.a, 2)).collect()[0][0] - 9.0) < 1e-9
    assert abs(right.select(F.pow(right.a, 2.0)).collect()[0][0] - 9.0) < 1e-9
    dated = spark.createDataFrame([(datetime.datetime(2017, 11, 6),)], ["date"])
    assert dated.select(F.dayname(dated.date)).collect()[0][0] == "Mon"
    assert dated.select(F.monthname(dated.date)).collect()[0][0] == "Nov"
    # date (not datetime) argument path for dayname/monthname.
    only_date = spark.range(1).select(F.dayname(F.lit(datetime.date(2017, 11, 6))).alias("d"))
    assert only_date.collect()[0][0] == "Mon"
