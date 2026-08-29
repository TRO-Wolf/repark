"""FN-A — ordering / null / math facade wrappers (value + Arrow type).

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow path
(``to_arrow()``): value AND type. Alias names resolve and share a behavior case
with their canonical. SEMANTIC-HAZARD names that shipped (``cbrt`` negatives)
hit the named hazard.
"""

from __future__ import annotations

import math

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-a").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


# Aliases: resolve + one behavior case


def test_sign_alias_of_signum(spark: ReparkSession) -> None:
    assert callable(F.sign)
    frame = spark.createDataFrame([(-3.0,), (0.0,), (2.0,)], ["x"])
    table = _table(frame.select(F.sign("x").alias("s"), F.signum("x").alias("g")))
    assert table.column("s").to_pylist() == table.column("g").to_pylist()
    assert table.schema.field("s").type == table.schema.field("g").type
    assert table.column("s").to_pylist() == [-1.0, 0.0, 1.0]
    assert pa.types.is_floating(table.schema.field("s").type)


def test_ifnull_and_nvl_are_two_arg_coalesce(spark: ReparkSession) -> None:
    assert callable(F.ifnull)
    assert callable(F.nvl)
    frame = spark.createDataFrame([(None,), ("a",)], ["s"])
    table = _table(
        frame.select(
            F.ifnull("s", F.lit("z")).alias("i"),
            F.nvl("s", F.lit("z")).alias("n"),
            F.coalesce(F.col("s"), F.lit("z")).alias("c"),
        )
    )
    assert table.column("i").to_pylist() == ["z", "a"]
    assert table.column("n").to_pylist() == ["z", "a"]
    assert table.column("c").to_pylist() == ["z", "a"]
    assert pa.types.is_string(table.schema.field("i").type) or pa.types.is_large_string(
        table.schema.field("i").type
    )


def test_ln_alias_of_log(spark: ReparkSession) -> None:
    assert callable(F.ln)
    frame = spark.createDataFrame([(math.e,)], ["x"])
    table = _table(frame.select(F.ln("x").alias("ln"), F.log("x").alias("lg")))
    assert abs(table.column("ln").to_pylist()[0] - 1.0) < 1e-9
    assert abs(table.column("lg").to_pylist()[0] - 1.0) < 1e-9
    assert table.schema.field("ln").type == table.schema.field("lg").type
    assert pa.types.is_floating(table.schema.field("ln").type)


def test_asc_nulls_first_and_desc_nulls_last_are_aliases(spark: ReparkSession) -> None:
    assert callable(F.asc_nulls_first)
    assert callable(F.desc_nulls_last)
    frame = spark.createDataFrame([(1,), (None,), (2,)], ["a"])
    asc_rows = _table(frame.orderBy(F.asc_nulls_first("a"))).column("a").to_pylist()
    desc_rows = _table(frame.orderBy(F.desc_nulls_last("a"))).column("a").to_pylist()
    assert asc_rows == [None, 1, 2]
    assert desc_rows == [2, 1, None]


# Ordering SHIMs


def test_asc_and_desc_null_ordering(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,), (None,), (2,)], ["a"])
    asc_table = _table(frame.orderBy(F.asc("a")))
    desc_table = _table(frame.orderBy(F.desc("a")))
    assert asc_table.column("a").to_pylist() == [None, 1, 2]
    assert desc_table.column("a").to_pylist() == [2, 1, None]
    assert pa.types.is_integer(asc_table.schema.field("a").type)


# Constants + unary


def test_e_and_pi_are_foldable_constants(spark: ReparkSession) -> None:
    table = _table(spark.range(1).select(F.e().alias("e"), F.pi().alias("p")))
    assert abs(table.column("e").to_pylist()[0] - math.e) < 1e-12
    assert abs(table.column("p").to_pylist()[0] - math.pi) < 1e-12
    assert pa.types.is_floating(table.schema.field("e").type)
    assert pa.types.is_floating(table.schema.field("p").type)


def test_e_stays_double_beside_an_aggregate(spark: ReparkSession) -> None:
    """``lit(math.e)`` re-embeds as DECIMAL on the SQL global-agg path; ``e()`` must not."""
    table = _table(
        spark.createDataFrame([(1,), (2,)], ["x"]).select(F.sum("x").alias("s"), F.e().alias("e"))
    )
    assert pa.types.is_floating(table.schema.field("e").type)
    assert not pa.types.is_decimal(table.schema.field("e").type)
    assert abs(float(table.column("e").to_pylist()[0]) - math.e) < 1e-12
    assert table.column("s").to_pylist() == [3]


def test_negative_and_positive(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(3,), (-4,)], ["x"])
    table = _table(
        frame.select(F.negative("x").alias("n"), F.positive("x").alias("p"), F.col("x").alias("x"))
    )
    assert table.column("n").to_pylist() == [-3, 4]
    assert table.column("p").to_pylist() == [3, -4]
    assert table.schema.field("n").type == table.schema.field("x").type
    assert table.schema.field("p").type == table.schema.field("x").type


# Math SHIMs / THIN-WIRE stand-ins


def test_pmod_positive_remainder(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(10, 3), (-10, 3), (10, -3)], ["a", "b"])
    table = _table(frame.select(F.pmod("a", "b").alias("p")))
    assert table.column("p").to_pylist() == [1, 2, -2]
    assert pa.types.is_integer(table.schema.field("p").type)


def test_expm1_ln_log2_log1p(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(0.0,), (1.0,), (7.0,)], ["x"])
    table = _table(
        frame.select(
            F.expm1("x").alias("em"),
            F.log2(F.lit(8.0)).alias("l2"),
            F.log1p("x").alias("l1"),
        )
    )
    rows = table.to_pylist()
    assert abs(rows[0]["em"] - 0.0) < 1e-12
    assert abs(rows[1]["em"] - (math.e - 1.0)) < 1e-9
    assert abs(rows[0]["l2"] - 3.0) < 1e-9
    assert abs(rows[0]["l1"] - 0.0) < 1e-12
    assert pa.types.is_floating(table.schema.field("em").type)
    assert pa.types.is_floating(table.schema.field("l2").type)
    assert pa.types.is_floating(table.schema.field("l1").type)


def test_degrees_and_radians(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(math.pi,), (0.0,)], ["r"])
    table = _table(
        frame.select(
            F.degrees("r").alias("d"),
            F.radians(F.lit(180.0)).alias("rad"),
        )
    )
    rows = table.to_pylist()
    assert abs(rows[0]["d"] - 180.0) < 1e-9
    assert abs(rows[1]["d"] - 0.0) < 1e-12
    assert abs(rows[0]["rad"] - math.pi) < 1e-9
    assert pa.types.is_floating(table.schema.field("d").type)
    assert pa.types.is_floating(table.schema.field("rad").type)


def test_cbrt_real_root_including_negatives(spark: ReparkSession) -> None:
    """Hazard: IEEE ``pow(x, 1/3)`` is NaN on negatives; Spark ``cbrt`` is real."""
    frame = spark.createDataFrame([(8.0,), (-8.0,), (0.0,), (None,)], ["x"])
    table = _table(frame.select(F.cbrt("x").alias("c")))
    values = table.column("c").to_pylist()
    assert abs(values[0] - 2.0) < 1e-9
    assert abs(values[1] - (-2.0)) < 1e-9
    assert values[2] == 0.0
    assert values[3] is None
    assert pa.types.is_floating(table.schema.field("c").type)


# Null helpers


def test_nvl2_picks_present_or_absent(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(None,), ("a",)], ["s"])
    table = _table(frame.select(F.nvl2("s", F.lit("yes"), F.lit("no")).alias("v")))
    assert table.column("v").to_pylist() == ["no", "yes"]
    assert pa.types.is_string(table.schema.field("v").type) or pa.types.is_large_string(
        table.schema.field("v").type
    )


def test_nullif_and_nullifzero(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,), (0,)], ["x"])
    table = _table(
        frame.select(
            F.nullif("x", F.lit(1)).alias("n"),
            F.nullifzero("x").alias("z"),
        )
    )
    assert table.column("n").to_pylist() == [None, 2, 0]
    assert table.column("z").to_pylist() == [1, 2, None]
    assert pa.types.is_integer(table.schema.field("n").type)
    assert pa.types.is_integer(table.schema.field("z").type)


def test_equal_null_is_null_safe(spark: ReparkSession) -> None:
    frame = spark.createDataFrame(
        [(None, None), (1, 1), (1, 2), (None, 1)],
        ["a", "b"],
    )
    table = _table(frame.select(F.equal_null("a", "b").alias("e")))
    assert table.column("e").to_pylist() == [True, True, False, False]
    assert pa.types.is_boolean(table.schema.field("e").type)


def test_zeroifnull_and_isnotnull(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(None,), (5,)], ["x"])
    table = _table(
        frame.select(
            F.zeroifnull("x").alias("z"),
            F.isnotnull("x").alias("nn"),
            F.isnull("x").alias("n"),
        )
    )
    assert table.column("z").to_pylist() == [0, 5]
    assert table.column("nn").to_pylist() == [False, True]
    assert table.column("n").to_pylist() == [True, False]
    assert pa.types.is_integer(table.schema.field("z").type)
    assert pa.types.is_boolean(table.schema.field("nn").type)
