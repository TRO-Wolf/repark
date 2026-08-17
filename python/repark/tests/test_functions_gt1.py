"""FN-GT1 — leftover THIN-WIRE math / string / bitwise / utf8 (value + Arrow type).

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow
path (``to_arrow()``): value AND type. ``getbit`` is the PySpark ``__all__``
alias of ``bit_get`` and ships with it.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-gt1").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _is_string(field_type: pa.DataType) -> bool:
    return pa.types.is_string(field_type) or pa.types.is_large_string(field_type)


def test_bin_hex_unhex(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.bin(F.lit(13)).alias("b"),
            F.hex(F.lit(17)).alias("h"),
            F.unhex(F.lit("48656C6C6F")).alias("u"),
        )
    )
    assert table.column("b").to_pylist() == ["1101"]
    assert _is_string(table.schema.field("b").type)
    assert table.column("h").to_pylist() == ["11"]
    assert _is_string(table.schema.field("h").type)
    raw = table.column("u").to_pylist()[0]
    assert bytes(raw) == b"Hello"
    assert pa.types.is_binary(table.schema.field("u").type) or pa.types.is_large_binary(
        table.schema.field("u").type
    )


def test_factorial_domain(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(0,), (5,), (20,), (21,), (-1,)], ["n"])
    table = _table(frame.select(F.factorial("n").alias("f")))
    assert table.column("f").to_pylist() == [1, 120, 2432902008176640000, None, None]
    assert pa.types.is_int64(table.schema.field("f").type)


def test_rint(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1.2,), (1.5,), (-1.5,), (2.5,)], ["x"])
    table = _table(frame.select(F.rint("x").alias("r")))
    # Java Math.rint / Spark rint: ties to even.
    assert table.column("r").to_pylist() == [1.0, 2.0, -2.0, 2.0]
    assert pa.types.is_floating(table.schema.field("r").type)


def test_width_bucket(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(F.width_bucket(F.lit(5.0), F.lit(0.0), F.lit(10.0), F.lit(5)).alias("w"))
    )
    assert table.column("w").to_pylist() == [3]
    assert pa.types.is_integer(table.schema.field("w").type)


def test_bit_count_and_bit_get(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(7,), (6,)], ["x"])
    table = _table(
        frame.select(
            F.bit_count("x").alias("c"),
            F.bit_get("x", 1).alias("g"),
            F.getbit("x", 1).alias("a"),
        )
    )
    assert table.column("c").to_pylist() == [3, 2]
    assert table.column("g").to_pylist() == [1, 1]
    assert table.column("a").to_pylist() == table.column("g").to_pylist()
    assert pa.types.is_integer(table.schema.field("c").type)
    assert table.schema.field("g").type == table.schema.field("a").type


def test_shifts(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.shiftleft(F.lit(2), 1).alias("l"),
            F.shiftright(F.lit(-2), 1).alias("r"),
            F.shiftrightunsigned(F.lit(8), 1).alias("u"),
        )
    )
    assert table.column("l").to_pylist() == [4]
    assert table.column("r").to_pylist() == [-1]
    assert table.column("u").to_pylist() == [4]
    assert pa.types.is_integer(table.schema.field("l").type)
    assert pa.types.is_integer(table.schema.field("r").type)
    assert pa.types.is_integer(table.schema.field("u").type)


def test_split_part_and_regexp(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.split_part(F.lit("a.b.c"), ".", 2).alias("p"),
            F.regexp_count(F.lit("ababab"), "ab").alias("c"),
            F.regexp_instr(F.lit("abcde"), "c").alias("i"),
        )
    )
    assert table.column("p").to_pylist() == ["b"]
    assert _is_string(table.schema.field("p").type)
    assert table.column("c").to_pylist() == [3]
    assert pa.types.is_integer(table.schema.field("c").type)
    assert table.column("i").to_pylist() == [3]
    assert pa.types.is_integer(table.schema.field("i").type)


def test_bit_length_and_octet_length(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.bit_length(F.lit("ab")).alias("b"),
            F.octet_length(F.lit("ab")).alias("o"),
        )
    )
    assert table.column("b").to_pylist() == [16]
    assert table.column("o").to_pylist() == [2]
    assert pa.types.is_integer(table.schema.field("b").type)
    assert pa.types.is_integer(table.schema.field("o").type)


def test_utf8_valid(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.is_valid_utf8(F.lit("ok")).alias("v"),
            F.make_valid_utf8(F.lit("ok")).alias("m"),
        )
    )
    assert table.column("v").to_pylist() == [True]
    assert pa.types.is_boolean(table.schema.field("v").type)
    assert table.column("m").to_pylist() == ["ok"]
    assert _is_string(table.schema.field("m").type)
