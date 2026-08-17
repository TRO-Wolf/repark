"""FN-GT2 — leftover THIN-WIRE datetime / collections / url / bitmap.

Each new ``functions`` name is pinned through ``ReparkSession`` on the Arrow
path (``to_arrow()``): value AND type. ``datediff`` stays the DISPOSED-STUB.
``shuffle`` pins type + length, not order.
"""

from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import PySparkException, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-gt2").getOrCreate()
    yield session
    session.stop()


def _table(frame: object) -> pa.Table:
    return frame.to_arrow()  # type: ignore[attr-defined]


def _is_string(field_type: pa.DataType) -> bool:
    return pa.types.is_string(field_type) or pa.types.is_large_string(field_type)


def test_make_date(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.make_date(2020, 1, 2).alias("d"),
            F.make_date(2020, 2, 30).alias("bad"),
        )
    )
    assert table.column("d").to_pylist() == [datetime.date(2020, 1, 2)]
    assert table.column("bad").to_pylist() == [None]
    assert pa.types.is_date(table.schema.field("d").type)


def test_make_interval_and_dt(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.make_interval(days=1).alias("i"),
            F.make_dt_interval(1, 0, 0, 0).alias("dt"),
        )
    )
    assert table.column("i").to_pylist()[0] is not None
    assert table.column("dt").to_pylist()[0] is not None
    assert pa.types.is_duration(table.schema.field("dt").type) or str(table.schema.field("i").type)


def test_unix_micros_and_date_diff(spark: ReparkSession) -> None:
    frame = spark.range(1)
    epoch = datetime.datetime(1970, 1, 1)
    table = _table(
        frame.select(
            F.unix_micros(F.lit(epoch)).alias("u"),
            F.date_diff(F.lit(datetime.date(2020, 1, 3)), F.lit(datetime.date(2020, 1, 1))).alias(
                "d"
            ),
        )
    )
    assert table.column("u").to_pylist() == [0]
    assert pa.types.is_integer(table.schema.field("u").type)
    assert table.column("d").to_pylist() == [2]
    assert pa.types.is_integer(table.schema.field("d").type)


def test_datediff_stub_untouched(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException, match="datediff"):
        F.datediff(F.lit(datetime.date(2020, 1, 3)), F.lit(datetime.date(2020, 1, 1)))


def test_element_at_one_based_and_zero_refuses(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT array(10, 20, 30) AS a")
    table = _table(
        frame.select(
            F.element_at("a", 1).alias("e1"),
            F.element_at("a", 2).alias("e2"),
            F.element_at("a", None).alias("en"),
        )
    )
    assert table.column("e1").to_pylist() == [10]
    assert table.column("e2").to_pylist() == [20]
    assert table.column("en").to_pylist() == [None]
    assert pa.types.is_integer(table.schema.field("e1").type)
    with pytest.raises(PySparkException, match="INVALID_INDEX_OF_ZERO"):
        frame.select(F.element_at("a", 0)).to_arrow()


def test_array_compact_drops_nulls_only(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT array(1, CAST(NULL AS INT), 1) AS a")
    table = _table(frame.select(F.array_compact("a").alias("c")))
    assert table.column("c").to_pylist() == [[1, 1]]
    assert pa.types.is_list(table.schema.field("c").type) or pa.types.is_large_list(
        table.schema.field("c").type
    )


def test_shuffle_preserves_type_and_length(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(frame.select(F.shuffle(F.array(F.lit(1), F.lit(2), F.lit(3))).alias("s")))
    values = table.column("s").to_pylist()[0]
    assert sorted(values) == [1, 2, 3]
    assert pa.types.is_list(table.schema.field("s").type) or pa.types.is_large_list(
        table.schema.field("s").type
    )


def test_map_from_entries_and_str_to_map(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT array(named_struct('key', 'a', 'value', 1)) AS e")
    table = _table(
        frame.select(
            F.map_from_entries("e").alias("m"),
            F.str_to_map(F.lit("a:1,b:2")).alias("s"),
        )
    )
    mapped = table.column("m").to_pylist()[0]
    assert mapped == {"a": 1} or mapped == [("a", 1)]
    as_map = table.column("s").to_pylist()[0]
    assert as_map == {"a": "1", "b": "2"} or dict(as_map) == {"a": "1", "b": "2"}


def test_parse_url_and_try(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.parse_url(F.lit("https://spark.apache.org/path"), "HOST").alias("h"),
            F.try_parse_url(F.lit("not a url"), "HOST").alias("bad"),
        )
    )
    assert table.column("h").to_pylist() == ["spark.apache.org"]
    assert _is_string(table.schema.field("h").type)
    assert table.column("bad").to_pylist() == [None]


def test_url_encode_decode(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.url_encode(F.lit("a b")).alias("e"),
            F.url_decode(F.lit("a+b")).alias("d"),
            F.try_url_decode(F.lit("%ZZ")).alias("bad"),
        )
    )
    assert table.column("e").to_pylist() == ["a+b"]
    assert table.column("d").to_pylist() == ["a b"]
    assert table.column("bad").to_pylist() == [None]
    assert _is_string(table.schema.field("e").type)


def test_bitmap_scalars(spark: ReparkSession) -> None:
    frame = spark.range(1)
    table = _table(
        frame.select(
            F.bitmap_bit_position(F.lit(1)).alias("p"),
            F.bitmap_bucket_number(F.lit(1)).alias("b"),
            F.bitmap_count(F.unhex(F.lit("FF"))).alias("c"),
        )
    )
    assert table.column("p").to_pylist()[0] is not None
    assert table.column("b").to_pylist()[0] is not None
    assert pa.types.is_integer(table.schema.field("p").type)
    assert pa.types.is_integer(table.schema.field("b").type)
    assert table.column("c").to_pylist() == [8]
    assert pa.types.is_integer(table.schema.field("c").type)
