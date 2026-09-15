"""SQL-door pins for Spark bitmap aggregates.

pins: fnp-6d/C-001, C-002, C-003, C-004, C-005, C-006
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from repark import ReparkSession

BITMAP_BYTES: int = 4096


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("fnp-6d-bitmap-aggregates").getOrCreate()
    yield session
    session.stop()


def _binary_cell(table: pa.Table, name: str) -> tuple[pa.DataType, bool, bytes]:
    field = table.schema.field(name)
    values = table.column(name).to_pylist()
    assert len(values) == 1
    payload = values[0]
    assert payload is not None
    return field.type, field.nullable, bytes(payload)


def _construct_fixture_bitmap() -> bytes:
    bits = bytearray(BITMAP_BYTES)
    bits[0] = 0x07
    bits[BITMAP_BYTES - 1] = 0x40
    return bytes(bits)


def test_construct_agg_sets_bits_and_ignores_null(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-001, C-005"""
    table = spark.sql(
        "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b, "
        "bitmap_count(bitmap_construct_agg(bitmap_bit_position(x))) AS c "
        "FROM VALUES (1), (2), (3), (32767), (NULL) AS t(x)"
    ).toArrow()
    binary_type, binary_null, bits = _binary_cell(table, "b")
    assert binary_type == pa.binary()
    assert binary_null is False
    assert bits == _construct_fixture_bitmap()
    count_field = table.schema.field("c")
    assert count_field.type == pa.int64()
    assert count_field.nullable is False
    assert table.column("c").to_pylist() == [4]


def test_or_and_agg_fold_grouped_bitmaps(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-002"""
    table = spark.sql(
        "SELECT bitmap_count(bitmap_or_agg(b)) AS o, "
        "bitmap_count(bitmap_and_agg(b)) AS a FROM ("
        "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b "
        "FROM VALUES (1, 1), (2, 1), (2, 2), (3, 2) AS t(x, g) GROUP BY g)"
    ).toArrow()
    for name, want in (("o", 3), ("a", 1)):
        field = table.schema.field(name)
        assert field.type == pa.int64()
        assert field.nullable is False
        assert table.column(name).to_pylist() == [want]


def test_empty_input_identities_are_non_null_binary(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-003, C-005"""
    table = spark.sql(
        "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b, "
        "bitmap_or_agg(CAST(NULL AS BINARY)) AS o, "
        "bitmap_and_agg(CAST(NULL AS BINARY)) AS a "
        "FROM VALUES (1) AS t(x) WHERE false"
    ).toArrow()
    zeros = bytes(BITMAP_BYTES)
    ones = bytes([0xFF] * BITMAP_BYTES)
    expected = {"b": zeros, "o": zeros, "a": ones}
    for name, want in expected.items():
        binary_type, binary_null, bits = _binary_cell(table, name)
        assert binary_type == pa.binary()
        assert binary_null is False
        assert bits == want


def test_and_agg_length_is_4096_int(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-004"""
    table = spark.sql(
        "SELECT length(bitmap_and_agg(b)) AS n FROM ("
        "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b "
        "FROM VALUES (1) AS t(x))"
    ).toArrow()
    field = table.schema.field("n")
    assert field.type in {pa.int32(), pa.int64()}
    assert field.nullable is False
    assert table.column("n").to_pylist() == [4096]


@pytest.mark.parametrize(
    "sql",
    [
        (
            "SELECT bitmap_construct_agg(bitmap_bit_position(x)) "
            "OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
            "FROM VALUES (1), (2) AS t(x)"
        ),
        (
            "SELECT bitmap_or_agg(CAST(NULL AS BINARY)) "
            "OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
            "FROM VALUES (1), (2) AS t(x)"
        ),
        (
            "SELECT bitmap_and_agg(CAST(NULL AS BINARY)) "
            "OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
            "FROM VALUES (1), (2) AS t(x)"
        ),
    ],
)
def test_sliding_frame_refuses_loudly(spark: ReparkSession, sql: str) -> None:
    """pins: fnp-6d/C-006"""
    with pytest.raises(Exception, match=r"(?i)retract_batch|sliding") as caught:
        spark.sql(sql).collect()
    message = str(caught.value)
    assert "retract_batch" in message or "sliding" in message.lower()
