"""SQL-door pins for Spark bitmap aggregates.

pins: fnp-6d/C-001, C-002, C-003, C-004, C-005, C-006, C-011, C-012, C-013, C-014,
C-015
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


def test_or_and_short_empty_and_long_normalize_to_4096(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-011"""
    cases = (
        (
            "SELECT length(bitmap_or_agg(b)) l, bitmap_count(bitmap_or_agg(b)) c "
            "FROM VALUES (X'01') AS t(b)",
            4096,
            1,
        ),
        (
            "SELECT length(bitmap_and_agg(b)) l, bitmap_count(bitmap_and_agg(b)) c "
            "FROM VALUES (X'01') AS t(b)",
            4096,
            1,
        ),
        (
            "SELECT length(bitmap_or_agg(b)) l, bitmap_count(bitmap_or_agg(b)) c "
            "FROM VALUES (X'') AS t(b)",
            4096,
            0,
        ),
        (
            "SELECT length(bitmap_or_agg(b)) l FROM ("
            "SELECT concat(bitmap_construct_agg(0), X'01') b FROM VALUES (1) AS t(x))",
            4096,
            None,
        ),
    )
    for sql, length, count in cases:
        table = spark.sql(sql).toArrow()
        field = table.schema.field("l")
        assert field.type in {pa.int32(), pa.int64()}
        assert field.nullable is False
        assert table.column("l").to_pylist() == [length]
        if count is not None:
            assert table.column("c").to_pylist() == [count]


def test_construct_agg_coerces_string_like_spark_bigint(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-012"""
    table = spark.sql("SELECT bitmap_construct_agg(x) AS b FROM VALUES ('1') AS t(x)").toArrow()
    binary_type, binary_null, bits = _binary_cell(table, "b")
    assert binary_type == pa.binary()
    assert binary_null is False
    assert len(bits) == BITMAP_BYTES
    assert bits[0] == 0x02
    assert bits[1:] == bytes(BITMAP_BYTES - 1)
    int_table = spark.sql(
        "SELECT bitmap_count(bitmap_construct_agg(x)) AS c FROM VALUES (CAST(3 AS INT)) AS t(x)"
    ).toArrow()
    assert int_table.column("c").to_pylist() == [1]


def test_construct_agg_refuses_out_of_range_with_spark_class(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-013"""
    cases = (
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (32768) AS t(x)",
            "32768",
        ),
        (
            "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (-1) AS t(x)",
            "-1",
        ),
    )
    for sql, position in cases:
        with pytest.raises(Exception) as caught:
            spark.sql(sql).collect()
        message = str(caught.value)
        assert "[INVALID_BITMAP_POSITION]" in message
        assert f"The 0-indexed bitmap position {position} is out of bounds." in message
        assert "The bitmap has 32768 bits (4096 bytes)." in message
        assert "SQLSTATE: 22003" in message
    ok = spark.sql(
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (32767) AS t(x)"
    ).toArrow()
    assert ok.column("c").to_pylist() == [1]


def test_all_null_and_empty_identities_and_group_schema(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-014"""
    construct = spark.sql(
        "SELECT length(b) l, bitmap_count(b) c FROM ("
        "SELECT bitmap_construct_agg(CAST(NULL AS BIGINT)) b FROM VALUES (1) AS t(x))"
    ).toArrow()
    assert construct.column("l").to_pylist() == [4096]
    assert construct.column("c").to_pylist() == [0]
    and_null = spark.sql(
        "SELECT length(a) l, bitmap_count(a) c FROM ("
        "SELECT bitmap_and_agg(CAST(NULL AS BINARY)) a FROM VALUES (1) AS t(x))"
    ).toArrow()
    assert and_null.column("l").to_pylist() == [4096]
    assert and_null.column("c").to_pylist() == [32768]
    or_null = spark.sql(
        "SELECT length(o) l, bitmap_count(o) c FROM ("
        "SELECT bitmap_or_agg(CAST(NULL AS BINARY)) o FROM VALUES (1) AS t(x))"
    ).toArrow()
    assert or_null.column("l").to_pylist() == [4096]
    assert or_null.column("c").to_pylist() == [0]
    and_empty = spark.sql(
        "SELECT length(a) l, bitmap_count(a) c FROM ("
        "SELECT bitmap_and_agg(CAST(NULL AS BINARY)) a FROM VALUES (1) AS t(x) WHERE false)"
    ).toArrow()
    assert and_empty.column("l").to_pylist() == [4096]
    assert and_empty.column("c").to_pylist() == [32768]
    grouped = spark.sql(
        "SELECT g, bitmap_construct_agg(x) b FROM VALUES (1, 1) AS t(g, x) GROUP BY g"
    ).toArrow()
    g_field = grouped.schema.field("g")
    b_field = grouped.schema.field("b")
    assert g_field.type == pa.int32()
    assert b_field.type == pa.binary()
    assert b_field.nullable is False
    bits = bytes(grouped.column("b").to_pylist()[0])
    assert bits[0] == 0x02
    assert len(bits) == BITMAP_BYTES


def test_unbounded_partition_window_answers_spark(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-015"""
    table = spark.sql(
        "SELECT g, bitmap_count(bitmap_construct_agg(x) OVER (PARTITION BY g)) c "
        "FROM VALUES (1, 1), (1, 2), (2, 3) AS t(g, x) ORDER BY g"
    ).toArrow()
    assert table.column("g").to_pylist() == [1, 1, 2]
    assert table.column("c").to_pylist() == [2, 2, 1]


def test_sliding_frame_answers_spark_via_win_slide_rescan(spark: ReparkSession) -> None:
    """pins: fnp-6d/C-006, C-015"""
    table = spark.sql(
        "SELECT x, bitmap_count(bitmap_construct_agg(x) OVER "
        "(ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) c "
        "FROM VALUES (1), (2), (3) AS t(x)"
    ).toArrow()
    assert table.column("x").to_pylist() == [1, 2, 3]
    assert table.column("c").to_pylist() == [1, 2, 2]
