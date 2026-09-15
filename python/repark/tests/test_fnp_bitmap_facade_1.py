"""Facade pins for the three Spark bitmap aggregates against oracle cells F6D-*/B8-*."""

from __future__ import annotations

import ast
import inspect
import json
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession, Window
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

BITMAP_BYTES: int = 4096
FIXTURE_PATH = Path(__file__).parent / "fnp_bitmap_facade_1_spark_oracle.json"
FACADE_NAMES: tuple[str, ...] = ("bitmap_construct_agg", "bitmap_or_agg", "bitmap_and_agg")
SPARK_TYPES: dict[str, pa.DataType] = {
    "int": pa.int32(),
    "bigint": pa.int64(),
    "string": pa.string(),
    "binary": pa.binary(),
}


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("fnp-bitmap-facade-1").getOrCreate()
    yield session
    session.stop()


def _fixture_cell(cell_id: str) -> dict:
    cells = json.loads(FIXTURE_PATH.read_text())["cells"]
    return next(cell for cell in cells if cell["id"] == cell_id)


def _fixture_value(raw: str) -> object:
    if raw.startswith(("'", '"', "b'", 'b"')):
        return ast.literal_eval(raw)
    return int(raw)


def _fixture_row(cell_id: str) -> list[object]:
    cell = _fixture_cell(cell_id)
    assert len(cell["rows"]) == 1, cell_id
    return [_fixture_value(value) for value in cell["rows"][0]]


def _fixture_column_type(cell_id: str, name: str) -> tuple[pa.DataType, bool]:
    for column_name, spark_type, nullable in _fixture_cell(cell_id)["schema"]:
        if column_name == name:
            return SPARK_TYPES[spark_type], nullable
    raise AssertionError(f"{cell_id} has no column {name}")


def _bitmap_column(table: pa.Table, name: str) -> bytes:
    field = table.schema.field(name)
    assert field.type == pa.binary(), (name, field.type)
    values = table.column(name).to_pylist()
    assert len(values) == 1
    assert values[0] is not None
    return bytes(values[0])


def _bitmap(bytes_length: int, first_byte: int) -> bytes:
    payload = bytearray(bytes_length)
    payload[0] = first_byte
    return bytes(payload)


def test_names_present_with_pyspark_signature() -> None:
    """pins: fnp-bitmap-facade-1/C-001"""
    for name in FACADE_NAMES:
        function = getattr(F, name, None)
        assert callable(function), name
        parameters = list(inspect.signature(function).parameters)
        assert parameters == ["col"], (name, parameters)


def test_construct_agg_global_matches_sql_door_and_fixture(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-001, C-002"""
    frame = spark.createDataFrame([(1,), (2,), (3,), (32767,), (None,)], "x int")
    facade = frame.select(
        F.bitmap_construct_agg(F.bitmap_bit_position(F.col("x"))).alias("b")
    ).toArrow()
    b_field = facade.schema.field("b")
    assert b_field.type == pa.binary() and b_field.nullable is False
    bits = _bitmap_column(facade, "b")
    assert len(bits) == BITMAP_BYTES
    assert bits[0] == 0x07
    assert bits[BITMAP_BYTES - 1] == 0x40
    frame.createOrReplaceTempView("fnp_facade1_construct")
    door = spark.sql(
        "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b, "
        "bitmap_count(bitmap_construct_agg(bitmap_bit_position(x))) AS c "
        "FROM fnp_facade1_construct"
    ).toArrow()
    assert bits == _bitmap_column(door, "b")
    fixture_hex, fixture_count = _fixture_row("F6D-construct")
    assert isinstance(fixture_hex, str) and bits.hex() == fixture_hex
    assert door.column("c").to_pylist() == [fixture_count]


def test_construct_agg_grouped_schema_and_bytes(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-001, C-002"""
    frame = spark.createDataFrame([(1, 1)], "g int, x int")
    grouped = frame.groupBy("g").agg(F.bitmap_construct_agg(F.col("x")).alias("b")).toArrow()
    fixture_g_type, fixture_g_nullable = _fixture_column_type("B8-group-schema", "g")
    fixture_b_type, fixture_b_nullable = _fixture_column_type("B8-group-schema", "b")
    assert fixture_g_type == pa.int32() and fixture_g_nullable is False
    assert fixture_b_type == pa.binary() and fixture_b_nullable is False
    g_field = grouped.schema.field("g")
    b_field = grouped.schema.field("b")
    assert g_field.type == pa.int32() and g_field.nullable is False
    assert b_field.type == pa.binary() and b_field.nullable is False
    bits = _bitmap_column(grouped, "b")
    assert len(bits) == BITMAP_BYTES and bits[0] == 0x02
    frame.createOrReplaceTempView("fnp_facade1_group_schema")
    door = spark.sql(
        "SELECT g, bitmap_construct_agg(x) AS b FROM fnp_facade1_group_schema GROUP BY g"
    ).toArrow()
    assert bits == _bitmap_column(door, "b")


def test_or_and_agg_match_sql_door_and_fixture(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-002"""
    source = spark.createDataFrame([(1, 1), (2, 1), (2, 2), (3, 2)], "x int, g int")
    inner = source.groupBy("g").agg(
        F.bitmap_construct_agg(F.bitmap_bit_position(F.col("x"))).alias("b")
    )
    outer = inner.agg(
        F.bitmap_count(F.bitmap_or_agg(F.col("b"))).alias("o"),
        F.bitmap_count(F.bitmap_and_agg(F.col("b"))).alias("a"),
    ).toArrow()
    fixture_o_type, _ = _fixture_column_type("F6D-or-and", "o")
    assert fixture_o_type == pa.int64()
    assert outer.column("o").to_pylist() == [_fixture_row("F6D-or-and")[0]]
    assert outer.column("a").to_pylist() == [_fixture_row("F6D-or-and")[1]]
    bitmaps = spark.createDataFrame(
        [
            (1, _bitmap(BITMAP_BYTES, 0x01)),
            (1, _bitmap(BITMAP_BYTES, 0x03)),
            (2, _bitmap(BITMAP_BYTES, 0x02)),
        ],
        "g int, b binary",
    )
    folded = (
        bitmaps.groupBy("g")
        .agg(F.bitmap_or_agg(F.col("b")).alias("o"), F.bitmap_and_agg(F.col("b")).alias("a"))
        .orderBy("g")
        .toArrow()
    )
    bitmaps.createOrReplaceTempView("fnp_facade1_fold")
    door = spark.sql(
        "SELECT g, bitmap_or_agg(b) AS o, bitmap_and_agg(b) AS a "
        "FROM fnp_facade1_fold GROUP BY g ORDER BY g"
    ).toArrow()
    assert folded.column("o").to_pylist() == door.column("o").to_pylist()
    assert folded.column("a").to_pylist() == door.column("a").to_pylist()
    assert _bitmap_column(folded, "o")[0] == 0x03
    assert _bitmap_column(folded, "a")[0] == 0x01


def test_null_rows_are_skipped_and_all_null_identities_hold(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-002"""
    mixed = spark.createDataFrame([(1,), (None,), (2,)], "x int")
    mixed.createOrReplaceTempView("fnp_facade1_mixed")
    facade = mixed.select(F.bitmap_construct_agg(F.col("x")).alias("b")).toArrow()
    bits = _bitmap_column(facade, "b")
    assert bits[0] == 0x06
    door = spark.sql("SELECT bitmap_construct_agg(x) AS b FROM fnp_facade1_mixed").toArrow()
    assert bits == _bitmap_column(door, "b")
    all_null = spark.createDataFrame([(None,)], "x bigint")
    construct = all_null.select(
        F.bitmap_count(F.bitmap_construct_agg(F.col("x"))).alias("c")
    ).toArrow()
    assert construct.column("c").to_pylist() == [_fixture_row("B8-construct-all-null")[1]]
    null_binary = spark.createDataFrame([(None,)], "b binary")
    or_null = null_binary.select(F.bitmap_count(F.bitmap_or_agg(F.col("b"))).alias("c")).toArrow()
    assert or_null.column("c").to_pylist() == [_fixture_row("B8-or-all-null")[1]]
    and_null = null_binary.select(F.bitmap_count(F.bitmap_and_agg(F.col("b"))).alias("c")).toArrow()
    assert and_null.column("c").to_pylist() == [_fixture_row("B8-and-all-null")[1]]


def test_empty_input_identities_are_non_null_binary(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-002"""
    empty = spark.createDataFrame([], "x int")
    construct = empty.select(F.bitmap_construct_agg(F.col("x")).alias("b")).toArrow()
    assert _bitmap_column(construct, "b") == bytes(BITMAP_BYTES)
    empty_binary = spark.createDataFrame([], "b binary")
    or_empty = empty_binary.select(F.bitmap_or_agg(F.col("b")).alias("o")).toArrow()
    assert _bitmap_column(or_empty, "o") == bytes(BITMAP_BYTES)
    and_empty = empty_binary.select(F.bitmap_and_agg(F.col("b")).alias("a")).toArrow()
    assert _bitmap_column(and_empty, "a") == bytes([0xFF] * BITMAP_BYTES)
    and_count = empty_binary.select(
        F.bitmap_count(F.bitmap_and_agg(F.col("b"))).alias("c")
    ).toArrow()
    assert and_count.column("c").to_pylist() == [_fixture_row("B8-and-empty")[1]]


def test_string_argument_coerces_like_bigint(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-002"""
    strings = spark.createDataFrame([("1",)], "x string")
    facade = strings.select(F.bitmap_construct_agg(F.col("x")).alias("b")).toArrow()
    bits = _bitmap_column(facade, "b")
    assert len(bits) == BITMAP_BYTES and bits[0] == 0x02
    strings.createOrReplaceTempView("fnp_facade1_string")
    door = spark.sql("SELECT bitmap_construct_agg(x) AS b FROM fnp_facade1_string").toArrow()
    assert bits == _bitmap_column(door, "b")
    ints = spark.createDataFrame([(3,)], "x int")
    count = ints.select(F.bitmap_count(F.bitmap_construct_agg(F.col("x"))).alias("c")).toArrow()
    assert count.column("c").to_pylist() == [_fixture_row("B8-construct-int")[0]]


def test_out_of_range_positions_raise_invalid_bitmap_position(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-003"""
    for cell_id in ("B8-pos-32768", "B8-pos-neg"):
        position = _fixture_cell(cell_id)["message"].split("position ", 1)[1].split(" ", 1)[0]
        frame = spark.createDataFrame([(int(position),)], "x int")
        with pytest.raises(Exception) as caught:
            frame.select(F.bitmap_construct_agg(F.col("x"))).collect()
        message = str(caught.value)
        assert "[INVALID_BITMAP_POSITION]" in message
        assert _fixture_cell(cell_id)["message"].split(". ", 1)[1] in message
    boundary = spark.createDataFrame([(32767,)], "x int")
    ok = boundary.select(F.bitmap_count(F.bitmap_construct_agg(F.col("x"))).alias("c")).toArrow()
    assert ok.column("c").to_pylist() == [_fixture_row("B8-pos-32767")[0]]


def test_sliding_window_rows_between_answers(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-002"""
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x int")
    window = Window.orderBy("x").rowsBetween(-1, 0)
    facade = frame.select(
        F.bitmap_count(F.bitmap_construct_agg(F.col("x")).over(window)).alias("c")
    ).toArrow()
    expected = [_fixture_value(row[1]) for row in _fixture_cell("B8-window-sliding")["rows"]]
    assert facade.column("c").to_pylist() == expected
