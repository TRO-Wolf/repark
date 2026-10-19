"""Facade pins for the three Spark bitmap aggregates against oracle cells F6D-*/B8-*."""

from __future__ import annotations

import ast
import datetime
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
FOLLOWUP_PATH = Path(__file__).parent / "fnp_6d_followup_1_spark_oracle.json"
FACADE_NAMES: tuple[str, ...] = ("bitmap_construct_agg", "bitmap_or_agg", "bitmap_and_agg")
SPARK_TYPES: dict[str, pa.DataType] = {
    "int": pa.int32(),
    "bigint": pa.int64(),
    "string": pa.string(),
    "binary": pa.binary(),
}
_FOLLOWUP_CELLS: dict[str, dict] = {
    cell["id"]: cell for cell in json.loads(FOLLOWUP_PATH.read_text())["cells"]
}
OR_AND_REFUSALS: tuple[tuple[str, str, list, str, str], ...] = (
    ("bitmap_or_agg", "x int", [(1,), (2,)], "INT", "FU-or-int"),
    ("bitmap_or_agg", "x float", [(1.0,), (2.0,)], "FLOAT", "FU-or-float"),
    ("bitmap_or_agg", "x boolean", [(True,), (False,)], "BOOLEAN", "FU-or-bool"),
    ("bitmap_or_agg", "x string", [("1",), ("2",)], "STRING", "FU-or-string"),
    ("bitmap_and_agg", "x int", [(1,), (2,)], "INT", "FU-and-int"),
    ("bitmap_and_agg", "x float", [(1.0,), (2.0,)], "FLOAT", "FU-and-float"),
    ("bitmap_and_agg", "x boolean", [(True,), (False,)], "BOOLEAN", "FU-and-bool"),
    ("bitmap_and_agg", "x string", [("1",), ("2",)], "STRING", "FU-and-string"),
    ("bitmap_or_agg", "x date", [(datetime.date(2020, 1, 1),)], "DATE", "FU-or-date"),
    ("bitmap_and_agg", "x date", [(datetime.date(2020, 1, 1),)], "DATE", "FU-or-date"),
)
FOLD_LENGTH_CASES: tuple[tuple[str, bytes, str], ...] = (
    ("B8-or-short", b"\x01", "bitmap_or_agg"),
    ("B8-and-short", b"\x01", "bitmap_and_agg"),
    ("B8-or-empty-bin", b"", "bitmap_or_agg"),
    ("B8-or-long", bytes(BITMAP_BYTES) + b"\x01", "bitmap_or_agg"),
)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("fnp-bitmap-facade-1").getOrCreate()
    yield session
    session.stop()


def _fixture_cell(cell_id: str) -> dict:
    cells = json.loads(FIXTURE_PATH.read_text())["cells"]
    return next(cell for cell in cells if cell["id"] == cell_id)


def _refusal_cell(cell_id: str) -> dict:
    for cell in json.loads(FIXTURE_PATH.read_text())["cells"]:
        if cell["id"] == cell_id:
            return cell
    return _FOLLOWUP_CELLS[cell_id]


def _fixture_value(raw: str) -> object:
    if raw.startswith(("'", '"', "b'", 'b"')):
        return ast.literal_eval(raw)
    return int(raw)


def _fixture_bitmap(raw: str) -> bytes:
    value = ast.literal_eval(raw)
    if isinstance(value, bytes):
        return value
    return bytes.fromhex(value)


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
    rows = _bitmap_column_rows(table, name)
    assert len(rows) == 1
    return rows[0]


def _bitmap_column_rows(table: pa.Table, name: str) -> list[bytes]:
    field = table.schema.field(name)
    assert field.type == pa.binary(), (name, field.type)
    values = table.column(name).to_pylist()
    assert all(value is not None for value in values)
    return [bytes(value) for value in values]


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
    """pins: fnp-bitmap-facade-1/C-001, C-002, R-4"""
    frame = spark.createDataFrame([(1, 1)], "g int, x int")
    grouped = frame.groupBy("g").agg(F.bitmap_construct_agg(F.col("x")).alias("b")).toArrow()
    fixture_b_type, fixture_b_nullable = _fixture_column_type("B8-group-schema", "b")
    assert fixture_b_type == pa.binary() and fixture_b_nullable is False
    g_field = grouped.schema.field("g")
    b_field = grouped.schema.field("b")
    assert g_field.type == pa.int32() and grouped.column("g").to_pylist() == [1]
    assert b_field.type == fixture_b_type and b_field.nullable is False
    bits = _bitmap_column(grouped, "b")
    assert len(bits) == BITMAP_BYTES and bits[0] == 0x02
    assert bits == _fixture_bitmap(_fixture_cell("B8-group-schema")["rows"][0][1])
    frame.createOrReplaceTempView("fnp_facade1_group_schema")
    door = spark.sql(
        "SELECT g, bitmap_construct_agg(x) AS b FROM fnp_facade1_group_schema GROUP BY g"
    ).toArrow()
    assert bits == _bitmap_column(door, "b")
    assert g_field.nullable == door.schema.field("g").nullable


def test_or_and_agg_match_sql_door_and_fixture(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/C-002, R-4"""
    source = spark.createDataFrame([(1, 1), (2, 1), (2, 2), (3, 2)], "x int, g int")
    inner = source.groupBy("g").agg(
        F.bitmap_construct_agg(F.bitmap_bit_position(F.col("x"))).alias("b")
    )
    outer = inner.agg(
        F.bitmap_count(F.bitmap_or_agg(F.col("b"))).alias("o"),
        F.bitmap_count(F.bitmap_and_agg(F.col("b"))).alias("a"),
    ).toArrow()
    o_fixture, _ = _fixture_column_type("F6D-or-and", "o")
    a_fixture, _ = _fixture_column_type("F6D-or-and", "a")
    assert outer.schema.field("o").type == o_fixture == pa.int64()
    assert outer.schema.field("a").type == a_fixture == pa.int64()
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
    folded_o = _bitmap_column_rows(folded, "o")
    folded_a = _bitmap_column_rows(folded, "a")
    assert folded_o[0][0] == 0x03 and folded_o[1][0] == 0x02
    assert folded_a[0][0] == 0x01 and folded_a[1][0] == 0x02
    assert all(len(bits) == BITMAP_BYTES for bits in (*folded_o, *folded_a))


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
    """pins: fnp-bitmap-facade-1/C-002, R-4"""
    strings = spark.createDataFrame([("1",)], "x string")
    facade = strings.select(F.bitmap_construct_agg(F.col("x")).alias("b")).toArrow()
    bits = _bitmap_column(facade, "b")
    assert len(bits) == BITMAP_BYTES and bits[0] == 0x02
    assert bits == _fixture_bitmap(_fixture_cell("B8-construct-string")["rows"][0][0])
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


@pytest.mark.parametrize(
    ("name", "schema", "rows", "spark_type", "cell_id"),
    OR_AND_REFUSALS,
    ids=[f"{row[0]}-over-{row[3].lower()}" for row in OR_AND_REFUSALS],
)
def test_or_and_agg_refuse_non_binary_columns(
    spark: ReparkSession, name: str, schema: str, rows: list, spark_type: str, cell_id: str
) -> None:
    """pins: fnp-bitmap-facade-1/R-1, C-015"""
    assert _refusal_cell(cell_id)["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    frame = spark.createDataFrame(rows, schema)
    with pytest.raises(Exception) as caught:
        frame.select(getattr(F, name)("x")).collect()
    message = str(caught.value)
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert name in message
    assert 'The first parameter requires the "BINARY" type, however' in message
    assert f'has the type "{spark_type}"' in message
    assert "SQLSTATE: 42K09" in message


@pytest.mark.parametrize(
    ("rows", "schema", "spark_type", "cell_id"),
    [
        ([(True,)], "x boolean", "BOOLEAN", "FU-construct-bool"),
        ([(datetime.date(2020, 1, 1),)], "x date", "DATE", "FU-construct-date"),
    ],
    ids=["boolean", "date"],
)
def test_construct_agg_refuses_non_bigint_columns(
    spark: ReparkSession, rows: list, schema: str, spark_type: str, cell_id: str
) -> None:
    """pins: fnp-bitmap-facade-1/R-1, C-015"""
    cell = _FOLLOWUP_CELLS[cell_id]
    assert cell["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    frame = spark.createDataFrame(rows, schema)
    with pytest.raises(Exception) as caught:
        frame.select(F.bitmap_construct_agg("x")).collect()
    message = str(caught.value)
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert "bitmap_construct_agg" in message
    assert 'The first parameter requires the "BIGINT" type, however' in message
    assert f'has the type "{spark_type}"' in message
    assert "SQLSTATE: 42K09" in message


@pytest.mark.parametrize(
    ("cell_id", "cast"),
    [
        ("FU2-construct-nan-double", "CAST('NaN' AS DOUBLE)"),
        ("FU2-construct-inf-double", "CAST('Infinity' AS DOUBLE)"),
    ],
    ids=["nan", "inf"],
)
def test_construct_agg_nan_and_inf_raise_cast_overflow(
    spark: ReparkSession, cell_id: str, cast: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-015"""
    cell = _FOLLOWUP_CELLS[cell_id]
    assert cell["condition"] == "CAST_OVERFLOW"
    frame = spark.sql(f"SELECT {cast} AS x")
    with pytest.raises(Exception) as caught:
        frame.select(F.bitmap_construct_agg("x")).collect()
    message = str(caught.value)
    assert "[CAST_OVERFLOW]" in message
    assert cell["message"].split(" Use `try_cast`")[0] in message
    assert "SQLSTATE: 22003" in message


def test_construct_agg_malformed_string_raises_cast_invalid_input(
    spark: ReparkSession,
) -> None:
    """pins: fnp-bitmap-facade-1/R-3"""
    cell = _FOLLOWUP_CELLS["FU-construct-abc"]
    assert cell["condition"] == "CAST_INVALID_INPUT"
    frame = spark.createDataFrame([("abc",)], "x string")
    with pytest.raises(Exception) as caught:
        frame.select(F.bitmap_construct_agg("x")).collect()
    message = str(caught.value)
    assert "[CAST_INVALID_INPUT]" in message
    assert 'The value \'abc\' of the type "STRING" cannot be cast to "BIGINT"' in message
    assert "SQLSTATE: 22018" in message


def test_construct_agg_float_fraction_answers_position_one(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/R-3"""
    assert _FOLLOWUP_CELLS["FU-construct-float-frac"]["rows"] == [[1]]
    frame = spark.createDataFrame([(1.7,)], "x float")
    counted = frame.select(F.bitmap_count(F.bitmap_construct_agg("x")).alias("c")).toArrow()
    assert counted.column("c").to_pylist() == [1]
    bits = _bitmap_column(frame.select(F.bitmap_construct_agg("x").alias("b")).toArrow(), "b")
    assert bits[0] == 0x02 and bits[1:] == bytes(BITMAP_BYTES - 1)


def test_call_function_construct_over_bit_position_matches_fixture(
    spark: ReparkSession,
) -> None:
    """pins: fnp-bitmap-facade-1/R-2"""
    frame = spark.createDataFrame([(1,), (2,), (3,), (32767,), (None,)], "x int")
    frame.createOrReplaceTempView("fnp_facade1_call_construct")
    via_call = frame.select(
        F.call_function("bitmap_construct_agg", F.bitmap_bit_position(F.col("x"))).alias("b")
    ).toArrow()
    via_facade = frame.select(
        F.bitmap_construct_agg(F.bitmap_bit_position(F.col("x"))).alias("b")
    ).toArrow()
    door = spark.sql(
        "SELECT bitmap_construct_agg(bitmap_bit_position(x)) AS b FROM fnp_facade1_call_construct"
    ).toArrow()
    bits = _bitmap_column(via_call, "b")
    assert bits == _bitmap_column(via_facade, "b")
    assert bits == _bitmap_column(door, "b")
    fixture_hex, fixture_count = _fixture_row("F6D-construct")
    assert isinstance(fixture_hex, str) and bits.hex() == fixture_hex
    counted = frame.select(
        F.bitmap_count(
            F.call_function("bitmap_construct_agg", F.bitmap_bit_position(F.col("x")))
        ).alias("c")
    ).toArrow()
    assert counted.column("c").to_pylist() == [fixture_count]


def test_call_function_or_and_over_binary_match_fixture(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/R-2"""
    source = spark.createDataFrame([(1, 1), (2, 1), (2, 2), (3, 2)], "x int, g int")
    inner = source.groupBy("g").agg(
        F.bitmap_construct_agg(F.bitmap_bit_position(F.col("x"))).alias("b")
    )
    outer = inner.agg(
        F.bitmap_count(F.call_function("bitmap_or_agg", F.col("b"))).alias("o"),
        F.bitmap_count(F.call_function("bitmap_and_agg", F.col("b"))).alias("a"),
    ).toArrow()
    direct = inner.agg(
        F.bitmap_count(F.bitmap_or_agg(F.col("b"))).alias("o"),
        F.bitmap_count(F.bitmap_and_agg(F.col("b"))).alias("a"),
    ).toArrow()
    assert outer.column("o").to_pylist() == direct.column("o").to_pylist()
    assert outer.column("a").to_pylist() == direct.column("a").to_pylist()
    assert outer.column("o").to_pylist() == [_fixture_row("F6D-or-and")[0]]
    assert outer.column("a").to_pylist() == [_fixture_row("F6D-or-and")[1]]


def test_empty_inputs_match_f6d_empty_payloads(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/R-4"""
    rows = _fixture_cell("F6D-empty")["rows"][0]
    empty = spark.createDataFrame([], "x int")
    construct = empty.select(
        F.bitmap_construct_agg(F.bitmap_bit_position(F.col("x"))).alias("b")
    ).toArrow()
    empty_binary = spark.createDataFrame([], "b binary")
    folded_or = empty_binary.select(F.bitmap_or_agg(F.col("b")).alias("o")).toArrow()
    folded_and = empty_binary.select(F.bitmap_and_agg(F.col("b")).alias("a")).toArrow()
    tables = {"b": (construct, rows[0]), "o": (folded_or, rows[1]), "a": (folded_and, rows[2])}
    for name, (table, raw) in tables.items():
        fixture_type, fixture_nullable = _fixture_column_type("F6D-empty", name)
        field = table.schema.field(name)
        assert field.type == fixture_type == pa.binary()
        assert field.nullable is False and fixture_nullable is False
        assert _bitmap_column(table, name) == _fixture_bitmap(raw)


def test_or_and_fold_short_empty_and_long_payloads(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/R-4"""
    for cell_id, payload, name in FOLD_LENGTH_CASES:
        frame = spark.createDataFrame([(payload,)], "b binary")
        frame.createOrReplaceTempView("fnp_facade1_fold_length")
        facade = frame.select(F.length(getattr(F, name)(F.col("b"))).alias("l")).toArrow()
        door = spark.sql(f"SELECT length({name}(b)) AS l FROM fnp_facade1_fold_length").toArrow()
        assert facade.column("l").to_pylist() == door.column("l").to_pylist()
        assert facade.column("l").to_pylist() == [_fixture_row(cell_id)[0]]
        fixture_type, _ = _fixture_column_type(cell_id, "l")
        assert facade.schema.field("l").type == fixture_type == pa.int32()
        assert door.schema.field("l").type == fixture_type
    short = spark.createDataFrame([(b"\x01",)], "b binary")
    short.createOrReplaceTempView("fnp_facade1_fold_count")
    for name in ("bitmap_or_agg", "bitmap_and_agg"):
        facade_count = short.select(
            F.bitmap_count(getattr(F, name)(F.col("b"))).alias("c")
        ).toArrow()
        door_count = spark.sql(
            f"SELECT bitmap_count({name}(b)) AS c FROM fnp_facade1_fold_count"
        ).toArrow()
        assert facade_count.column("c").to_pylist() == [1]
        assert door_count.column("c").to_pylist() == [1]


def test_unbounded_window_matches_fixture(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/R-4"""
    frame = spark.createDataFrame([(1, 1), (1, 2), (2, 3)], "g int, x int")
    window = Window.partitionBy("g")
    ordered = (
        frame.select(
            F.col("g"),
            F.bitmap_count(F.bitmap_construct_agg(F.col("x")).over(window)).alias("c"),
        )
        .orderBy("g")
        .toArrow()
    )
    frame.createOrReplaceTempView("fnp_facade1_window_unbounded")
    door = spark.sql(
        "SELECT g, bitmap_count(bitmap_construct_agg(x) OVER (PARTITION BY g)) AS c "
        "FROM fnp_facade1_window_unbounded ORDER BY g"
    ).toArrow()
    assert ordered.column("c").to_pylist() == door.column("c").to_pylist()
    expected = _fixture_cell("B8-window-unbounded")["rows"]
    assert ordered.column("g").to_pylist() == [int(row[0]) for row in expected]
    assert ordered.column("c").to_pylist() == [int(row[1]) for row in expected]
    g_fixture, _ = _fixture_column_type("B8-window-unbounded", "g")
    c_fixture, _ = _fixture_column_type("B8-window-unbounded", "c")
    assert ordered.schema.field("g").type == g_fixture == pa.int32()
    assert ordered.schema.field("c").type == c_fixture == pa.int64()


def test_default_name_matches_fixture_schema(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/R-5"""
    expected = _fixture_cell("B8-construct-string")["schema"][0][0]
    frame = spark.createDataFrame([(1,)], "x int")
    assert frame.select(F.bitmap_construct_agg("x")).columns == [expected]


def test_sql_door_qualifier_leak_is_expected_divergence(spark: ReparkSession) -> None:
    """pins: fnp-bitmap-facade-1/R-5"""
    frame = spark.createDataFrame([(1,)], "x int")
    frame.createOrReplaceTempView("fnp_facade1_name_leak")
    leaked = spark.sql("SELECT bitmap_construct_agg(x) FROM fnp_facade1_name_leak").columns
    assert leaked == ["bitmap_construct_agg(datafusion.public.fnp_facade1_name_leak.x)"]
    expressed = frame.selectExpr("bitmap_construct_agg(x)").columns
    assert expressed == ["bitmap_construct_agg(x)"]
