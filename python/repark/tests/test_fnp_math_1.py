"""FNP-MATH-1 step-1 red pins over fnp_math_1_spark_oracle.json, both doors, both ANSI settings."""

from __future__ import annotations

import ast
import inspect
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark.spark.column import Column
from repark.spark.dataframe.core import DataFrame
from repark.spark.functions import col, lit

_FIXTURE: dict[str, Any] = json.loads(
    Path(__file__).with_name("fnp_math_1_spark_oracle.json").read_text()
)
_O245: list[dict[str, Any]] = list(_FIXTURE["blocks"]["o245"])
_F14: list[dict[str, Any]] = list(_FIXTURE["blocks"]["f14"])
_Q12: list[dict[str, Any]] = list(_FIXTURE["blocks"]["q12"])
_BL6: list[dict[str, Any]] = list(_FIXTURE["blocks"]["bl6"])
_SIGS: dict[str, str] = dict(_FIXTURE["signatures"])

_C001_NAMES: tuple[str, ...] = (
    "bround",
    "conv",
    "hash",
    "format_number",
    "mask",
    "collate",
    "collation",
    "sentences",
    "split",
    "aes_encrypt",
    "aes_decrypt",
    "try_aes_decrypt",
    "locate",
    "array_join",
)

_C005_NAMES: frozenset[str] = frozenset({"hash", "aes_encrypt", "aes_decrypt", "try_aes_decrypt"})

_COLLATE_UNKNOWN_CONDITION = "COLLATION_INVALID_NAME"

_FRAME_VALUES = (
    "SELECT * FROM VALUES "
    "(1, CAST(2.5 AS DOUBLE), CAST(-2.5 AS DOUBLE), CAST(12345.6789 AS DECIMAL(10,4)), "
    "'Hello World. How are you?', 'a,b,,c', '100', 'abcd-EFG-123', '{\"a\":1}', "
    "'1,abc,2.5', '<p/>', "
    "array(named_struct('x', 1, 'y', 'p'), named_struct('x', 2, 'y', 'q')), "
    "array(10, 20), 'k1'), "
    "(2, CAST(NULL AS DOUBLE), CAST(3.5 AS DOUBLE), CAST(NULL AS DECIMAL(10,4)), "
    "CAST(NULL AS STRING), CAST(NULL AS STRING), 'zz', CAST(NULL AS STRING), "
    "CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), "
    "CAST(NULL AS ARRAY<STRUCT<x:INT,y:STRING>>), array(), 'k1'), "
    "(3, CAST(0.125 AS DOUBLE), CAST(1.0 AS DOUBLE), CAST(-0.5 AS DECIMAL(10,4)), "
    "'one', 'x', '-17', 'x', '{\"a\":null}', ',,', '<p/>', array(), "
    "CAST(NULL AS ARRAY<INT>), 'k2') "
    "AS t(id, d, d2, dec, txt, csvs, num, masked, js, csvrow, xml, arr_s, arr_i, key)"
)

_TS_CASE = (
    "CASE id WHEN 1 THEN TIMESTAMP'2024-01-01 10:07:30' "
    "WHEN 2 THEN TIMESTAMP'2024-01-01 10:12:00' "
    "ELSE TIMESTAMP'2024-01-01 10:31:00' END"
)

_FRAME_SQL = f"SELECT *, {_TS_CASE} AS ts FROM ({_FRAME_VALUES})"

_Q12_FRAME_SQL = "SELECT 'a,b,,c' AS s"

_DECIMAL_RE = re.compile(r"^decimal\((\d+),(\d+)\)$")


@pytest.fixture
def spark() -> ReparkSession:
    """Build one isolated session per pin."""
    session = ReparkSession.builder.appName("pytest-fnp-math-1").getOrCreate()
    yield session
    session.stop()


def _set_ansi(spark: ReparkSession, ansi: bool) -> None:
    """Flip the ANSI flag to the cell's recorded setting."""
    spark.conf.set("spark.sql.ansi.enabled", "true" if ansi else "false")


def _decode_o245(value: Any) -> Any:
    """Decode one o245 recorded value to its Python answer."""
    if isinstance(value, dict) and set(value) == {"repr"}:
        text = str(value["repr"])
        if text.startswith("Decimal("):
            return Decimal(text[len("Decimal('") : -len("')")])
        raise AssertionError(f"undecodable repr value: {text}")
    if isinstance(value, dict) and set(value) == {"bytes_hex"}:
        return bytes.fromhex(str(value["bytes_hex"]))
    return value


def _decode_repr_text(text: str) -> Any:
    """Decode one repr-string recorded value to its Python answer."""
    return ast.literal_eval(text)


def _spark_simple_to_arrow(text: str) -> pa.DataType:
    """Map one Spark simpleString to the Arrow type the door must answer."""
    if text == "string collate UTF8_LCASE":
        return pa.string()
    if text.startswith("array<") and text.endswith(">"):
        return pa.list_(_spark_simple_to_arrow(text[len("array<") : -1]))
    match = _DECIMAL_RE.match(text)
    if match is not None:
        return pa.decimal128(int(match.group(1)), int(match.group(2)))
    return {
        "double": pa.float64(),
        "string": pa.string(),
        "int": pa.int32(),
        "boolean": pa.bool_(),
        "binary": pa.binary(),
    }[text]


def _q12_arrow(node: Any) -> pa.DataType:
    """Map one Q12 Spark JSON type node to the Arrow type the door must answer."""
    if isinstance(node, str):
        return {"string": pa.string(), "integer": pa.int32(), "long": pa.int64()}[node]
    assert node["type"] == "array"
    return pa.list_(_q12_arrow(node["elementType"]))


def _assert_o245_value_table(table: pa.Table, cell: dict[str, Any]) -> None:
    """Compare every column of an o245 answer against the recorded cell."""
    columns = list(cell["columns"])
    assert [field.name for field in table.schema] == [c["name"] for c in columns]
    for index, column in enumerate(columns):
        field = table.schema.field(index)
        assert field.type == _spark_simple_to_arrow(column["type"]), column["name"]
        assert field.nullable == column["nullable"], column["name"]
        assert table.column(index).to_pylist() == [
            _decode_o245(row[index]) for row in cell["rows"]
        ], column["name"]


def _assert_repr_block_table(
    table: pa.Table, names: list[str], types: list[str], nullables: list[bool], rows: list[Any]
) -> None:
    """Compare one repr-string block answer against its decoded cell."""
    assert [field.name for field in table.schema] == names
    for index, (want_type, want_nullable) in enumerate(zip(types, nullables, strict=True)):
        field = table.schema.field(index)
        assert field.type == _spark_simple_to_arrow(want_type), names[index]
        assert field.nullable == want_nullable, names[index]
        assert table.column(index).to_pylist() == [_decode_repr_text(row[index]) for row in rows], (
            names[index]
        )


def _frame(spark: ReparkSession) -> DataFrame:
    """Build the shared o245 input frame with per-row timestamps."""
    return spark.sql(_FRAME_SQL)


def _run_python(frame: DataFrame, expr: str) -> DataFrame:
    """Run one recorded Python-door expression over the frame."""
    namespace: dict[str, Any] = {"F": F, "col": col, "lit": lit, "__builtins__": {}}
    return frame.select(eval(expr, namespace))


def _cell_key(cell: dict[str, Any]) -> str:
    """Identify one o245 cell by name, door, ANSI setting and expression."""
    return f"{cell['name']}|{cell['door']}|ansi={cell['ansi']}|{cell['expr'][:64]}"


_O245_BY_KEY: dict[str, dict[str, Any]] = {_cell_key(cell): cell for cell in _O245}
_Q12_BY_ID: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in _Q12}
_F14_BY_ID: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in _F14}
_BL6_BY_ID: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in _BL6}


def _o245_value_keys(door: str, exclude: frozenset[str] = frozenset()) -> list[str]:
    """List value-cell keys for one o245 door outside the excluded names."""
    return [
        key
        for key, cell in _O245_BY_KEY.items()
        if cell["door"] == door and cell.get("rows") is not None and cell["name"] not in exclude
    ]


def _spark_param_names(signature: str) -> list[str]:
    """Read parameter names off one recorded PySpark signature string."""
    inner = signature.rsplit(") ->", 1)[0][1:]
    names: list[str] = []
    depth = 0
    current: list[str] = []
    for char in inner:
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        if char == "," and depth == 0:
            names.append("".join(current))
            current = []
        else:
            current.append(char)
    names.append("".join(current))
    return [part.strip().lstrip("*").split(":")[0].strip() for part in names]


def _f14_facade(cell_id: str) -> list[Column]:
    """Spell one F14 SQL cell through the Python-door facade."""
    if cell_id == "F14-mask":
        return [
            F.mask(F.lit("AbCd-123")),
            F.mask(F.lit("AbCd-123"), F.lit("Q"), F.lit("q"), F.lit("d"), F.lit("o")),
            F.mask(F.lit(None).cast("string")),
        ]
    if cell_id == "F14-mask-null-repl":
        null = F.lit(None).cast("string")
        return [F.mask(F.lit("AbCd-123"), null, null, null, null)]
    if cell_id == "F14-aes-gcm-rt":
        encrypted = F.aes_encrypt(F.lit("Spark"), F.lit("0000111122223333"))
        return [F.aes_decrypt(encrypted, F.lit("0000111122223333")).cast("string")]
    if cell_id == "F14-aes-ecb":
        encrypted = F.aes_encrypt(
            F.lit("Spark"), F.lit("0000111122223333"), F.lit("ECB"), F.lit("PKCS")
        )
        return [F.hex(encrypted)]
    if cell_id == "F14-aes-cbc-iv":
        return [
            F.hex(
                F.aes_encrypt(
                    F.lit("Spark"),
                    F.lit("0000111122223333"),
                    F.lit("CBC"),
                    F.lit("PKCS"),
                    F.unhex(F.lit("00000000000000000000000000000000")),
                )
            )
        ]
    if cell_id == "F14-aes-gcm-iv":
        return [
            F.hex(
                F.aes_encrypt(
                    F.lit("Spark"),
                    F.lit("0000111122223333"),
                    F.lit("GCM"),
                    F.lit("DEFAULT"),
                    F.unhex(F.lit("000000000000000000000000")),
                    F.lit("aad"),
                )
            )
        ]
    if cell_id == "F14-aes-dec-ecb":
        return [
            F.aes_decrypt(
                F.unhex(
                    F.lit(
                        "6E7CA17BBB468D3084B5744BCA729FB7B2B7BCB8E4472847D02670489D95FA97DBBA7D3210"
                    )
                ),
                F.lit("0000111122223333"),
                F.lit("GCM"),
            ).cast("string")
        ]
    if cell_id == "F14-try-aes-bad":
        return [F.try_aes_decrypt(F.unhex(F.lit("00")), F.lit("0000111122223333"))]
    raise AssertionError(f"unmapped F14 cell: {cell_id}")


def _q12_facade(cell_id: str) -> Column:
    """Spell one Q12 split SQL cell through F.split."""
    if cell_id == "Q12-41":
        return F.split(F.lit("a,b"), ",")
    if cell_id == "Q12-42":
        return F.split("s", ",")
    if cell_id == "Q12-43":
        return F.split("s", ",", 2)
    if cell_id == "Q12-44":
        return F.split("s", ",", -1)
    if cell_id == "Q12-45":
        return F.split("s", ",", 0)
    if cell_id == "Q12-46":
        return F.split(F.lit("a1b22c"), "[0-9]+")
    if cell_id == "Q12-47":
        return F.split(F.lit("abc"), "")
    if cell_id == "Q12-48":
        return F.split(F.lit(""), ",")
    if cell_id == "Q12-49":
        return F.split(F.lit(None).cast("string"), ",")
    if cell_id == "Q12-50":
        return F.split("s", F.lit(None).cast("string"))
    if cell_id == "Q12-51":
        return F.split(F.lit("a.b"), ".")
    if cell_id == "Q12-52":
        return F.split(F.lit("a|b"), "\\|")
    if cell_id == "Q12-53":
        return F.split("s", ",", F.lit(None).cast("int"))
    if cell_id == "Q12-54":
        return F.split(F.lit("aXbxc"), "(?i)x")
    if cell_id == "Q12-55":
        return F.split(F.lit(123), "2")
    raise AssertionError(f"unmapped Q12 cell: {cell_id}")


def _assert_q12_table(table: pa.Table, cell: dict[str, Any]) -> None:
    """Compare one split answer against its Q12 cell, type and nullability included."""
    field = cell["field"]
    assert [f.name for f in table.schema] == ["v"]
    arrow_field = table.schema.field("v")
    assert arrow_field.type == _q12_arrow(field["type"]), cell["id"]
    assert arrow_field.nullable == field["nullable"], cell["id"]
    assert arrow_field.type.value_field.nullable == field["type"]["containsNull"], cell["id"]
    assert table.column("v").to_pylist() == [_decode_repr_text(cell["rows"][0])], cell["id"]


@pytest.mark.parametrize("name", _C001_NAMES)
def test_c001_name_present_with_spark_parameters(name: str) -> None:
    """Pin one D-6 name on the facade with PySpark 4.1.2 parameter names."""
    assert hasattr(F, name), name
    assert list(inspect.signature(getattr(F, name)).parameters) == _spark_param_names(_SIGS[name])


@pytest.mark.parametrize("name", ("bin", "rint", "like"))
def test_c001_presence_without_recorded_signature(name: str) -> None:
    """Pin one BL-6 name present; its exact PySpark signature has no recorded cell yet."""
    assert hasattr(F, name), name


@pytest.mark.parametrize("key", _o245_value_keys("python", frozenset({"split"}) | _C005_NAMES))
def test_c002_python_door_value_cells(spark: ReparkSession, key: str) -> None:
    """Pin one o245 Python-door value cell on the recorded ANSI setting."""
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, bool(cell["ansi"]))
    _assert_o245_value_table(_run_python(_frame(spark), str(cell["expr"])).to_arrow(), cell)


@pytest.mark.parametrize("key", _o245_value_keys("sql", _C005_NAMES))
def test_c003_sql_door_value_cells(spark: ReparkSession, key: str) -> None:
    """Pin one o245 SQL-door value cell on the recorded ANSI setting."""
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, bool(cell["ansi"]))
    query = str(cell["expr"]).replace("FRAME", f"({_FRAME_SQL})")
    _assert_o245_value_table(spark.sql(query).to_arrow(), cell)


@pytest.mark.parametrize("key", [k for k, c in _O245_BY_KEY.items() if "error_type" in c])
def test_c004_o245_error_cells(spark: ReparkSession, key: str) -> None:
    """Pin one o245 error cell class on its recorded door and ANSI setting."""
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, bool(cell["ansi"]))
    condition = cell.get("error_condition") or _COLLATE_UNKNOWN_CONDITION
    with pytest.raises(Exception) as excinfo:
        if cell["door"] == "python":
            _run_python(_frame(spark), str(cell["expr"])).to_arrow()
        else:
            query = str(cell["expr"]).replace("FRAME", f"({_FRAME_SQL})")
            spark.sql(query).to_arrow()
    assert condition in str(excinfo.value), key


@pytest.mark.parametrize("cell_id", ["F14-aes-badkey", "F14-aes-dec-bad"])
@pytest.mark.parametrize("ansi", (True, False))
def test_c004_f14_aes_error_cells_sql_door(spark: ReparkSession, cell_id: str, ansi: bool) -> None:
    """Pin one F14 AES error cell on the SQL door under both ANSI settings."""
    cell = _F14_BY_ID[cell_id]
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        spark.sql(str(cell["input"])).to_arrow()
    assert str(cell["condition"]) in str(excinfo.value), cell_id


@pytest.mark.parametrize(
    ("cell_id", "condition"),
    (
        ("badkey", "INVALID_PARAMETER_VALUE.AES_KEY_LENGTH"),
        ("badvalue", "INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR"),
    ),
)
@pytest.mark.parametrize("ansi", (True, False))
def test_c004_f14_aes_error_cells_python_door(
    spark: ReparkSession, cell_id: str, condition: str, ansi: bool
) -> None:
    """Pin one F14 AES error shape through the facade under both ANSI settings."""
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        if cell_id == "badkey":
            spark.range(1).select(
                F.aes_encrypt(F.lit("Spark"), F.lit("short")).alias("v")
            ).to_arrow()
        else:
            spark.range(1).select(
                F.aes_decrypt(F.unhex(F.lit("00")), F.lit("0000111122223333")).alias("v")
            ).to_arrow()
    assert condition in str(excinfo.value), cell_id


@pytest.mark.parametrize(
    "key",
    [k for k, c in _O245_BY_KEY.items() if c.get("rows") is not None and c["name"] in _C005_NAMES],
)
def test_c005_o245_exact_hash_aes_cells(spark: ReparkSession, key: str) -> None:
    """Pin one o245 hash or AES cell byte-exact on its recorded door and ANSI setting."""
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, bool(cell["ansi"]))
    if cell["door"] == "python":
        table = _run_python(_frame(spark), str(cell["expr"])).to_arrow()
    else:
        query = str(cell["expr"]).replace("FRAME", f"({_FRAME_SQL})")
        table = spark.sql(query).to_arrow()
    _assert_o245_value_table(table, cell)


@pytest.mark.parametrize(
    "cell_id",
    [
        "F14-mask",
        "F14-mask-null-repl",
        "F14-aes-gcm-rt",
        "F14-aes-ecb",
        "F14-aes-cbc-iv",
        "F14-aes-gcm-iv",
        "F14-aes-dec-ecb",
        "F14-try-aes-bad",
    ],
)
@pytest.mark.parametrize("ansi", (True, False))
def test_c005_f14_cells_sql_door(spark: ReparkSession, cell_id: str, ansi: bool) -> None:
    """Pin one F14 value cell byte-exact on the SQL door under both ANSI settings."""
    cell = _F14_BY_ID[cell_id]
    _set_ansi(spark, ansi)
    schema = list(cell["schema"])
    _assert_repr_block_table(
        spark.sql(str(cell["input"])).to_arrow(),
        [column[0] for column in schema],
        [column[1] for column in schema],
        [bool(column[2]) for column in schema],
        list(cell["rows"]),
    )


@pytest.mark.parametrize(
    "cell_id",
    [
        "F14-mask",
        "F14-mask-null-repl",
        "F14-aes-gcm-rt",
        "F14-aes-ecb",
        "F14-aes-cbc-iv",
        "F14-aes-gcm-iv",
        "F14-aes-dec-ecb",
        "F14-try-aes-bad",
    ],
)
@pytest.mark.parametrize("ansi", (True, False))
def test_c005_f14_cells_python_door(spark: ReparkSession, cell_id: str, ansi: bool) -> None:
    """Pin one F14 value cell byte-exact through the facade under both ANSI settings."""
    cell = _F14_BY_ID[cell_id]
    _set_ansi(spark, ansi)
    columns = _f14_facade(cell_id)
    table = spark.range(1).select(*columns).to_arrow()
    schema = list(cell["schema"])
    assert table.num_columns == len(schema), cell_id
    for index, column in enumerate(schema):
        assert table.schema.field(index).type == _spark_simple_to_arrow(column[1]), cell_id
        assert table.schema.field(index).nullable == bool(column[2]), cell_id
        assert table.column(index).to_pylist() == [
            _decode_repr_text(row[index]) for row in cell["rows"]
        ], cell_id


def test_c006_frame_control_rows_answer(spark: ReparkSession) -> None:
    """Control the shared frame answers its seed rows, so reds are function gaps."""
    _set_ansi(spark, True)
    table = spark.sql(f"SELECT id, txt, num FROM ({_FRAME_SQL})").to_arrow()
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert table.column("txt").to_pylist() == ["Hello World. How are you?", None, "one"]
    assert table.column("num").to_pylist() == ["100", "zz", "-17"]


def test_c007_fixture_blocks_present() -> None:
    """Control the fixture carries every recorded block with identified cells."""
    assert _FIXTURE["spark_version"] == "4.1.2"
    assert set(_FIXTURE["blocks"]) == {"o245", "f14", "q12", "bl6"}
    assert len(_O245) == 106
    assert len(_F14) == 10
    assert len(_Q12) == 15
    assert len(_BL6) == 6
    assert {cell["id"] for cell in _Q12} == {f"Q12-{i}" for i in range(41, 56)}
    assert {cell["id"] for cell in _F14} == {
        "F14-mask",
        "F14-mask-null-repl",
        "F14-aes-gcm-rt",
        "F14-aes-ecb",
        "F14-aes-cbc-iv",
        "F14-aes-gcm-iv",
        "F14-aes-dec-ecb",
        "F14-aes-badkey",
        "F14-aes-dec-bad",
        "F14-try-aes-bad",
    }
    for cell in _O245:
        if "error_type" in cell and cell.get("error_condition") is None:
            assert cell["name"] == "collate", _cell_key(cell)


@pytest.mark.parametrize(
    "key",
    [
        k
        for k, c in _O245_BY_KEY.items()
        if c["name"] == "split" and c["door"] == "python" and c.get("rows") is not None
    ],
)
def test_c008_split_python_door_cells(spark: ReparkSession, key: str) -> None:
    """Pin one o245 split Python-door cell on the recorded ANSI setting."""
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, bool(cell["ansi"]))
    _assert_o245_value_table(_run_python(_frame(spark), str(cell["expr"])).to_arrow(), cell)


@pytest.mark.parametrize("cell_id", [f"Q12-{i}" for i in range(41, 56)])
@pytest.mark.parametrize("ansi", (True, False))
def test_c008_q12_split_sql_door(spark: ReparkSession, cell_id: str, ansi: bool) -> None:
    """Pin one Q12 split cell on the SQL door under both ANSI settings."""
    cell = _Q12_BY_ID[cell_id]
    _set_ansi(spark, ansi)
    table = spark.sql(f"SELECT {cell['expr']} AS v FROM ({_Q12_FRAME_SQL})").to_arrow()
    _assert_q12_table(table, cell)


@pytest.mark.parametrize("cell_id", [f"Q12-{i}" for i in range(41, 56)])
@pytest.mark.parametrize("ansi", (True, False))
def test_c008_q12_split_python_door(spark: ReparkSession, cell_id: str, ansi: bool) -> None:
    """Pin one Q12 split cell through F.split under both ANSI settings."""
    cell = _Q12_BY_ID[cell_id]
    _set_ansi(spark, ansi)
    frame = spark.createDataFrame([("a,b,,c",)], ["s"])
    _assert_q12_table(frame.select(_q12_facade(cell_id).alias("v")).to_arrow(), cell)


@pytest.mark.parametrize("cell_id", ["BL6-sql-0", "BL6-sql-1"])
@pytest.mark.parametrize("ansi", (True, False))
def test_c009_bl6_refusal_cells_sql_door(spark: ReparkSession, cell_id: str, ansi: bool) -> None:
    """Pin one BL-6 BOOLEAN refusal cell on the SQL door under both ANSI settings."""
    cell = _BL6_BY_ID[cell_id]
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        spark.sql(str(cell["input"])).to_arrow()
    assert str(cell["condition"]) in str(excinfo.value), cell_id


@pytest.mark.parametrize("cell_id", ["BL6-sql-2", "BL6-sql-3", "DIV-like-1"])
@pytest.mark.parametrize("ansi", (True, False))
def test_c009_bl6_value_cells_sql_door(spark: ReparkSession, cell_id: str, ansi: bool) -> None:
    """Pin one BL-6 value cell on the SQL door under both ANSI settings."""
    cell = _BL6_BY_ID[cell_id]
    _set_ansi(spark, ansi)
    schema = list(cell["schema"])
    _assert_repr_block_table(
        spark.sql(str(cell["input"])).to_arrow(),
        [column[0] for column in schema],
        [column[1] for column in schema],
        [bool(column[2]) for column in schema],
        list(cell["rows"]),
    )


@pytest.mark.parametrize("kind", ("bin", "rint"))
@pytest.mark.parametrize("ansi", (True, False))
def test_c009_bl6_facade_refuses_boolean(spark: ReparkSession, kind: str, ansi: bool) -> None:
    """Pin the BL-6 facade BOOLEAN refusal before the kernel under both ANSI settings."""
    condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        if kind == "bin":
            spark.range(1).select(F.bin(F.lit(True)).alias("v")).to_arrow()
        else:
            spark.range(1).select(F.rint(F.lit(True)).alias("v")).to_arrow()
    assert condition in str(excinfo.value), kind


@pytest.mark.parametrize("ansi", (True, False))
def test_c009_bl6_facade_bin_rint_values(spark: ReparkSession, ansi: bool) -> None:
    """Pin the BL-6 accepted facade values both doors share under both ANSI settings."""
    _set_ansi(spark, ansi)
    bin_table = spark.range(1).select(F.bin(F.lit(1)).alias("v")).to_arrow()
    assert bin_table.column("v").to_pylist() == ["1"]
    rint_table = spark.range(1).select(F.rint(F.lit(2.5)).alias("v")).to_arrow()
    assert rint_table.column("v").to_pylist() == [2.0]


@pytest.mark.parametrize("ansi", (True, False))
def test_c009_bl6_like_escape_facade(spark: ReparkSession, ansi: bool) -> None:
    """Pin the three-argument like escape form through the facade under both ANSI settings."""
    _set_ansi(spark, ansi)
    frame = spark.createDataFrame([("a_c",)], ["l"])
    table = frame.select(F.like("l", F.lit("a/_c"), F.lit("/")).alias("v")).to_arrow()
    assert table.column("v").to_pylist() == [True]
    assert table.schema.field("v").type == pa.boolean()
