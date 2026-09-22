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

_FNP_FENCE_REASON = "FNP-MATH-1 fence: {} not built in this PR; flips when the follow-up lands"

_C001_FENCED: frozenset[str] = frozenset(
    {"collate", "collation", "sentences", "aes_encrypt", "aes_decrypt", "try_aes_decrypt"}
)
_C002_FENCED: frozenset[str] = frozenset(
    {"collate", "collation", "sentences", "locate", "array_join"}
)
_C003_FENCED: frozenset[str] = frozenset({"collate", "locate", "sentences", "array_join"})
_C004_FENCED: frozenset[str] = frozenset({"collate", "aes_decrypt"})
_C005_FENCED: frozenset[str] = frozenset({"aes_encrypt", "aes_decrypt", "try_aes_decrypt"})
_F14_FENCE_BY_ID: dict[str, str] = {
    "F14-aes-badkey": "aes_encrypt",
    "F14-aes-dec-bad": "aes_decrypt",
    "F14-aes-gcm-rt": "aes_decrypt",
    "F14-aes-ecb": "aes_encrypt",
    "F14-aes-cbc-iv": "aes_encrypt",
    "F14-aes-gcm-iv": "aes_encrypt",
    "F14-aes-dec-ecb": "aes_decrypt",
    "F14-try-aes-bad": "try_aes_decrypt",
}


def _fnp_fence(name: str) -> Any:
    """Build the strict FNP-MATH-1 xfail mark for one unbuilt function."""
    return pytest.mark.xfail(strict=True, reason=_FNP_FENCE_REASON.format(name))


def _fenced_key(key: str, names: frozenset[str]) -> Any:
    """Strict-xfail one parametrized key whose leading function is not built in this PR."""
    name = key.split("|", 1)[0]
    if name in names:
        return pytest.param(key, marks=_fnp_fence(name))
    return key


def _fenced_cell(cell_id: str) -> Any:
    """Strict-xfail one F14 cell whose function is not built in this PR."""
    name = _F14_FENCE_BY_ID.get(cell_id)
    if name is None:
        return cell_id
    return pytest.param(cell_id, marks=_fnp_fence(name))


_COLLATE_UNKNOWN_CONDITION = "COLLATION_INVALID_NAME"

_FRAME_VALUES = (
    "SELECT * FROM VALUES "
    "(1, CAST(2.5 AS DOUBLE), CAST(-2.5 AS DOUBLE), CAST(12345.6789 AS DECIMAL(10,4)), "
    "'Hello World. How are you?', 'a,b,,c', '100', 'abcd-EFG-123', '{\"a\":1}', "
    "'1,abc,2.5', '<p/>', "
    "array(named_struct('x', 1, 'y', 'p'), named_struct('x', 2, 'y', 'q')), "
    "array(CAST(10 AS INT), CAST(20 AS INT)), 'k1'), "
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
    """Map one Spark simpleString to the Arrow type the door must answer.

    SimpleString carries no element nullability; every array-typed cell in this
    suite is a ``split`` output, whose Q12 block records ``containsNull`` false,
    and the door renders list elements under the engine ``element`` field.
    """
    if text == "string collate UTF8_LCASE":
        return pa.string()
    if text.startswith("array<") and text.endswith(">"):
        inner = _spark_simple_to_arrow(text[len("array<") : -1])
        if inner.equals(pa.string()):
            return pa.list_(pa.field("element", inner, nullable=False))
        return pa.list_(inner)
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
    element = pa.field("element", _q12_arrow(node["elementType"]), nullable=node["containsNull"])
    return pa.list_(element)


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


_SQLSTATE_RE = re.compile(r"SQLSTATE:\s*([A-Z0-9]{5})")


def _recorded_sqlstate(cell: dict[str, Any]) -> str | None:
    """Read the SQLSTATE the oracle message records, when it records one."""
    match = _SQLSTATE_RE.search(str(cell.get("message") or ""))
    return match.group(1) if match is not None else None


def _assert_recorded_error(
    error: BaseException, error_type: str, condition: str, sqlstate: str | None, key: str
) -> None:
    """Pin one refusal's recorded class, condition and SQLSTATE."""
    assert type(error).__name__ == error_type, key
    assert error.getErrorClass() == condition, key  # type: ignore[attr-defined]
    assert error.getSqlState() == sqlstate, key  # type: ignore[attr-defined]


def _frame(spark: ReparkSession) -> DataFrame:
    """Build the shared o245 input frame with per-row timestamps."""
    return spark.sql(_FRAME_SQL)


def _run_python(frame: DataFrame, expr: str) -> DataFrame:
    """Run one recorded Python-door expression over the frame."""
    namespace: dict[str, Any] = {"F": F, "col": col, "lit": lit, "__builtins__": {}}
    return frame.select(eval(expr, namespace))


def _select_items(query: str) -> list[str]:
    """Split one recorded SELECT list into items, honouring quotes and parens."""
    body = query.strip()
    assert body[:6].upper() == "SELECT"
    body = body[6:]
    from_index = body.upper().rfind(" FROM ")
    assert from_index >= 0
    items: list[str] = []
    depth = 0
    quote: str | None = None
    start = 0
    for index, char in enumerate(body[:from_index]):
        if quote is not None:
            if char == quote:
                quote = None
        elif char in ("'", '"'):
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            items.append(body[start:index].strip())
            start = index + 1
    items.append(body[start:from_index].strip())
    return [item for item in items if item]


def _assert_o245_single_column(
    spark: ReparkSession, expr: str, frame_sql: str, cell: dict[str, Any], index: int
) -> None:
    """Pin one recorded select item through its own query on the SQL door.

    DataFusion requires unique projection names while Spark answers duplicate
    names (mask columns 0 and 3 both render ``mask(masked, X, x, n, NULL)``),
    and its projection optimizer cannot keep an arg-rendered UDF name stable
    when one column repeats across items (split over ``csvs``); each recorded
    item therefore runs alone, keeping its recorded name, type and rows.
    """
    item_query = f"SELECT {_select_items(expr)[index]} FROM ({frame_sql})"
    table = spark.sql(item_query).to_arrow()
    columns = list(cell["columns"])
    rows = list(cell["rows"])
    sub = {"columns": [columns[index]], "rows": [[row[index]] for row in rows]}
    _assert_o245_value_table(table, sub)


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


@pytest.mark.parametrize("name", [_fenced_key(name, _C001_FENCED) for name in _C001_NAMES])
def test_c001_name_present_with_spark_parameters(name: str) -> None:
    """Pin one D-6 name on the facade with PySpark 4.1.2 parameter names."""
    assert hasattr(F, name), name
    assert list(inspect.signature(getattr(F, name)).parameters) == _spark_param_names(_SIGS[name])


@pytest.mark.parametrize("name", ("bin", "rint", "like"))
def test_c001_presence_without_recorded_signature(name: str) -> None:
    """Pin one BL-6 name present; its exact PySpark signature has no recorded cell yet."""
    assert hasattr(F, name), name


@pytest.mark.parametrize(
    "key",
    [
        _fenced_key(key, _C002_FENCED)
        for key in _o245_value_keys("python", frozenset({"split"}) | _C005_NAMES)
    ],
)
def test_c002_python_door_value_cells(spark: ReparkSession, key: str) -> None:
    """Pin one o245 Python-door value cell on the recorded ANSI setting."""
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, bool(cell["ansi"]))
    _assert_o245_value_table(_run_python(_frame(spark), str(cell["expr"])).to_arrow(), cell)


@pytest.mark.parametrize(
    "key", [_fenced_key(key, _C003_FENCED) for key in _o245_value_keys("sql", _C005_NAMES)]
)
def test_c003_sql_door_value_cells(spark: ReparkSession, key: str) -> None:
    """Pin one o245 SQL-door value cell on the recorded ANSI setting."""
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, bool(cell["ansi"]))
    expr = str(cell["expr"])
    query = expr.replace("FRAME", f"({_FRAME_SQL})")
    if cell["name"] in ("mask", "split") and len(_select_items(expr)) > 1:
        for index in range(len(cell["columns"])):
            _assert_o245_single_column(spark, expr, _FRAME_SQL, cell, index)
        return
    _assert_o245_value_table(spark.sql(query).to_arrow(), cell)


@pytest.mark.parametrize(
    "key",
    [_fenced_key(key, _C004_FENCED) for key, c in _O245_BY_KEY.items() if "error_type" in c],
)
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
    _assert_recorded_error(
        excinfo.value,
        str(cell["error_type"]),
        str(condition),
        _recorded_sqlstate(cell),
        key,
    )


@pytest.mark.parametrize(
    "cell_id", [_fenced_cell(c) for c in ("F14-aes-badkey", "F14-aes-dec-bad")]
)
@pytest.mark.parametrize("ansi", (True, False))
def test_c004_f14_aes_error_cells_sql_door(spark: ReparkSession, cell_id: str, ansi: bool) -> None:
    """Pin one F14 AES error cell on the SQL door under both ANSI settings."""
    cell = _F14_BY_ID[cell_id]
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        spark.sql(str(cell["input"])).to_arrow()
    _assert_recorded_error(
        excinfo.value,
        str(cell["error_type"]),
        str(cell["condition"]),
        _recorded_sqlstate(cell),
        cell_id,
    )


_F14_SHORT_ID: dict[str, str] = {"badkey": "F14-aes-badkey", "badvalue": "F14-aes-dec-bad"}


@pytest.mark.parametrize(
    ("cell_id", "condition"),
    (
        pytest.param(
            "badkey",
            "INVALID_PARAMETER_VALUE.AES_KEY_LENGTH",
            marks=_fnp_fence("aes_encrypt"),
        ),
        pytest.param(
            "badvalue",
            "INVALID_PARAMETER_VALUE.AES_CRYPTO_ERROR",
            marks=_fnp_fence("aes_decrypt"),
        ),
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
    oracle = _F14_BY_ID[_F14_SHORT_ID[cell_id]]
    _assert_recorded_error(
        excinfo.value,
        str(oracle["error_type"]),
        condition,
        _recorded_sqlstate(oracle),
        cell_id,
    )


_REFUSAL_TEMPLATE = (
    '[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "{name}(<expr>)" due to '
    'data type mismatch: The {ordinal} parameter requires the "{required}" type, however '
    'the argument has the type "{got}". SQLSTATE: 42K09'
)


@pytest.mark.parametrize("ansi", (True, False))
def test_c004_mask_later_arg_refusal_sql_door(spark: ReparkSession, ansi: bool) -> None:
    """Pin the mask second-argument refusal on the SQL door under both ANSI settings."""
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        spark.sql("SELECT mask('Ab', 1)").to_arrow()
    _assert_recorded_error(
        excinfo.value,
        "AnalysisException",
        "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        "42K09",
        "mask-sql-second-int",
    )
    assert str(excinfo.value) == _REFUSAL_TEMPLATE.format(
        name="mask", ordinal="second", required="STRING", got="BIGINT"
    ), "mask-sql-second-int"


@pytest.mark.parametrize("ansi", (True, False))
def test_c004_mask_later_arg_refusal_python_door(spark: ReparkSession, ansi: bool) -> None:
    """Pin the mask second-argument refusal through the facade under both ANSI settings."""
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        spark.range(1).select(F.mask(F.lit("Ab"), F.lit(1)).alias("v")).to_arrow()
    _assert_recorded_error(
        excinfo.value,
        "AnalysisException",
        "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        "42K09",
        "mask-python-second-int",
    )
    assert str(excinfo.value) == _REFUSAL_TEMPLATE.format(
        name="mask", ordinal="second", required="STRING", got="INT"
    ), "mask-python-second-int"


@pytest.mark.parametrize("ansi", (True, False))
def test_c004_conv_later_arg_refusal_sql_door(spark: ReparkSession, ansi: bool) -> None:
    """Pin the conv fromBase refusal naming the second parameter on the SQL door."""
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        spark.sql("SELECT conv('1', 1.5, 10)").to_arrow()
    _assert_recorded_error(
        excinfo.value,
        "AnalysisException",
        "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        "42K09",
        "conv-sql-base-double",
    )
    assert str(excinfo.value) == _REFUSAL_TEMPLATE.format(
        name="conv", ordinal="second", required="INT", got="DECIMAL128(2, 1)"
    ), "conv-sql-base-double"


@pytest.mark.parametrize("ansi", (True, False))
def test_c004_bround_later_arg_refusal_sql_door(spark: ReparkSession, ansi: bool) -> None:
    """Pin the bround scale refusal naming the second parameter on the SQL door."""
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        spark.sql("SELECT bround(CAST(1.5 AS DOUBLE), 1.5)").to_arrow()
    _assert_recorded_error(
        excinfo.value,
        "AnalysisException",
        "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        "42K09",
        "bround-sql-scale-double",
    )
    assert str(excinfo.value) == _REFUSAL_TEMPLATE.format(
        name="bround", ordinal="second", required="INT", got="DECIMAL128(2, 1)"
    ), "bround-sql-scale-double"


_HASH_SQL_XFAIL = (
    "run 18c owns the SQL planner -0.0 fold: CAST(-0.0 AS DOUBLE) plans identical "
    "to CAST(0.0 AS DOUBLE), so the 12-column hash SELECT fails projection-name "
    "uniqueness before any kernel runs; registry EX-FN-7-RESID-1"
)


def _c005_param(key: str) -> Any:
    """Wrap one C-005 key, strict-xfailing the unbuilt AES names."""
    return _fenced_key(key, _C005_FENCED)


@pytest.mark.parametrize(
    "key",
    [
        _c005_param(k)
        for k, c in _O245_BY_KEY.items()
        if c.get("rows") is not None and c["name"] in _C005_NAMES
    ],
)
def test_c005_o245_exact_hash_aes_cells(spark: ReparkSession, key: str) -> None:
    """Pin one o245 hash or AES cell byte-exact on its recorded door and ANSI setting."""
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, bool(cell["ansi"]))
    if cell["door"] == "python":
        table = _run_python(_frame(spark), str(cell["expr"])).to_arrow()
    elif cell["name"] == "hash" and len(_select_items(str(cell["expr"]))) > 1:
        for index in range(len(cell["columns"])):
            _assert_o245_single_column(spark, str(cell["expr"]), _FRAME_SQL, cell, index)
        return
    else:
        query = str(cell["expr"]).replace("FRAME", f"({_FRAME_SQL})")
        table = spark.sql(query).to_arrow()
    _assert_o245_value_table(table, cell)


@pytest.mark.xfail(strict=True, reason=_HASH_SQL_XFAIL)
@pytest.mark.parametrize("ansi", (True, False))
def test_c005_hash_sql_full_select_negzero_collision(spark: ReparkSession, ansi: bool) -> None:
    """Pin the 12-column hash SELECT's -0.0 collision on the SQL door, both ANSI settings."""
    key = next(
        key
        for key, cell in _O245_BY_KEY.items()
        if cell["name"] == "hash" and cell["door"] == "sql" and cell.get("rows") is not None
    )
    cell = _O245_BY_KEY[key]
    _set_ansi(spark, ansi)
    query = str(cell["expr"]).replace("FRAME", f"({_FRAME_SQL})")
    try:
        spark.sql(query).to_arrow()
    except Exception as exc:
        assert type(exc).__name__ == "AnalysisException", key
        assert "Projections require unique expression names" in str(exc), key
        raise


@pytest.mark.parametrize(
    "cell_id",
    [
        _fenced_cell(c)
        for c in (
            "F14-mask",
            "F14-mask-null-repl",
            "F14-aes-gcm-rt",
            "F14-aes-ecb",
            "F14-aes-cbc-iv",
            "F14-aes-gcm-iv",
            "F14-aes-dec-ecb",
            "F14-try-aes-bad",
        )
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
        _fenced_cell(c)
        for c in (
            "F14-mask",
            "F14-mask-null-repl",
            "F14-aes-gcm-rt",
            "F14-aes-ecb",
            "F14-aes-cbc-iv",
            "F14-aes-gcm-iv",
            "F14-aes-dec-ecb",
            "F14-try-aes-bad",
        )
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
    _assert_recorded_error(
        excinfo.value,
        str(cell["error_type"]),
        str(cell["condition"]),
        _recorded_sqlstate(cell),
        cell_id,
    )


@pytest.mark.parametrize(
    "cell_id",
    [
        "BL6-sql-2",
        pytest.param(
            "BL6-sql-3",
            marks=pytest.mark.xfail(
                strict=True,
                reason="run 18c owns the SQL parser 2.5D DOUBLE-suffix fence",
            ),
        ),
        pytest.param(
            "DIV-like-1",
            marks=pytest.mark.xfail(
                strict=True,
                reason="LIT-DECIMAL-1 owns like/ilike escapeChar (R-17a-6)",
            ),
        ),
    ],
)
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
    oracle = _BL6_BY_ID["BL6-sql-0" if kind == "bin" else "BL6-sql-1"]
    _set_ansi(spark, ansi)
    with pytest.raises(Exception) as excinfo:
        if kind == "bin":
            spark.range(1).select(F.bin(F.lit(True)).alias("v")).to_arrow()
        else:
            spark.range(1).select(F.rint(F.lit(True)).alias("v")).to_arrow()
    _assert_recorded_error(
        excinfo.value,
        str(oracle["error_type"]),
        str(oracle["condition"]),
        _recorded_sqlstate(oracle),
        kind,
    )


@pytest.mark.parametrize("ansi", (True, False))
def test_c009_bl6_facade_bin_rint_values(spark: ReparkSession, ansi: bool) -> None:
    """Pin the BL-6 accepted facade values both doors share under both ANSI settings."""
    _set_ansi(spark, ansi)
    bin_table = spark.range(1).select(F.bin(F.lit(1)).alias("v")).to_arrow()
    assert bin_table.column("v").to_pylist() == ["1"]
    rint_table = spark.range(1).select(F.rint(F.lit(2.5)).alias("v")).to_arrow()
    assert rint_table.column("v").to_pylist() == [2.0]


@pytest.mark.parametrize("ansi", (True, False))
@pytest.mark.xfail(strict=True, reason="LIT-DECIMAL-1 owns like/ilike escapeChar (R-17a-6)")
def test_c009_bl6_like_escape_facade(spark: ReparkSession, ansi: bool) -> None:
    """Pin the three-argument like escape form through the facade under both ANSI settings."""
    _set_ansi(spark, ansi)
    frame = spark.createDataFrame([("a_c",)], ["l"])
    table = frame.select(F.like("l", F.lit("a/_c"), F.lit("/")).alias("v")).to_arrow()
    assert table.column("v").to_pylist() == [True]
    assert table.schema.field("v").type == pa.boolean()
