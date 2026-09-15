"""DEGREES-RUST-1 pins: facade and SQL-door degrees/radians against DEG-* and DEGI-* cells."""

from __future__ import annotations

import json
import math
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

FIXTURE_PATH = Path(__file__).parent / "fnp_degrees_rust_1_spark_oracle.json"
_CELLS: dict[str, dict] = {
    cell["id"]: cell for cell in json.loads(FIXTURE_PATH.read_text())["cells"]
}
LITERALS: dict[str, str] = {
    "bool": "true",
    "str-num": "'1.0'",
    "str-abc": "'abc'",
    "str-empty": "''",
    "date": "DATE'2020-01-01'",
    "int": "CAST(1 AS INT)",
    "bigint": "CAST(1 AS BIGINT)",
    "decimal": "CAST(1.5 AS DECIMAL(10,1))",
    "float": "CAST(1.5 AS FLOAT)",
    "null": "CAST(NULL AS DOUBLE)",
    "timestamp": "TIMESTAMP'2020-01-01 00:00:00'",
    "binary": "X'01'",
}
ANSWER_TAGS: tuple[str, ...] = ("str-num", "int", "bigint", "decimal", "float", "null")
DEGI_LITERALS: dict[str, str] = {
    "Inf": "'Inf'",
    "inf": "'inf'",
    "neg-Inf": "'-Inf'",
    "plus-inf": "'+inf'",
    "Infinity": "'Infinity'",
    "neg-infinity": "'-infinity'",
    "INFINITY": "'INFINITY'",
    "NaN": "'NaN'",
    "nan": "'nan'",
    "NAN": "'NAN'",
    "space-Inf": "' Inf '",
    "e2": "'1e2'",
    "hex": "'0x10'",
    "neg-zero": "'-0'",
    "d-suffix": "'1d'",
    "f-suffix": "'1f'",
    "plus-one": "'+1'",
    "dot-one": "'.5'",
    "one-dot": "'1.'",
    "space-num": "' 1.0 '",
}
DEGI_TAGS: tuple[str, ...] = tuple(DEGI_LITERALS)
REFUSAL_TAGS: tuple[tuple[str, str], ...] = (
    ("bool", "BOOLEAN"),
    ("date", "DATE"),
    ("timestamp", "TIMESTAMP"),
    ("binary", "BINARY"),
)
MALFORMED_TAGS: tuple[str, ...] = ("str-abc", "str-empty")
PYTHON_DOOR_DIVERGENT_TYPE_NAMES: dict[str, str] = {"timestamp": "TIMESTAMP_LTZ"}
WARNINGS: dict[str, str] = {
    "toDegrees": "Deprecated in 2.1, use degrees instead.",
    "toRadians": "Deprecated in 2.1, use radians instead.",
}


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("fnp-degrees-rust-1").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def spark_ansi_off() -> Iterator[ReparkSession]:
    session = (
        ReparkSession.builder.appName("fnp-degrees-rust-1-ansi-off")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _cell(cell_id: str) -> dict:
    return _CELLS[cell_id]


def _python_value(raw: str) -> float | None:
    return None if raw == "None" else float(raw)


def _answer_cases() -> list[tuple[str, str, str]]:
    cases = []
    for function in ("degrees", "radians"):
        for tag in ANSWER_TAGS:
            cases.append((function, tag, "python"))
            cases.append((function, tag, "sql"))
    for function in ("toDegrees", "toRadians"):
        for tag in ANSWER_TAGS:
            cases.append((function, tag, "python"))
    return cases


_ANSWER_IDS: list[str] = [f"{function}-{tag}-{door}" for function, tag, door in _answer_cases()]


def _apply_python(function: str) -> object:
    if function in WARNINGS:
        with pytest.warns(FutureWarning, match=WARNINGS[function]):
            return getattr(F, function)("x")
    return getattr(F, function)("x")


def _answer_cell_id(function: str, tag: str, door: str, suffix: str) -> str:
    if function in ("degrees", "radians"):
        return f"DEG-{function}-{tag}-{door}{suffix}"
    return f"DEG-{function}-{tag}-python{suffix}"


def _assert_answer(session: ReparkSession, function: str, tag: str, door: str, suffix: str) -> None:
    cell_id = _answer_cell_id(function, tag, door, suffix)
    cell = _cell(cell_id)
    assert "rows" in cell, cell_id
    literal = LITERALS[tag]
    if door == "sql":
        table = session.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
        name, spark_type, nullable = cell["schema"][0]
        assert name == "r"
    else:
        frame = session.sql(f"SELECT {literal} AS x")
        table = frame.select(_apply_python(function)).toArrow()
        name, spark_type, nullable = cell["schema"][0]
        assert table.column_names == [name]
    assert spark_type == "double" and nullable is True
    field = table.schema.field(name)
    assert field.type == pa.float64() and field.nullable is True
    answered = [None if value is None else float(value) for value in table.column(name).to_pylist()]
    assert answered == [_python_value(row[0]) for row in cell["rows"]]


@pytest.mark.parametrize(("function", "tag", "door"), _answer_cases(), ids=_ANSWER_IDS)
def test_answer_matches_oracle(spark: ReparkSession, function: str, tag: str, door: str) -> None:
    """pins: fnp-bitmap-facade-1/C-012"""
    _assert_answer(spark, function, tag, door, "")


def _answer_nonansi_cases() -> list[tuple[str, str, str]]:
    cases = []
    for function in ("degrees", "radians"):
        for tag in ANSWER_TAGS:
            cases.append((function, tag, "python"))
            cases.append((function, tag, "sql"))
    for function in ("toDegrees", "toRadians"):
        for tag in ANSWER_TAGS:
            cases.append((function, tag, "python"))
    return cases


_ANSWER_NONANSI_IDS: list[str] = [
    f"{function}-{tag}-{door}-nonansi" for function, tag, door in _answer_nonansi_cases()
]


@pytest.mark.parametrize(
    ("function", "tag", "door"), _answer_nonansi_cases(), ids=_ANSWER_NONANSI_IDS
)
def test_answer_nonansi_matches_oracle(
    spark_ansi_off: ReparkSession, function: str, tag: str, door: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-012"""
    _assert_answer(spark_ansi_off, function, tag, door, "-nonansi")


def _refusal_cases() -> list[tuple[str, str, str]]:
    cases = []
    for function in ("degrees", "radians"):
        for tag, _ in REFUSAL_TAGS:
            cases.append((function, tag, "python"))
            cases.append((function, tag, "sql"))
    for function in ("toDegrees", "toRadians"):
        for tag, _ in REFUSAL_TAGS:
            cases.append((function, tag, "python"))
    return cases


def _refusal_cell_id(function: str, tag: str, door: str) -> str:
    if function in ("degrees", "radians"):
        return f"DEG-{function}-{tag}-{door}"
    return f"DEG-{function}-{tag}-python"


def _refusal_display(function: str) -> str:
    return "DEGREES(x)" if function in ("degrees", "toDegrees") else "RADIANS(x)"


_REFUSAL_IDS: list[str] = [f"{function}-{tag}-{door}" for function, tag, door in _refusal_cases()]


@pytest.mark.parametrize(("function", "tag", "door"), _refusal_cases(), ids=_REFUSAL_IDS)
def test_refusal_matches_oracle(spark: ReparkSession, function: str, tag: str, door: str) -> None:
    """pins: fnp-bitmap-facade-1/C-013"""
    cell_id = _refusal_cell_id(function, tag, door)
    display = _refusal_display(function)
    cell = _cell(cell_id)
    assert cell["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    spark_type = next(known for known_tag, known in REFUSAL_TAGS if known_tag == tag)
    if door == "python":
        spark_type = PYTHON_DOOR_DIVERGENT_TYPE_NAMES.get(tag, spark_type)
    literal = LITERALS[tag]
    with pytest.raises(Exception) as caught:
        if door == "sql":
            spark.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
        else:
            frame = spark.sql(f"SELECT {literal} AS x")
            frame.select(_apply_python(function)).collect()
    message = str(caught.value)
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert f'Cannot resolve "{display}"' in message
    assert 'The first parameter requires the "DOUBLE" type, however' in message
    assert f'has the type "{spark_type}"' in message
    assert "SQLSTATE: 42K09" in message
    if tag != "timestamp" or door != "python":
        assert cell["message"].split(" SQLSTATE")[0] in message


@pytest.mark.parametrize("function", ["degrees", "radians"])
@pytest.mark.parametrize("tag", [known_tag for known_tag, _ in REFUSAL_TAGS])
def test_refusal_sql_nonansi_matches_oracle(
    spark_ansi_off: ReparkSession, function: str, tag: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-013"""
    display = _refusal_display(function)
    cell = _cell(f"DEG-{function}-{tag}-sql-nonansi")
    assert cell["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    spark_type = next(known for known_tag, known in REFUSAL_TAGS if known_tag == tag)
    literal = LITERALS[tag]
    with pytest.raises(Exception) as caught:
        spark_ansi_off.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
    message = str(caught.value)
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert f'Cannot resolve "{display}"' in message
    assert 'The first parameter requires the "DOUBLE" type, however' in message
    assert f'has the type "{spark_type}"' in message
    assert "SQLSTATE: 42K09" in message
    assert cell["message"].split(" SQLSTATE")[0] in message


def test_wrapper_builds_without_cast_node(
    spark: ReparkSession, capsys: pytest.CaptureFixture[str]
) -> None:
    """pins: fnp-bitmap-facade-1/C-016"""
    frame = spark.createDataFrame([(1.0,)], "x double")
    frame.select(F.degrees("x")).explain(extended=True)
    frame.select(F.radians("x")).explain(extended=True)
    assert "CAST" not in capsys.readouterr().out


@pytest.mark.parametrize("function", ["degrees", "radians", "toDegrees", "toRadians"])
def test_refusal_timestamp_ltz_is_expected_divergence(spark: ReparkSession, function: str) -> None:
    """pins: fnp-bitmap-facade-1/C-013"""
    cell = _cell(_refusal_cell_id(function, "timestamp", "python"))
    assert cell["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    assert '"TIMESTAMP"' in cell["message"] and "LTZ" not in cell["message"]
    frame = spark.sql(f"SELECT {LITERALS['timestamp']} AS x")
    with pytest.raises(Exception) as caught:
        frame.select(_apply_python(function)).collect()
    message = str(caught.value)
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert 'The first parameter requires the "DOUBLE" type, however' in message
    assert 'has the type "TIMESTAMP_LTZ"' in message
    assert "SQLSTATE: 42K09" in message


def _assert_malformed_raises(session: ReparkSession, function: str, tag: str, door: str) -> None:
    if function in ("degrees", "radians"):
        cell = _cell(f"DEG-{function}-{tag}-{door}")
    else:
        cell = _cell(f"DEG-{function}-{tag}-python")
    assert cell["condition"] == "CAST_INVALID_INPUT"
    literal = LITERALS[tag]
    with pytest.raises(Exception) as caught:
        if door == "sql":
            session.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
        else:
            session.sql(f"SELECT {literal} AS x").select(_apply_python(function)).collect()
    message = str(caught.value)
    assert "[CAST_INVALID_INPUT]" in message
    value = "abc" if tag == "str-abc" else ""
    assert f'The value \'{value}\' of the type "STRING" cannot be cast to "DOUBLE"' in message
    assert "SQLSTATE: 22018" in message


@pytest.mark.parametrize("function", ["degrees", "radians", "toDegrees", "toRadians"])
@pytest.mark.parametrize("tag", MALFORMED_TAGS)
def test_malformed_string_raises_cast_invalid_input(
    spark: ReparkSession, function: str, tag: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-014"""
    _assert_malformed_raises(spark, function, tag, "python")


@pytest.mark.parametrize("function", ["degrees", "radians"])
@pytest.mark.parametrize("tag", MALFORMED_TAGS)
def test_malformed_string_sql_raises_cast_invalid_input(
    spark: ReparkSession, function: str, tag: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-014"""
    _assert_malformed_raises(spark, function, tag, "sql")


def _assert_malformed_nulls(session: ReparkSession, function: str, tag: str, door: str) -> None:
    if function in ("degrees", "radians"):
        cell = _cell(f"DEG-{function}-{tag}-{door}-nonansi")
    else:
        cell = _cell(f"DEG-{function}-{tag}-python-nonansi")
    assert cell["rows"] == [["None"]]
    literal = LITERALS[tag]
    if door == "sql":
        table = session.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
    else:
        table = session.sql(f"SELECT {literal} AS x").select(_apply_python(function)).toArrow()
    assert table.column(0).to_pylist() == [None]


@pytest.mark.parametrize("function", ["degrees", "radians", "toDegrees", "toRadians"])
@pytest.mark.parametrize("tag", MALFORMED_TAGS)
def test_malformed_string_ansi_off_answers_null(
    spark_ansi_off: ReparkSession, function: str, tag: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-014"""
    _assert_malformed_nulls(spark_ansi_off, function, tag, "python")


@pytest.mark.parametrize("function", ["degrees", "radians"])
@pytest.mark.parametrize("tag", MALFORMED_TAGS)
def test_malformed_string_sql_ansi_off_answers_null(
    spark_ansi_off: ReparkSession, function: str, tag: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-014"""
    _assert_malformed_nulls(spark_ansi_off, function, tag, "sql")


def _degi_cases() -> list[tuple[str, str, str]]:
    cases = []
    for function in ("degrees", "radians"):
        for tag in DEGI_TAGS:
            cases.append((function, tag, "python"))
            cases.append((function, tag, "sql"))
    return cases


_DEGI_IDS: list[str] = [f"{function}-{tag}-{door}" for function, tag, door in _degi_cases()]


def _assert_degi_value(answered: list, raw: str, cell_id: str) -> None:
    assert len(answered) == 1, cell_id
    value = answered[0]
    if raw == "None":
        assert value is None, cell_id
    elif raw == "nan":
        assert value is not None and math.isnan(value), cell_id
    elif raw in ("inf", "-inf"):
        assert value is not None and math.isinf(value), cell_id
        assert (value > 0.0) == (raw == "inf"), cell_id
    else:
        assert value == float(raw), cell_id


def _assert_degi(session: ReparkSession, function: str, tag: str, door: str, suffix: str) -> None:
    cell_id = f"DEGI-{function}-{tag}-{door}{suffix}"
    cell = _cell(cell_id)
    literal = DEGI_LITERALS[tag]
    if "rows" not in cell:
        assert cell["condition"] == "CAST_INVALID_INPUT", cell_id
        with pytest.raises(Exception) as caught:
            if door == "sql":
                session.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
            else:
                session.sql(f"SELECT {literal} AS x").select(getattr(F, function)("x")).collect()
        message = str(caught.value)
        assert "[CAST_INVALID_INPUT]" in message
        assert (
            f"The value '{literal[1:-1]}' of the type \"STRING\" "
            'cannot be cast to "DOUBLE"' in message
        )
        assert "SQLSTATE: 22018" in message
        return
    if door == "sql":
        table = session.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
        name = "r"
    else:
        table = session.sql(f"SELECT {literal} AS x").select(getattr(F, function)("x")).toArrow()
        name = _refusal_display(function)
        assert table.column_names == [name]
    expected_name, spark_type, nullable = cell["schema"][0]
    assert name == expected_name and spark_type == "double" and nullable is True
    field = table.schema.field(name)
    assert field.type == pa.float64() and field.nullable is True
    _assert_degi_value(table.column(name).to_pylist(), cell["rows"][0][0], cell_id)


@pytest.mark.parametrize(("function", "tag", "door"), _degi_cases(), ids=_DEGI_IDS)
def test_degi_matches_oracle(spark: ReparkSession, function: str, tag: str, door: str) -> None:
    """pins: fnp-bitmap-facade-1/C-017, C-018"""
    _assert_degi(spark, function, tag, door, "")


_DEGI_NONANSI_IDS: list[str] = [
    f"{function}-{tag}-{door}-nonansi" for function, tag, door in _degi_cases()
]


@pytest.mark.parametrize(("function", "tag", "door"), _degi_cases(), ids=_DEGI_NONANSI_IDS)
def test_degi_nonansi_matches_oracle(
    spark_ansi_off: ReparkSession, function: str, tag: str, door: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-017, C-018"""
    _assert_degi(spark_ansi_off, function, tag, door, "-nonansi")


@pytest.mark.parametrize("function", ["degrees", "radians", "toDegrees", "toRadians"])
@pytest.mark.parametrize("tag", [known_tag for known_tag, _ in REFUSAL_TAGS])
def test_refusal_ansi_off_still_refuses(
    spark_ansi_off: ReparkSession, function: str, tag: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-013"""
    cell_id = f"DEG-{function}-{tag}-python-nonansi"
    cell = _cell(cell_id)
    assert cell["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    literal = LITERALS[tag]
    frame = spark_ansi_off.sql(f"SELECT {literal} AS x")
    with pytest.raises(Exception) as caught:
        frame.select(_apply_python(function)).collect()
    message = str(caught.value)
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert "SQLSTATE: 42K09" in message
