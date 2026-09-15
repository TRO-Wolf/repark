"""DEGREES-RUST-1 pins: facade and SQL-door degrees/radians against DEG-* cells."""

from __future__ import annotations

import json
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
REFUSAL_TAGS: tuple[tuple[str, str], ...] = (
    ("bool", "BOOLEAN"),
    ("date", "DATE"),
    ("timestamp", "TIMESTAMP"),
    ("binary", "BINARY"),
)
MALFORMED_TAGS: tuple[str, ...] = ("str-abc", "str-empty")
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


@pytest.mark.parametrize(
    ("function", "tag", "door"), _answer_cases(), ids=_ANSWER_IDS
)
def test_answer_matches_oracle(
    spark: ReparkSession, function: str, tag: str, door: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-012"""
    if function in ("degrees", "radians"):
        cell_id = f"DEG-{function}-{tag}-{door}"
    else:
        cell_id = f"DEG-{function}-{tag}-python"
    cell = _cell(cell_id)
    assert "rows" in cell, cell_id
    literal = LITERALS[tag]
    if door == "sql":
        table = spark.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
        name, spark_type, nullable = cell["schema"][0]
        assert name == "r"
    else:
        frame = spark.sql(f"SELECT {literal} AS x")
        table = frame.select(_apply_python(function)).toArrow()
        name, spark_type, nullable = cell["schema"][0]
        assert table.column_names == [name]
    assert spark_type == "double" and nullable is True
    field = table.schema.field(name)
    assert field.type == pa.float64() and field.nullable is True
    answered = [None if value is None else float(value) for value in table.column(name).to_pylist()]
    assert answered == [_python_value(row[0]) for row in cell["rows"]]


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


_REFUSAL_IDS: list[str] = [f"{function}-{tag}-{door}" for function, tag, door in _refusal_cases()]


@pytest.mark.parametrize(
    ("function", "tag", "door"), _refusal_cases(), ids=_REFUSAL_IDS
)
def test_refusal_matches_oracle(
    spark: ReparkSession, function: str, tag: str, door: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-013"""
    if function in ("degrees", "radians"):
        cell_id = f"DEG-{function}-{tag}-{door}"
        display = "DEGREES(x)" if function == "degrees" else "RADIANS(x)"
    else:
        cell_id = f"DEG-{function}-{tag}-python"
        display = "DEGREES(x)" if function == "toDegrees" else "RADIANS(x)"
    cell = _cell(cell_id)
    assert cell["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    spark_type = next(known for known_tag, known in REFUSAL_TAGS if known_tag == tag)
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
    assert cell["message"].split(" SQLSTATE")[0] in message


@pytest.mark.parametrize("function", ["degrees", "radians"])
@pytest.mark.parametrize("tag", MALFORMED_TAGS)
@pytest.mark.parametrize("door", ["python", "sql"])
def test_malformed_string_raises_cast_invalid_input(
    spark: ReparkSession, function: str, tag: str, door: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-014"""
    cell = _cell(f"DEG-{function}-{tag}-{door}")
    assert cell["condition"] == "CAST_INVALID_INPUT"
    literal = LITERALS[tag]
    with pytest.raises(Exception) as caught:
        if door == "sql":
            spark.sql(f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)").toArrow()
        else:
            spark.sql(f"SELECT {literal} AS x").select(getattr(F, function)("x")).collect()
    message = str(caught.value)
    assert "[CAST_INVALID_INPUT]" in message
    value = "abc" if tag == "str-abc" else ""
    assert f'The value \'{value}\' of the type "STRING" cannot be cast to "DOUBLE"' in message
    assert "SQLSTATE: 22018" in message


@pytest.mark.parametrize("function", ["degrees", "radians"])
@pytest.mark.parametrize("tag", MALFORMED_TAGS)
@pytest.mark.parametrize("door", ["python", "sql"])
def test_malformed_string_ansi_off_answers_null(
    spark_ansi_off: ReparkSession, function: str, tag: str, door: str
) -> None:
    """pins: fnp-bitmap-facade-1/C-014"""
    cell = _cell(f"DEG-{function}-{tag}-{door}-nonansi")
    assert cell["rows"] == [["None"]]
    literal = LITERALS[tag]
    if door == "sql":
        table = spark_ansi_off.sql(
            f"SELECT {function}(x) AS r FROM (SELECT {literal} AS x)"
        ).toArrow()
    else:
        table = spark_ansi_off.sql(f"SELECT {literal} AS x").select(
            getattr(F, function)("x")
        ).toArrow()
    assert table.column(0).to_pylist() == [None]


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
