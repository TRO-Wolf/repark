"""SQL-door pins for the FNP-6D followup bitmap signature fix."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession

_FIXTURE_PATH = Path(__file__).with_name("fnp_6d_followup_1_spark_oracle.json")
_FIXTURE: dict[str, Any] = json.loads(_FIXTURE_PATH.read_text())
_CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in _FIXTURE["cells"]}

OR_AND_REFUSALS: tuple[tuple[str, str], ...] = (
    (
        "FU-or-int",
        "SELECT bitmap_or_agg(x) b FROM VALUES (CAST(1 AS INT)), (CAST(2 AS INT)) AS t(x)",
    ),
    (
        "FU-and-int",
        "SELECT bitmap_and_agg(x) b FROM VALUES (CAST(1 AS INT)), (CAST(2 AS INT)) AS t(x)",
    ),
    (
        "FU-or-bigint-bitpos",
        "SELECT bitmap_or_agg(bitmap_bit_position(x)) b FROM VALUES (1), (2) AS t(x)",
    ),
    (
        "FU-or-float",
        "SELECT bitmap_or_agg(x) b FROM VALUES (CAST(1.0 AS FLOAT)) AS t(x)",
    ),
    (
        "FU-or-double",
        "SELECT bitmap_or_agg(x) b FROM VALUES (CAST(1.0 AS DOUBLE)) AS t(x)",
    ),
    (
        "FU-or-bool",
        "SELECT bitmap_or_agg(x) b FROM VALUES (true) AS t(x)",
    ),
    (
        "FU-and-bool",
        "SELECT bitmap_and_agg(x) b FROM VALUES (true) AS t(x)",
    ),
    (
        "FU-or-string",
        "SELECT bitmap_or_agg(x) b FROM VALUES ('abc') AS t(x)",
    ),
    (
        "FU-and-string",
        "SELECT bitmap_and_agg(x) b FROM VALUES ('abc') AS t(x)",
    ),
    (
        "FU-or-decimal",
        "SELECT bitmap_or_agg(x) b FROM VALUES (CAST(1.5 AS DECIMAL(2,1))) AS t(x)",
    ),
    (
        "FU-or-date",
        "SELECT bitmap_or_agg(x) b FROM VALUES (DATE'2020-01-01') AS t(x)",
    ),
    (
        "FU-or-null-literal",
        "SELECT bitmap_or_agg(NULL) b FROM VALUES (1) AS t(x)",
    ),
)

CONSTRUCT_REFUSALS: tuple[tuple[str, str], ...] = (
    (
        "FU-construct-bool",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (true) AS t(x)",
    ),
    (
        "FU-construct-binary",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (X'01') AS t(x)",
    ),
    (
        "FU-construct-date",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (DATE'2020-01-01') AS t(x)",
    ),
)

CONSTRUCT_MALFORMED: tuple[tuple[str, str], ...] = (
    (
        "FU-construct-abc",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('abc') AS t(x)",
    ),
    (
        "FU-construct-empty-str",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('') AS t(x)",
    ),
    (
        "FU-construct-1.5-str",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('1.5') AS t(x)",
    ),
    (
        "FU-construct-str-mixed",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('1'), ('abc') AS t(x)",
    ),
    (
        "FU2-construct-str-overflow",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES ('9223372036854775808') AS t(x)",
    ),
    (
        "FU2-construct-str-u64",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES ('18446744073709551615') AS t(x)",
    ),
)

CONSTRUCT_ANSWERS: tuple[tuple[str, str, int], ...] = (
    (
        "FU-construct-float",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (CAST(1.0 AS FLOAT)) AS t(x)",
        1,
    ),
    (
        "FU-construct-float-frac",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (CAST(1.7 AS FLOAT)) AS t(x)",
        1,
    ),
    (
        "FU-construct-double",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (CAST(2.0 AS DOUBLE)) AS t(x)",
        1,
    ),
    (
        "FU-construct-decimal",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (CAST(1.5 AS DECIMAL(2,1))) AS t(x)",
        1,
    ),
    (
        "FU-construct-space-str",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (' 1 ') AS t(x)",
        1,
    ),
    (
        "FU2-construct-str-plus",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('+1') AS t(x)",
        1,
    ),
    (
        "FU-construct-null-literal",
        "SELECT bitmap_count(bitmap_construct_agg(NULL)) c FROM VALUES (1) AS t(x)",
        0,
    ),
    (
        "construct-int",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (CAST(3 AS INT)) AS t(x)",
        1,
    ),
    (
        "construct-null-bigint",
        "SELECT bitmap_count(bitmap_construct_agg(CAST(NULL AS BIGINT))) c FROM VALUES (1) AS t(x)",
        0,
    ),
)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("fnp-6d-followup-1").getOrCreate()
    yield session
    session.stop()


_CLASS_RE = re.compile(r"\[([A-Z_]+(?:\.[A-Z_]+)?)\]")
_REQUIRED_RE = re.compile(r'The first parameter requires the "(BINARY|BIGINT)" type')
_TYPE_RE = re.compile(r'has the type "([^"]+)"')
_STATE_RE = re.compile(r"SQLSTATE: (\d+)")
_OVERFLOW_VALUE_RE = re.compile(
    r'The value (\S+) of the type "([A-Z0-9(),]+)" cannot be cast to "BIGINT"'
)
_STRING_VALUE_RE = re.compile(
    r'The value \'([^\']*)\' of the type "STRING" cannot be cast to "BIGINT"'
)
_CALL_RE = re.compile(r'Cannot resolve "(bitmap_(?:or|and|construct)_agg)\(')
_POSITION_RE = re.compile(r"bitmap position (-?\d+)")
_RENDERING_QUALIFIERS: dict[str, str] = {
    "FU-or-bigint-bitpos": "bitmap_bit_position(t.x)",
}


def _refusal_needles(cell_id: str) -> list[str]:
    """Extract the fixture message core needles for cell_id."""
    cell = _CELLS[cell_id]
    message = str(cell["message"])
    class_match = _CLASS_RE.search(message)
    state_match = _STATE_RE.search(message)
    assert class_match is not None, f"no class in {cell_id}"
    assert state_match is not None, f"no SQLSTATE in {cell_id}"
    needles = [class_match.group(0), f"SQLSTATE: {state_match.group(1)}"]
    required_match = _REQUIRED_RE.search(message)
    if required_match is not None:
        needles.append(required_match.group(0))
    type_match = _TYPE_RE.search(message)
    if type_match is not None:
        needles.append(f'has the type "{type_match.group(1)}"')
    overflow_match = _OVERFLOW_VALUE_RE.search(message)
    if overflow_match is not None:
        needles.append(
            f"The value {overflow_match.group(1)} of the type "
            f'"{overflow_match.group(2)}" cannot be cast to "BIGINT"'
        )
    string_match = _STRING_VALUE_RE.search(message)
    if string_match is not None:
        needles.append(
            f'The value \'{string_match.group(1)}\' of the type "STRING" cannot be cast to "BIGINT"'
        )
    position_match = _POSITION_RE.search(message)
    if position_match is not None:
        needles.append(f"bitmap position {position_match.group(1)}")
    return needles


def _refusal_message(spark: ReparkSession, cell_id: str, sql: str) -> str:
    """Run sql and assert the door error carries the fixture cell core."""
    message = refuse_text(spark, sql)
    call_match = _CALL_RE.search(str(_CELLS[cell_id].get("message", "")))
    if call_match is not None:
        assert f'Cannot resolve "{call_match.group(1)}(' in message
    for needle in _refusal_needles(cell_id):
        assert needle in message
    if cell_id in _RENDERING_QUALIFIERS:
        assert _RENDERING_QUALIFIERS[cell_id] in message
    return message


def refuse_text(spark: ReparkSession, sql: str) -> str:
    """Run sql and return its error text, failing the test when sql answers."""
    with pytest.raises(Exception) as caught:
        spark.sql(sql).toArrow()
    return str(caught.value)


def count_cell(spark: ReparkSession, sql: str) -> int:
    """Run a single-row count query and return its integer cell."""
    table = spark.sql(sql).toArrow()
    values = table.column(0).to_pylist()
    assert len(values) == 1
    value = values[0]
    assert isinstance(value, int)
    return value


@pytest.mark.parametrize(
    ("cell_id", "sql"),
    OR_AND_REFUSALS,
    ids=[row[0] for row in OR_AND_REFUSALS],
)
def test_or_and_agg_refuses_non_binary_payload(
    spark: ReparkSession, cell_id: str, sql: str
) -> None:
    """pins: fnp-6d-followup-1/C-001."""
    _refusal_message(spark, cell_id, sql)


@pytest.mark.parametrize(
    ("cell_id", "sql"),
    CONSTRUCT_REFUSALS,
    ids=[row[0] for row in CONSTRUCT_REFUSALS],
)
def test_construct_agg_refuses_non_bigint_payload(
    spark: ReparkSession, cell_id: str, sql: str
) -> None:
    """pins: fnp-6d-followup-1/C-002."""
    _refusal_message(spark, cell_id, sql)


def test_construct_agg_refuses_timestamp_without_fixture_cell(
    spark: ReparkSession,
) -> None:
    """pins: fnp-6d-followup-1/C-002."""
    message = refuse_text(
        spark,
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (CAST('2020-01-01 00:00:00' AS TIMESTAMP)) AS t(x)",
    )
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert 'The first parameter requires the "BIGINT" type, however' in message
    assert 'has the type "TIMESTAMP"' in message
    assert "SQLSTATE: 42K09" in message


@pytest.mark.parametrize(
    ("cell_id", "sql"),
    CONSTRUCT_MALFORMED,
    ids=[row[0] for row in CONSTRUCT_MALFORMED],
)
def test_construct_agg_malformed_string_raises_cast_invalid_input(
    spark: ReparkSession, cell_id: str, sql: str
) -> None:
    """pins: fnp-6d-followup-1/C-003."""
    _refusal_message(spark, cell_id, sql)


def test_construct_agg_i64max_string_raises_bitmap_position(
    spark: ReparkSession,
) -> None:
    """pins: fnp-6d-followup-1/C-004."""
    _refusal_message(
        spark,
        "FU2-construct-str-i64max",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES ('9223372036854775807') AS t(x)",
    )


@pytest.mark.parametrize(
    ("cell_id", "sql", "want"),
    CONSTRUCT_ANSWERS,
    ids=[row[0] for row in CONSTRUCT_ANSWERS],
)
def test_construct_agg_answers_numeric_trimmed_and_null(
    spark: ReparkSession, cell_id: str, sql: str, want: int
) -> None:
    """pins: fnp-6d-followup-1/C-004."""
    if cell_id in _CELLS:
        assert _CELLS[cell_id]["rows"] == [[want]]
    assert count_cell(spark, sql) == want


def test_concat_binary_types_string_expected_divergence(spark: ReparkSession) -> None:
    """pins: fnp-6d-followup-1/C-007; expected divergence DOOR-CONVERGE-2."""
    table = spark.sql(
        "SELECT concat(bitmap_construct_agg(0), X'01') b FROM VALUES (1) AS t(x)"
    ).toArrow()
    assert table.schema.field("b").type == pa.string()


@pytest.fixture
def spark_ansi_off() -> Iterator[ReparkSession]:
    session = (
        ReparkSession.builder.appName("fnp-6d-followup-1-ansi-off")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_builder_ansi_off_still_raises_expected_divergence(
    spark_ansi_off: ReparkSession,
) -> None:
    """pins: fnp-6d-followup-1/C-005; expected divergence SET-ANSI-RUNTIME-1."""
    assert _CELLS["FU-construct-abc-nonansi"]["rows"] == [[0]]
    message = refuse_text(
        spark_ansi_off,
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('abc') AS t(x)",
    )
    assert "[CAST_INVALID_INPUT]" in message
    assert "SQLSTATE: 22018" in message


OVERFLOW_CASES: tuple[tuple[str, str], ...] = (
    (
        "FU2-construct-nan-double",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (CAST('NaN' AS DOUBLE)) AS t(x)",
    ),
    (
        "FU2-construct-inf-double",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (CAST('Infinity' AS DOUBLE)) AS t(x)",
    ),
    (
        "FU2-construct-neginf-double",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (CAST('-Infinity' AS DOUBLE)) AS t(x)",
    ),
    (
        "FU2-construct-nan-float",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (CAST('NaN' AS FLOAT)) AS t(x)",
    ),
    (
        "FU2-construct-big-double",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (CAST('1e30' AS DOUBLE)) AS t(x)",
    ),
    (
        "FU2-construct-decimal-big",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (CAST('99999999999999999999' AS DECIMAL(20,0))) AS t(x)",
    ),
)


@pytest.mark.parametrize(
    ("cell_id", "sql"),
    OVERFLOW_CASES,
    ids=[row[0] for row in OVERFLOW_CASES],
)
def test_construct_agg_numeric_overflow_raises_cast_overflow(
    spark: ReparkSession, cell_id: str, sql: str
) -> None:
    """pins: fnp-6d-followup-1/C-004."""
    _refusal_message(spark, cell_id, sql)


def test_construct_agg_overflow_raises_on_grouped_and_window_paths(
    spark: ReparkSession,
) -> None:
    """pins: fnp-6d-followup-1/C-004."""
    grouped = (
        "SELECT g, bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (1, CAST('NaN' AS DOUBLE)) AS t(g, x) GROUP BY g"
    )
    windowed = (
        "SELECT bitmap_count(bitmap_construct_agg(x) OVER ()) c "
        "FROM VALUES (CAST('NaN' AS DOUBLE)) AS t(x)"
    )
    for sql in (grouped, windowed):
        _refusal_message(spark, "FU2-construct-nan-double", sql)
