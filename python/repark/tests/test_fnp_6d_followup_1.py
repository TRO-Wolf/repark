"""SQL-door pins for the FNP-6D followup bitmap signature fix."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession

_FIXTURE_PATH = Path(__file__).with_name("fnp_6d_followup_1_spark_oracle.json")
_FIXTURE: dict[str, Any] = json.loads(_FIXTURE_PATH.read_text())
_CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in _FIXTURE["cells"]}

OR_AND_REFUSALS: tuple[tuple[str, str, str, str], ...] = (
    (
        "FU-or-int",
        "SELECT bitmap_or_agg(x) b FROM VALUES (CAST(1 AS INT)), (CAST(2 AS INT)) AS t(x)",
        "bitmap_or_agg",
        "INT",
    ),
    (
        "FU-and-int",
        "SELECT bitmap_and_agg(x) b FROM VALUES (CAST(1 AS INT)), (CAST(2 AS INT)) AS t(x)",
        "bitmap_and_agg",
        "INT",
    ),
    (
        "FU-or-bigint-bitpos",
        "SELECT bitmap_or_agg(bitmap_bit_position(x)) b FROM VALUES (1), (2) AS t(x)",
        "bitmap_or_agg",
        "BIGINT",
    ),
    (
        "FU-or-float",
        "SELECT bitmap_or_agg(x) b FROM VALUES (CAST(1.0 AS FLOAT)) AS t(x)",
        "bitmap_or_agg",
        "FLOAT",
    ),
    (
        "FU-or-double",
        "SELECT bitmap_or_agg(x) b FROM VALUES (CAST(1.0 AS DOUBLE)) AS t(x)",
        "bitmap_or_agg",
        "DOUBLE",
    ),
    (
        "FU-or-bool",
        "SELECT bitmap_or_agg(x) b FROM VALUES (true) AS t(x)",
        "bitmap_or_agg",
        "BOOLEAN",
    ),
    (
        "FU-and-bool",
        "SELECT bitmap_and_agg(x) b FROM VALUES (true) AS t(x)",
        "bitmap_and_agg",
        "BOOLEAN",
    ),
    (
        "FU-or-string",
        "SELECT bitmap_or_agg(x) b FROM VALUES ('abc') AS t(x)",
        "bitmap_or_agg",
        "STRING",
    ),
    (
        "FU-and-string",
        "SELECT bitmap_and_agg(x) b FROM VALUES ('abc') AS t(x)",
        "bitmap_and_agg",
        "STRING",
    ),
    (
        "FU-or-decimal",
        "SELECT bitmap_or_agg(x) b FROM VALUES (CAST(1.5 AS DECIMAL(2,1))) AS t(x)",
        "bitmap_or_agg",
        "DECIMAL(2,1)",
    ),
    (
        "FU-or-date",
        "SELECT bitmap_or_agg(x) b FROM VALUES (DATE'2020-01-01') AS t(x)",
        "bitmap_or_agg",
        "DATE",
    ),
    (
        "FU-or-null-literal",
        "SELECT bitmap_or_agg(NULL) b FROM VALUES (1) AS t(x)",
        "bitmap_or_agg",
        "VOID",
    ),
)

CONSTRUCT_REFUSALS: tuple[tuple[str, str, str], ...] = (
    (
        "FU-construct-bool",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (true) AS t(x)",
        "BOOLEAN",
    ),
    (
        "FU-construct-binary",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (X'01') AS t(x)",
        "BINARY",
    ),
    (
        "FU-construct-date",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES (DATE'2020-01-01') AS t(x)",
        "DATE",
    ),
    (
        "construct-timestamp",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c "
        "FROM VALUES (CAST('2020-01-01 00:00:00' AS TIMESTAMP)) AS t(x)",
        "TIMESTAMP",
    ),
)

CONSTRUCT_MALFORMED: tuple[tuple[str, str, str], ...] = (
    (
        "FU-construct-abc",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('abc') AS t(x)",
        "abc",
    ),
    (
        "FU-construct-empty-str",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('') AS t(x)",
        "",
    ),
    (
        "FU-construct-1.5-str",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('1.5') AS t(x)",
        "1.5",
    ),
    (
        "FU-construct-str-mixed",
        "SELECT bitmap_count(bitmap_construct_agg(x)) c FROM VALUES ('1'), ('abc') AS t(x)",
        "abc",
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
    ("cell_id", "sql", "function", "spark_type"),
    OR_AND_REFUSALS,
    ids=[row[0] for row in OR_AND_REFUSALS],
)
def test_or_and_agg_refuses_non_binary_payload(
    spark: ReparkSession, cell_id: str, sql: str, function: str, spark_type: str
) -> None:
    """pins: fnp-6d-followup-1/C-001."""
    assert _CELLS[cell_id]["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    message = refuse_text(spark, sql)
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert function in message
    assert 'The first parameter requires the "BINARY" type, however' in message
    assert f'has the type "{spark_type}"' in message
    assert "SQLSTATE: 42K09" in message


@pytest.mark.parametrize(
    ("cell_id", "sql", "spark_type"),
    CONSTRUCT_REFUSALS,
    ids=[row[0] for row in CONSTRUCT_REFUSALS],
)
def test_construct_agg_refuses_non_bigint_payload(
    spark: ReparkSession, cell_id: str, sql: str, spark_type: str
) -> None:
    """pins: fnp-6d-followup-1/C-002."""
    if cell_id in _CELLS:
        assert _CELLS[cell_id]["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
    message = refuse_text(spark, sql)
    assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in message
    assert "bitmap_construct_agg" in message
    assert 'The first parameter requires the "BIGINT" type, however' in message
    assert f'has the type "{spark_type}"' in message
    assert "SQLSTATE: 42K09" in message


@pytest.mark.parametrize(
    ("cell_id", "sql", "value"),
    CONSTRUCT_MALFORMED,
    ids=[row[0] for row in CONSTRUCT_MALFORMED],
)
def test_construct_agg_malformed_string_raises_cast_invalid_input(
    spark: ReparkSession, cell_id: str, sql: str, value: str
) -> None:
    """pins: fnp-6d-followup-1/C-003."""
    assert _CELLS[cell_id]["condition"] == "CAST_INVALID_INPUT"
    message = refuse_text(spark, sql)
    assert "[CAST_INVALID_INPUT]" in message
    assert f'The value \'{value}\' of the type "STRING" cannot be cast to "BIGINT"' in message
    assert "SQLSTATE: 22018" in message


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
