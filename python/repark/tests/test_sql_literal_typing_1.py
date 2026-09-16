"""BL-20 pins: Spark integral literal typing and binary-arithmetic promotion.

Live PySpark 4.1.2 (local[2], ANSI on unless a cell carries ``conf``) is the
oracle, recorded 2026-09-16 in ``sql_literal_typing_1_spark_oracle.json``
(batch ``sc18-bl20-literal-typing``). Every pin runs the fixture's own SQL or
Python-door expression for a named cell id and asserts the recorded value plus
the Arrow type from ``to_arrow().schema`` (never ``df.schema``, whose
tinyint/smallint display is LOGICAL-WIDTH-1, owned by run 18b). Error pins
assert Spark's error class and message shape.

pins: sql-literal-typing-1/C-001, C-002, C-003, C-004, C-005, C-006
pins: sql-literal-typing-1/L-001, L-002, L-003
"""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark.spark import types as T  # noqa: N812

FIXTURE_PATH = Path(__file__).parent / "sql_literal_typing_1_spark_oracle.json"

PARSER_RESIDUES = frozenset({"LIT-SQL-26", "LIT-SQL-38"})


def _fixture() -> dict[str, Any]:
    """The verbatim BL-20 oracle recording."""
    return json.loads(FIXTURE_PATH.read_text())


def _cells() -> dict[str, dict[str, Any]]:
    """Oracle cells keyed by cell id, parser residues excluded."""
    return {cell["id"]: cell for cell in _fixture()["cells"] if cell["id"] not in PARSER_RESIDUES}


def _spark() -> ReparkSession:
    """Default session (ANSI on)."""
    return ReparkSession.builder.appName("sql-literal-typing-1").getOrCreate()


def _spark_legacy() -> ReparkSession:
    """Session with ``spark.sql.ansi.enabled=false``."""
    return ReparkSession.builder.config("spark.sql.ansi.enabled", "false").getOrCreate()


def _session_for(cell: dict[str, Any]) -> ReparkSession:
    """The ANSI-on or ANSI-off session the cell's ``conf`` selects."""
    if cell.get("conf", {}).get("spark.sql.ansi.enabled") == "false":
        return _spark_legacy()
    return _spark()


def _normalize(value: Any) -> Any:
    """Oracle-comparable form of one collected value."""
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return repr(value)
    if isinstance(value, Decimal):
        return f"Decimal('{value}')"
    if isinstance(value, list):
        return repr(value)
    return value


def _normalized_rows(table: pa.Table) -> list[list[Any]]:
    """Collected rows with decimals and arrays in oracle repr form."""
    names = table.schema.names
    return [[_normalize(row[name]) for name in names] for row in table.to_pylist()]


def _run_sql(cell: dict[str, Any]) -> pa.Table:
    """Collect the cell's SQL expression on its ANSI session as Arrow."""
    return _session_for(cell).sql(cell["expr"]).to_arrow()


def _run_py(cell: dict[str, Any]) -> pa.Table:
    """Collect the cell's Python-door expression on its ANSI session as Arrow."""
    session = _session_for(cell)
    frame = eval(cell["expr"], {"spark": session, "F": F, "T": T})
    return frame.to_arrow()


def _check_ok_cell(cell_id: str, field: str, expected_type: pa.DataType) -> None:
    """One ok cell: recorded rows plus the recorded value's Arrow type.

    pins: sql-literal-typing-1/C-001, C-002, C-003, C-004, C-006
    """
    cell = _cells()[cell_id]
    assert cell["outcome"] == "ok", f"{cell_id} is an error cell in the oracle"
    table = _run_sql(cell) if cell["door"] == "sql" else _run_py(cell)
    assert table.schema.field(field).type == expected_type, (
        f"{cell_id} Arrow type {table.schema.field(field).type}"
    )
    assert _normalized_rows(table) == cell["rows"], f"{cell_id} rows"


def _check_error_cell(cell_id: str, error_class: str) -> str:
    """One error cell: Spark's class must appear in the refusal message.

    pins: sql-literal-typing-1/C-001, C-002, C-005, C-006
    """
    cell = _cells()[cell_id]
    assert cell["outcome"] == "error", f"{cell_id} is an ok cell in the oracle"
    with pytest.raises(Exception, match=error_class) as excinfo:
        if cell["door"] == "sql":
            _run_sql(cell)
        else:
            _run_py(cell)
    return str(excinfo.value)


OK_SQL_CASES: list[tuple[str, str, pa.DataType]] = [
    ("LIT-SQL-00", "v", pa.int32()),
    ("LIT-SQL-01", "v", pa.int32()),
    ("LIT-SQL-02", "v", pa.int64()),
    ("LIT-SQL-03", "v", pa.int32()),
    ("LIT-SQL-04", "v", pa.int64()),
    ("LIT-SQL-05", "v", pa.int64()),
    ("LIT-SQL-06", "v", pa.decimal128(19, 0)),
    ("LIT-SQL-07", "v", pa.int64()),
    ("LIT-SQL-08", "v", pa.decimal128(38, 0)),
    ("LIT-SQL-10", "v", pa.int8()),
    ("LIT-SQL-11", "v", pa.int16()),
    ("LIT-SQL-12", "v", pa.int64()),
    ("LIT-SQL-13", "v", pa.int32()),
    ("LIT-SQL-14", "v", pa.int32()),
    ("LIT-SQL-15", "v", pa.int32()),
    ("LIT-SQL-16", "v", pa.int32()),
    ("LIT-SQL-17", "v", pa.int8()),
    ("LIT-SQL-18", "v", pa.int16()),
    ("LIT-SQL-19", "v", pa.int32()),
    ("LIT-SQL-20", "v", pa.int64()),
    ("LIT-SQL-21", "v", pa.int32()),
    ("LIT-SQL-23", "v", pa.int64()),
    ("LIT-SQL-24", "v", pa.decimal128(3, 1)),
    ("LIT-SQL-25", "v", pa.float64()),
    ("LIT-SQL-27", "v", pa.int32()),
    ("LIT-SQL-28", "v", pa.int8()),
    ("LIT-SQL-29", "v", pa.int8()),
    ("LIT-SQL-30", "v", pa.int32()),
    ("LIT-SQL-31", "v", pa.int32()),
    ("LIT-SQL-32", "v", pa.list_(pa.int32())),
    ("LIT-SQL-33", "v", pa.int32()),
    ("LIT-SQL-34", "v", pa.bool_()),
    ("LIT-SQL-35", "v", pa.decimal128(20, 0)),
    ("LIT-SQL-36", "v", pa.decimal128(6, 2)),
    ("LIT-SQL-37", "v", pa.int16()),
    ("LIT-SQL-39", "v", pa.int8()),
    ("LIT-SQL-40", "v", pa.int64()),
    ("LIT-SQL-41", "v", pa.int32()),
    ("LIT-SQL-42", "v", pa.int32()),
    ("LIT-SQL-43", "v", pa.int16()),
    ("LIT-SQL-44", "h", pa.string()),
    ("LIT-SQL-45", "h", pa.string()),
    ("LIT-SQL-46", "h", pa.string()),
    ("LIT-SQL-47", "h", pa.string()),
    ("LIT-SQL-48", "h", pa.string()),
    ("LIT-SQL-49", "h", pa.string()),
    ("LIT-SQL-50", "h", pa.string()),
    ("LIT-SQL-51", "h", pa.string()),
    ("LIT2-SQL-00", "v", pa.date32()),
    ("LIT2-SQL-01", "v", pa.date32()),
    ("LIT2-SQL-02", "v", pa.int64()),
    ("LIT2-SQL-03", "v", pa.int64()),
    ("LIT2-SQL-04", "v", pa.int32()),
    ("LIT2-SQL-06", "v", pa.int32()),
]

OK_PY_CASES: list[tuple[str, str, pa.DataType]] = [
    ("LIT-PY-00", "v", pa.int32()),
    ("LIT-PY-01", "v", pa.int64()),
    ("LIT-PY-02", "v", pa.int32()),
    ("LIT-PY-03", "v", pa.int8()),
    ("LIT-PY-04", "v", pa.int16()),
    ("LIT-PY-05", "h", pa.string()),
    ("LIT-PY-06", "v", pa.int32()),
    ("LIT-PY-08", "v", pa.int32()),
    ("LIT-PY-09", "v", pa.int64()),
    ("LIT2-PY-00", "v", pa.int32()),
    ("LIT2-PY-01", "v", pa.int32()),
    ("LIT2-PY-02", "v", pa.list_(pa.int32())),
    ("LIT2-PY-03", "v", pa.date32()),
    ("LIT2-PY-04", "v", pa.int32()),
]


@pytest.mark.parametrize(("cell_id", "field", "expected_type"), OK_SQL_CASES)
def test_sql_door_ok_cells(cell_id: str, field: str, expected_type: pa.DataType) -> None:
    """SQL-door ok cells answer the recorded rows with Spark's Arrow type.

    pins: sql-literal-typing-1/C-001, C-002, C-003, C-004
    """
    _check_ok_cell(cell_id, field, expected_type)


@pytest.mark.parametrize(("cell_id", "field", "expected_type"), OK_PY_CASES)
def test_python_door_ok_cells(cell_id: str, field: str, expected_type: pa.DataType) -> None:
    """Python-door ok cells stay as recorded (C-006, no regression).

    pins: sql-literal-typing-1/C-006
    """
    _check_ok_cell(cell_id, field, expected_type)


def test_decimal_precision_overflow_refuses() -> None:
    """39-digit literals refuse with DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION.

    pins: sql-literal-typing-1/C-001
    """
    message = _check_error_cell("LIT-SQL-09", "DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION")
    assert "exceeds max precision 38" in message
    assert "SQLSTATE: 22003" in message


def test_int_overflow_raises_arithmetic_overflow() -> None:
    """``2147483647 + 1`` raises ARITHMETIC_OVERFLOW with and without typeof.

    pins: sql-literal-typing-1/C-002
    """
    for cell_id in ("LIT-SQL-22", "LIT-SQL-53"):
        message = _check_error_cell(cell_id, "ARITHMETIC_OVERFLOW")
        assert "try_add" in message


def test_lit2_overflow_cells_raise_arithmetic_overflow() -> None:
    """``abs(-2147483648)`` and ``CAST(2147483647 AS INT) + 1`` raise.

    pins: sql-literal-typing-1/L-003
    """
    for cell_id in ("LIT2-SQL-05", "LIT2-SQL-07"):
        _check_error_cell(cell_id, "ARITHMETIC_OVERFLOW")


def test_fexpr_mixed_width_matches_sql_door() -> None:
    """The critic's L-001 table, re-pinned on the ``F.expr`` door.

    ``spark.range(1).select(F.expr(text))`` must answer the same rows with
    the same Arrow types as ``spark.sql(f"SELECT {text} AS v")``.

    pins: sql-literal-typing-1/L-001
    """
    session = _spark()
    for text in (
        "1 + CAST(1 AS TINYINT)",
        "CAST(1 AS SMALLINT) * 2",
        "coalesce(1, CAST(1 AS TINYINT))",
        "array(1, CAST(1 AS TINYINT))",
        "greatest(1, CAST(1 AS TINYINT))",
        "pmod(7, CAST(2 AS TINYINT))",
        "7 % CAST(2 AS TINYINT)",
    ):
        expected = session.sql(f"SELECT {text} AS v").to_arrow()
        observed = session.range(1).select(F.expr(text).alias("v")).to_arrow()
        assert observed.schema.field("v").type == expected.schema.field("v").type, text
        assert observed.column("v").to_pylist() == expected.column("v").to_pylist(), text


def test_hof_parenthesized_int_min_stays_bigint_on_both_doors() -> None:
    """A parenthesized ``-(2147483648)`` stays bigint inside HOFs too.

    ``HigherOrderPreparation`` used to fold it through the shared
    provisional-integer helper while the top-level door answered bigint
    (LIT2-SQL-03). Both doors must answer the Spark-true widths here.

    pins: sql-literal-typing-1/V-001
    """
    session = _spark()
    cases = (
        ("transform(array(-(2147483648)), x -> x)", pa.list_(pa.int64())),
        ("array(-(2147483648))", pa.list_(pa.int64())),
        ("filter(array(-(2147483648), 1), x -> x < 0)", pa.list_(pa.int64())),
        (
            "transform_keys(map(-(2147483648), 1), (k, v) -> k)",
            pa.map_(pa.int64(), pa.int32()),
        ),
    )
    for text, expected_type in cases:
        expected = session.sql(f"SELECT {text} AS v").to_arrow()
        observed = session.range(1).select(F.expr(text).alias("v")).to_arrow()
        assert expected.schema.field("v").type == expected_type, text
        assert observed.schema.field("v").type == expected_type, text
        assert observed.column("v").to_pylist() == expected.column("v").to_pylist(), text


def test_tinyint_overflow_wrap_is_declared() -> None:
    """``CAST(127 AS TINYINT) + CAST(1 AS TINYINT)`` wraps to -128, declared.

    Spark raises BINARY_ARITHMETIC_OVERFLOW under ANSI; repark has no checked
    Int8/Int16 kernel yet (residue BL-20-OVF). The pin holds today's wrap so
    the kernel round flips it red on arrival.

    pins: sql-literal-typing-1/C-005
    """
    table = _run_sql(_cells()["LIT-SQL-52"])
    assert table.schema.field("v").type == pa.int8()
    assert table.column("v").to_pylist() == [-128]


def test_python_huge_literal_refuses() -> None:
    """``F.lit(2**63)`` refuses on the Python door, as it does on main.

    pins: sql-literal-typing-1/C-006
    """
    _check_error_cell("LIT-PY-07", "too large to convert")


def test_hex_widths_agree_across_doors() -> None:
    """LIT-SQL-44 and LIT-PY-05 agree: the BL-20 door-disagreement pin.

    pins: sql-literal-typing-1/C-004
    """
    cells = _cells()
    sql_hex = _run_sql(cells["LIT-SQL-44"]).column("h").to_pylist()
    py_hex = _run_py(cells["LIT-PY-05"]).column("h").to_pylist()
    assert sql_hex == py_hex == ["00000002"]


def test_bare_literals_are_non_nullable() -> None:
    """Unsuffixed literal projections are non-nullable, as in Spark.

    pins: sql-literal-typing-1/C-001
    """
    for cell_id in (
        "LIT-SQL-00",
        "LIT-SQL-02",
        "LIT-SQL-06",
        "LIT-SQL-08",
        "LIT-SQL-13",
        "LIT-SQL-15",
    ):
        table = _run_sql(_cells()[cell_id])
        assert table.schema.field("v").nullable is False, cell_id
