"""RANGE-TVF-ID-2 - range() argument edge cases answer Spark 4.1.2.

pins: range-tvf-id-2/C-001, C-002, C-003, C-004, C-005, C-006
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark.errors import AnalysisException, IllegalArgumentException, PySparkException

_HERE = Path(__file__).resolve().parent
_ORACLE: dict[str, Any] = json.loads(
    (_HERE / "range_tvf_id_1" / "range_tvf_id_2_spark_oracle.json").read_text(encoding="utf-8")
)
_CELLS: dict[str, Any] = _ORACLE["cells"]

_SQL_ANSWER_CELLS = [
    "range_cast_int",
    "range_tinyint",
    "range_smallint",
    "range_decimal",
    "range_double",
    "range_expr",
    "range_neg",
    "range_overflow_up",
    "range_overflow_down",
    "parts_zero_empty",
]
_OVERFLOW_CELLS = {"range_overflow_up", "range_overflow_down"}
_OVERFLOW_BOUND = 8
_SUFFIX_LITERAL_CELLS = {"range_tinyint", "range_smallint", "range_double"}
_REFUSAL_CONTRACT: dict[str, tuple[str, Any, str]] = {
    "range_null": ("AnalysisException", AnalysisException, "UNEXPECTED_INPUT_TYPE"),
    "range_null_end": ("AnalysisException", AnalysisException, "UNEXPECTED_INPUT_TYPE"),
    "range_cast_null_bigint": ("AnalysisException", AnalysisException, "UNEXPECTED_INPUT_TYPE"),
    "parts_null": ("AnalysisException", AnalysisException, "UNEXPECTED_INPUT_TYPE"),
    "parts_zero": (
        "IllegalArgumentException",
        IllegalArgumentException,
        "Positive number of partitions required",
    ),
    "parts_negative": (
        "IllegalArgumentException",
        IllegalArgumentException,
        "Positive number of partitions required",
    ),
    "parts_string": ("NumberFormatException", PySparkException, "CAST_INVALID_INPUT"),
    "string_bogus": ("NumberFormatException", PySparkException, "CAST_INVALID_INPUT"),
}
_DOORS = ["facade", "native"]
_SPARK_TYPE_NAMES = {"int64": "bigint"}


@pytest.fixture
def spark() -> ReparkSession:
    """A default session for the facade SQL and DataFrame doors."""
    return ReparkSession.builder.appName("pytest-range-tvf-id-2").getOrCreate()


def spark_schema_string(table: Any) -> str:
    """Render an Arrow table schema in Spark simpleString struct form."""
    parts = ",".join(f"{field.name}:{_SPARK_TYPE_NAMES[str(field.type)]}" for field in table.schema)
    return f"struct<{parts}>"


def rows_as_lists(table: Any) -> list[list[Any]]:
    """Read an Arrow table as plain row lists in schema order."""
    names = table.column_names
    return [[row[name] for name in names] for row in table.to_pylist()]


def run_on_door(spark: ReparkSession, door: str, query: str) -> Any:
    """Run one query on the named SQL door and return its Arrow table."""
    if door == "native":
        return repark.sql(query).to_arrow()
    return spark.sql(query).to_arrow()


def bounded_query(cell_id: str, query: str) -> str:
    """Wrap an overflow-cell query in a LIMIT so a regression cannot hang CI."""
    if cell_id in _OVERFLOW_CELLS:
        return f"SELECT * FROM ({query}) LIMIT {_OVERFLOW_BOUND}"
    return query


def doors_for_cell(cell_id: str) -> list[str]:
    """Both doors run every cell except Spark-suffix literals, which parse facade-only."""
    if cell_id in _SUFFIX_LITERAL_CELLS:
        return ["facade"]
    return _DOORS


_ANSWER_CASES = [
    (cell_id, door) for cell_id in _SQL_ANSWER_CELLS for door in doors_for_cell(cell_id)
]


@pytest.mark.parametrize(("cell_id", "door"), _ANSWER_CASES)
def test_sql_range_id2_answer_cell_matches_recorded_schema_and_rows(
    spark: ReparkSession, cell_id: str, door: str
) -> None:
    """One recorded edge-case cell answers Spark's schema and rows."""
    expected = _CELLS[cell_id]
    assert "rows" in expected
    table = run_on_door(spark, door, bounded_query(cell_id, expected["sql"]))
    assert spark_schema_string(table) == expected["schema"]
    assert rows_as_lists(table) == expected["rows"]
    if expected["schema"] == "struct<id:bigint>":
        assert [field.nullable for field in table.schema] == [False]


@pytest.mark.parametrize("door", _DOORS)
def test_sql_range_id2_near_max_count_answers_rows_with_door_display_name(
    spark: ReparkSession, door: str
) -> None:
    """count(*) near int64 max answers 7; the name keeps the systemic count(*) form."""
    expected = _CELLS["range_near_max_step1"]
    table = run_on_door(spark, door, expected["sql"])
    assert rows_as_lists(table) == expected["rows"]
    assert [str(field.type) for field in table.schema] == ["int64"]
    assert [field.name for field in table.schema] == ["count(*)"]


@pytest.mark.parametrize("cell_id", list(_REFUSAL_CONTRACT))
@pytest.mark.parametrize("door", _DOORS)
def test_sql_range_id2_refusal_matches_mapped_class_and_token(
    spark: ReparkSession, cell_id: str, door: str
) -> None:
    """Refusals raise RePark's mapped class carrying Spark's error token."""
    expected = _CELLS[cell_id]
    spark_class, repark_class, token = _REFUSAL_CONTRACT[cell_id]
    assert expected["error_class"] == spark_class
    with pytest.raises(repark_class, match=token):
        run_on_door(spark, door, expected["sql"])
