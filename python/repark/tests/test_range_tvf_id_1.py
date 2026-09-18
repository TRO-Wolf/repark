"""RANGE-TVF-ID-1 — the range() table function names its column id like Spark.

pins: range-tvf-id-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark.errors import AnalysisException

_HERE = Path(__file__).resolve().parent
_ORACLE: dict[str, Any] = json.loads(
    (_HERE / "range_tvf_id_1" / "range_tvf_id_1_spark_oracle.json").read_text(encoding="utf-8")
)
_CELLS: dict[str, Any] = _ORACLE["cells"]

_SQL_VALUE_CELLS = [
    "range_n",
    "range_a_b",
    "range_a_b_step",
    "range_a_b_step_parts",
    "range_negative_step",
    "range_empty",
    "select_id",
    "alias_col",
    "alias_rename",
    "range_string_arg",
]
_SQL_REFUSAL_CELLS = ["select_value", "range_zero_step"]
_DATAFRAME_CELLS = ["spark_range_n", "spark_range_a_b", "spark_range_a_b_step"]
_DOORS = ["facade", "native"]
_SPARK_TYPE_NAMES = {"int64": "bigint"}


@pytest.fixture
def spark() -> ReparkSession:
    """A default session for the facade SQL and DataFrame doors."""
    return ReparkSession.builder.appName("pytest-range-tvf-id-1").getOrCreate()


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


@pytest.mark.parametrize("cell_id", _SQL_VALUE_CELLS)
@pytest.mark.parametrize("door", _DOORS)
def test_sql_range_cell_answers_recorded_schema_and_rows(
    spark: ReparkSession, cell_id: str, door: str
) -> None:
    """One recorded range() SQL cell answers Spark's schema, nullability, and rows."""
    expected = _CELLS[cell_id]
    table = run_on_door(spark, door, expected["sql"])
    assert spark_schema_string(table) == expected["schema"]
    assert [field.nullable for field in table.schema] == expected["nullable"]
    assert rows_as_lists(table) == expected["rows"]


@pytest.mark.parametrize("door", _DOORS)
def test_sql_sum_cell_answers_rows_with_qualified_display_name(
    spark: ReparkSession, door: str
) -> None:
    """sum(id) answers 45 nullable; the name keeps the systemic range() qualifier leak."""
    expected = _CELLS["sum_id"]
    table = run_on_door(spark, door, expected["sql"])
    assert rows_as_lists(table) == expected["rows"]
    assert [field.nullable for field in table.schema] == expected["nullable"]
    assert [str(field.type) for field in table.schema] == ["int64"]
    assert [field.name for field in table.schema] == ["sum(range().id)"]


@pytest.mark.parametrize("cell_id", _SQL_REFUSAL_CELLS)
@pytest.mark.parametrize("door", _DOORS)
def test_sql_range_refusal_matches_spark_class(
    spark: ReparkSession, cell_id: str, door: str
) -> None:
    """Refusals carry AnalysisException; without a Spark token the pin is the class only."""
    expected = _CELLS[cell_id]
    assert expected["error_class"] == "AnalysisException"
    with pytest.raises(AnalysisException):
        run_on_door(spark, door, expected["sql"])


@pytest.mark.parametrize("cell_id", _DATAFRAME_CELLS)
def test_spark_range_door_answers_recorded_schema_and_rows(
    spark: ReparkSession, cell_id: str
) -> None:
    """One recorded spark.range call answers Spark's schema, nullability, and rows."""
    expected = _CELLS[cell_id]
    args = ast.literal_eval(expected["call"].split("spark.range", 1)[1])
    table = spark.range(*args).to_arrow()
    assert spark_schema_string(table) == expected["schema"]
    assert [field.nullable for field in table.schema] == expected["nullable"]
    assert rows_as_lists(table) == expected["rows"]
