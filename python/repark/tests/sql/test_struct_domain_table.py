from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions

DATA = Path(__file__).parent / "data"


def _driver() -> Any:
    if str(DATA) not in sys.path:
        sys.path.insert(0, str(DATA))
    return importlib.import_module("struct_domain_driver")


DRIVER = _driver()

CARD = "card"
ORACLE = "oracle defect"
CANNOT_PLAN = "cannot plan"
SEEDS = ([[1, 1], [2, 2]], [[1, {"x": 1}], [2, {"x": 2}]])
DIFFERS = {
    "a_select_bare_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_bare_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_bare_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_bare_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_bare_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_bare_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_bare_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_qualified_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_qualified_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_qualified_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_qualified_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_qualified_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_qualified_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_qualified_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_nested_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_nested_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_nested_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_nested_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_nested_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_nested_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_nested_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_paren_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_paren_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_paren_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_paren_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_paren_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_paren_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_paren_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_delete_subscript_int_int_diff": (
        CARD,
        "card C1: a DELETE WHERE struct field chain refuses with the extracted text",
    ),
    "a_delete_subscript_int_int_same": (
        CARD,
        "card C1: a DELETE WHERE struct field chain refuses with the extracted text",
    ),
    "a_delete_subscript_int_bigint_same": (
        CARD,
        "card C1: a DELETE WHERE struct field chain refuses with the extracted text",
    ),
    "a_select_subscript_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_delete_subscript_int_double_same": (
        CARD,
        "card C1: a DELETE WHERE struct field chain refuses with the extracted text",
    ),
    "a_select_subscript_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_delete_subscript_int_dec12_same": (
        CARD,
        "card C1: a DELETE WHERE struct field chain refuses with the extracted text",
    ),
    "a_select_subscript_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_subscript_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_subscript_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_subscript_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_subscript_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_dot_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_dot_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_dot_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_dot_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_dot_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_dot_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_dot_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_literal_int_int_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_literal_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_literal_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_literal_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_literal_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_literal_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_literal_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_literal_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_expression_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_expression_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_expression_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_expression_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_expression_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_expression_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_expression_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_alias_int_bigint_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_alias_int_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_alias_int_dec12_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_alias_int_string_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_alias_int_boolean_diff": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_alias_int_boolean_same": (
        CANNOT_PLAN,
        "DataFusion's SQL planner types the comparison before the analyzer",
    ),
    "a_select_alias_int_boolean_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_bare_float_double_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_bare_date_timestamp_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "a_select_bare_dec5_dec7_dup": (
        CANNOT_PLAN,
        "DataFusion asserts on duplicate struct field names while planning",
    ),
    "b_union_all_k4_string": (CARD, "card: UNION widening of a STRING field to a number"),
    "c_insert_union_all_vl_int_named": (
        CANNOT_PLAN,
        "the Iceberg provider cannot cast a UNION input into a wider struct",
    ),
    "c_insert_union_all_vl_int_alias_x": (
        CANNOT_PLAN,
        "the Iceberg provider cannot cast a UNION input into a wider struct",
    ),
    "c_insert_union_all_vi_string_x": (CARD, "card: UNION widening of a STRING field to a number"),
    "c_insert_union_all_vl_string_x": (CARD, "card: UNION widening of a STRING field to a number"),
    "c_insert_union_all_vi_arity": (
        CANNOT_PLAN,
        "DataFusion refuses a UNION of different struct arity while planning",
    ),
    "c_insert_union_all_vl_arity": (
        CANNOT_PLAN,
        "DataFusion refuses a UNION of different struct arity while planning",
    ),
    "c_insert_union_all_vl_literal": (
        CANNOT_PLAN,
        "the Iceberg provider cannot cast a UNION input into a wider struct",
    ),
    "c_insert_union_all_vl_int_y": (
        CANNOT_PLAN,
        "the Iceberg provider cannot cast a UNION input into a wider struct",
    ),
    "c_df_insert_into_vi_null_y": (
        CANNOT_PLAN,
        "the DataFrame door cannot parse a struct cast type string",
    ),
    "c_df_append_vi_null_y": (
        CANNOT_PLAN,
        "the DataFrame door cannot parse a struct cast type string",
    ),
    "c_insert_union_all_vl_null_y": (
        CANNOT_PLAN,
        "the Iceberg provider cannot cast a UNION input into a wider struct",
    ),
    "c_df_insert_into_vl_null_y": (
        CANNOT_PLAN,
        "the DataFrame door cannot parse a struct cast type string",
    ),
    "c_df_append_vl_null_y": (
        CANNOT_PLAN,
        "the DataFrame door cannot parse a struct cast type string",
    ),
    "c_insert_union_all_vl_column_x": (
        CANNOT_PLAN,
        "the Iceberg provider cannot cast a UNION input into a wider struct",
    ),
    "c_insert_union_all_vl_column_y": (
        CANNOT_PLAN,
        "the Iceberg provider cannot cast a UNION input into a wider struct",
    ),
    "d_union_all_struct_in_struct_3": (CARD, "card: UNION widening of a STRING field to a number"),
    "d_delete_struct_in_array_10": (
        ORACLE,
        "oracle defect: Spark fails on an Iceberg struct literal pushdown",
    ),
    "d_delete_literal_vs_column_16": (
        ORACLE,
        "oracle defect: Spark fails on an Iceberg struct literal pushdown",
    ),
    "d_union_all_two_columns_22": (CARD, "card: UNION widening of a STRING field to a number"),
}
PENDING_RULING = {
    "d_delete_typed_null_24": (
        "DELETE WHERE st = CAST(NULL AS STRUCT<y: INT>) deletes every row, on main too"
    ),
}
CELL_ROWS = [json.loads(line) for line in DRIVER.CELLS.read_text().splitlines()]


SEATS = sorted({row["seat"] for row in CELL_ROWS})


@pytest.fixture
def grid(tmp_path: Path) -> Iterator[Any]:
    session = (
        ReparkSession.builder.appName("struct-domain")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path / "spark-warehouse"))
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(tmp_path / "hd"))
        .getOrCreate()
    )
    session.conf.set("spark.sql.ansi.enabled", "true")
    yield DRIVER.Grid(session, functions, "hd.sd")
    session.stop()


def test_the_table_holds_every_cell_and_every_listed_key() -> None:
    keys = {row["key"] for row in CELL_ROWS}
    assert len(CELL_ROWS) == 971
    assert len(SEATS) == 21
    assert set(DIFFERS) <= keys
    assert set(PENDING_RULING) <= keys
    assert {reason for reason, _ in DIFFERS.values()} <= {CARD, ORACLE, CANNOT_PLAN}


def _mismatch(grid: Any, cell: dict[str, Any]) -> str | None:
    result = grid.run(cell)
    got = (result["kind"], result["answer"], result["readback"])
    want = (cell["spark_kind"], cell["spark"], cell["readback"])
    listed = DIFFERS.get(cell["key"])
    if listed is None:
        return None if got == want else f"{cell['key']}: {got!r} != {want!r}"
    if got == want:
        return f"{cell['key']} now matches Spark; drop it from DIFFERS"
    nothing_written = result["readback"] is None or result["readback"] in SEEDS
    if listed[0] == CANNOT_PLAN and (result["kind"] != "error" or not nothing_written):
        return f"{cell['key']} must refuse and write nothing: {got!r}"
    return None


@pytest.mark.parametrize("seat", SEATS)
def test_every_cell_of_the_seat_answers_as_spark_does(grid: Any, seat: str) -> None:
    cells = [row for row in CELL_ROWS if row["seat"] == seat and row["key"] not in PENDING_RULING]
    mismatches = [found for cell in cells if (found := _mismatch(grid, cell)) is not None]
    assert mismatches == []
