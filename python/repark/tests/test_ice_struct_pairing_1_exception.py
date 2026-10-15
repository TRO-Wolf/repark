from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _record_ice_struct_pairing_1 as base
import _record_ice_struct_pairing_1_exception as recorder
import pytest
from test_ice_struct_pairing_1_round3 import answers_as_spark, cell_id, seed_only

from repark.spark.session import _reset_active_session_for_tests

HERE = Path(__file__).resolve().parent
ORACLE = json.loads((HERE / "ice_struct_pairing_1_spark_oracle.json").read_text("utf-8"))
MAIN = json.loads((HERE / "ice_struct_pairing_1_main.json").read_text("utf-8"))
UPDATE_REFUSAL = (
    "UPDATE with SET column = (SELECT ...) is not supported on this statement: RePark cannot "
    "write the subquery value here, so it refuses before any write."
)
SEEDED = [[0, 10, {"a": 10, "b": 20}], [3, 30, {"a": 30, "b": 40}]]
ROWS_REFUSED_ELSEWHERE = (
    "nocol|leaf_types_differ",
    "overwrite_nocol|names_differ",
    "select_union|names_differ",
)
ROWS_CELLS = [("x_rows", key) for key in sorted(ORACLE["x_rows"])]
ROWS_SPARK_CELLS = [cell for cell in ROWS_CELLS if cell[1] not in ROWS_REFUSED_ELSEWHERE]
ROWS_ELSEWHERE_CELLS = [cell for cell in ROWS_CELLS if cell[1] in ROWS_REFUSED_ELSEWHERE]
UPDATE_CELLS = [("x_update", recorder.update_key(*key)) for key in recorder.update_keys()]


def update_parts(key: str) -> list[str]:
    return key.split("|")


def identity_shape(key: str) -> bool:
    door, _, where, _ = update_parts(key)
    return door == "spark" and where in recorder.IDENTITY_PREDICATES


UPDATE_STORED_CELLS = [cell for cell in UPDATE_CELLS if identity_shape(cell[1])]
UPDATE_REFUSED_INT_CELLS = [
    cell
    for cell in UPDATE_CELLS
    if not identity_shape(cell[1]) and update_parts(cell[1])[1] == "int"
]
UPDATE_REFUSED_STRUCT_CELLS = [
    cell
    for cell in UPDATE_CELLS
    if not identity_shape(cell[1]) and update_parts(cell[1])[1] != "int"
]


def inline_table_text(error: str) -> str:
    return error.split("SQLSTATE: 42000")[0] + "SQLSTATE: 42000"


def replay(doors: dict[str, recorder.Door], spark: Any, cell: tuple[str, str]) -> dict[str, Any]:
    group, key = cell
    if group == "x_rows":
        return recorder.rows_cell(spark, key)
    door, column, where, form = update_parts(key)
    return recorder.update_cell(doors, (door, column, where, form))


@pytest.fixture(scope="module")
def replayed(tmp_path_factory: pytest.TempPathFactory) -> dict[tuple[str, str], Any]:
    _reset_active_session_for_tests()
    warehouse = str(tmp_path_factory.mktemp("ice-struct-pairing-1-exception"))
    spark = base.open_repark(warehouse)
    doors = recorder.open_doors(spark, "repark", warehouse)
    recorder.create_namespaces(spark, doors, warehouse)
    return {cell: replay(doors, spark, cell) for cell in (*ROWS_CELLS, *UPDATE_CELLS)}


def test_the_recorded_groups_hold_every_cell_of_the_recorder() -> None:
    assert sorted(ORACLE["x_rows"]) == sorted(recorder.ROWS)
    assert sorted(ORACLE["x_update"]) == sorted(key for _, key in UPDATE_CELLS)
    for group in ("x_rows", "x_update"):
        assert sorted(MAIN[group]) == sorted(ORACLE[group])


def test_spark_stores_every_update_from_a_subquery_cell() -> None:
    for group, key in UPDATE_CELLS:
        rows = ORACLE[group][key].get("rows")
        assert rows is not None and rows != SEEDED, (key, ORACLE[group][key])


@pytest.mark.parametrize("cell", ROWS_SPARK_CELLS, ids=cell_id)
def test_multi_row_values_answer_as_spark(
    replayed: dict[tuple[str, str], Any], cell: tuple[str, str]
) -> None:
    spark = ORACLE[cell[0]][cell[1]]
    actual = replayed[cell]
    assert answers_as_spark(spark, actual), f"spark {spark}, engine {actual}"
    if "error" in spark:
        assert inline_table_text(spark["error"]) in actual["error"], actual


@pytest.mark.parametrize("cell", ROWS_ELSEWHERE_CELLS, ids=cell_id)
def test_multi_row_values_outside_the_agreement_rule_still_refuse(
    replayed: dict[tuple[str, str], Any], cell: tuple[str, str]
) -> None:
    actual = replayed[cell]
    assert "error" in actual, actual
    assert seed_only(actual.get("after")), actual


@pytest.mark.parametrize("cell", UPDATE_STORED_CELLS, ids=cell_id)
def test_the_identity_update_from_a_subquery_stores_spark_value(
    replayed: dict[tuple[str, str], Any], cell: tuple[str, str]
) -> None:
    assert replayed[cell] == ORACLE[cell[0]][cell[1]], replayed[cell]


@pytest.mark.parametrize("cell", UPDATE_REFUSED_INT_CELLS, ids=cell_id)
def test_ruling_74_a_flat_int_update_from_a_subquery_refuses_before_any_write(
    replayed: dict[tuple[str, str], Any], cell: tuple[str, str]
) -> None:
    actual = replayed[cell]
    assert UPDATE_REFUSAL in actual.get("error", ""), actual
    assert actual.get("after") == SEEDED, actual


@pytest.mark.parametrize("cell", UPDATE_REFUSED_STRUCT_CELLS, ids=cell_id)
def test_ruling_90_a_struct_update_from_a_subquery_refuses_before_any_write(
    replayed: dict[tuple[str, str], Any], cell: tuple[str, str]
) -> None:
    actual = replayed[cell]
    assert UPDATE_REFUSAL in actual.get("error", ""), actual
    assert actual.get("after") == SEEDED, actual
