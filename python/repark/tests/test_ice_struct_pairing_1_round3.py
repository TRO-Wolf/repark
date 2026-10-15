from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import _record_ice_struct_pairing_1 as base
import _record_ice_struct_pairing_1_round3 as recorder
import pytest

from repark.spark.session import _reset_active_session_for_tests

HERE = Path(__file__).resolve().parent
ORACLE = json.loads((HERE / "ice_struct_pairing_1_spark_oracle.json").read_text("utf-8"))
MAIN = json.loads((HERE / "ice_struct_pairing_1_main.json").read_text("utf-8"))
GROUPS = tuple(recorder.GROUPS)
CARVE_OUT_HINT = "the table's field names in the table's"
CONDITION = re.compile(r"\[([A-Z_]+(?:\.[A-Z_]+)?)\]")
DETAIL = re.compile(r"Cannot write incompatible data for the table .*?: (.*?)\. SQLSTATE")


def condition(error: str) -> str | None:
    found = CONDITION.search(error)
    return found.group(1) if found else None


def detail(error: str) -> str | None:
    found = DETAIL.search(error)
    return found.group(1) if found else None


def door_and_shape(group: str, key: str) -> tuple[str, str]:
    parts = key.split("|")
    if group == "r3_misc":
        return "", key
    return parts[-2], parts[-1]


def in_carve_out(group: str, key: str) -> bool:
    door, shape = door_and_shape(group, key)
    return (
        door in recorder.PART_DOORS
        and shape not in recorder.NAMED_ALIKE
        and "rows" in ORACLE[group][key]
    )


def pre_existing_setup(group: str, key: str) -> bool:
    return "setup_error" in MAIN[group][key]


def cells(group: str) -> list[tuple[str, str]]:
    return [(group, key) for key in sorted(ORACLE[group])]


ALL_CELLS = [cell for group in GROUPS for cell in cells(group)]
SETUP_CELLS = [cell for cell in ALL_CELLS if pre_existing_setup(*cell)]
CARVE_OUT_CELLS = [
    cell for cell in ALL_CELLS if in_carve_out(*cell) and not pre_existing_setup(*cell)
]
SPARK_CELLS = [
    cell for cell in ALL_CELLS if not in_carve_out(*cell) and not pre_existing_setup(*cell)
]


def cell_id(cell: tuple[str, str]) -> str:
    return "|".join(cell)


def seed_only(after: Any) -> bool:
    return isinstance(after, list) and [row[0] for row in after] == [0]


def answers_as_spark(spark: dict[str, Any], actual: dict[str, Any]) -> bool:
    if "rows" in spark:
        return actual.get("rows") == spark["rows"]
    error = actual.get("error")
    if error is None or condition(error) != condition(spark["error"]):
        return False
    expected_detail = detail(spark["error"])
    if expected_detail is not None and expected_detail not in error:
        return False
    return actual.get("after") == spark.get("after")


def replay(spark: Any, group: str, key: str) -> dict[str, Any]:
    if group == "r3_leaf":
        door, case = key.split("|")
        return recorder.leaf_cell(spark, door, case)
    if group == "r3_part":
        version, sensitivity, door, shape = key.split("|")
        case_sensitive = "true" if sensitivity == "cs" else "false"
        return recorder.part_cell(spark, version.removeprefix("v"), case_sensitive, door, shape)
    return recorder.misc_cell(spark, key)


@pytest.fixture(scope="module")
def replayed(tmp_path_factory: pytest.TempPathFactory) -> dict[tuple[str, str], Any]:
    _reset_active_session_for_tests()
    warehouse = tmp_path_factory.mktemp("ice-struct-pairing-1-round3")
    spark = base.open_repark(str(warehouse))
    spark.sql(f"CREATE NAMESPACE ice.{recorder.NAMESPACE}")
    return {cell: replay(spark, *cell) for cell in ALL_CELLS}


def test_the_recorded_groups_hold_every_cell_of_the_recorder() -> None:
    assert sorted(ORACLE["r3_leaf"]) == sorted(
        recorder.leaf_key(door, case)
        for door in recorder.LEAF_DOORS
        for case in recorder.LEAF_CASES
    )
    assert sorted(ORACLE["r3_part"]) == sorted(
        recorder.part_key(*key) for key in recorder.part_keys()
    )
    assert sorted(ORACLE["r3_misc"]) == sorted(recorder.MISC)
    for group in GROUPS:
        assert sorted(MAIN[group]) == sorted(ORACLE[group])


@pytest.mark.parametrize("cell", SPARK_CELLS, ids=cell_id)
def test_the_engine_answers_as_spark(
    replayed: dict[tuple[str, str], Any], cell: tuple[str, str]
) -> None:
    spark = ORACLE[cell[0]][cell[1]]
    actual = replayed[cell]
    assert answers_as_spark(spark, actual), f"spark {spark}, engine {actual}"


@pytest.mark.parametrize("cell", CARVE_OUT_CELLS, ids=cell_id)
def test_the_partition_carve_out_refuses_with_the_workaround(
    replayed: dict[tuple[str, str], Any], cell: tuple[str, str]
) -> None:
    actual = replayed[cell]
    error = actual.get("error", "")
    assert condition(error) == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST", actual
    assert CARVE_OUT_HINT in error, actual
    assert seed_only(actual.get("after")), actual


@pytest.mark.parametrize("cell", SETUP_CELLS, ids=cell_id)
def test_a_required_nested_field_still_cannot_be_seeded_as_on_main(
    replayed: dict[tuple[str, str], Any], cell: tuple[str, str]
) -> None:
    assert "setup_error" in replayed[cell], replayed[cell]
    assert "rows" in ORACLE[cell[0]][cell[1]] or "error" in ORACLE[cell[0]][cell[1]]
