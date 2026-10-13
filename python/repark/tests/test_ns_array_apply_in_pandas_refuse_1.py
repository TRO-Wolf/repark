from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _ice_tsns_merge_wall_1_doors as doors
import _ns_array_apply_in_pandas_refuse_1_cases as cases
import pytest

from repark.errors import PySparkTypeError

RECORDED: dict[str, Any] = json.loads(cases.FIXTURE.read_text(encoding="utf-8"))
MAIN_CELLS: dict[str, Any] = RECORDED["cells"]
MAIN_COMMIT = "f3f98823"
KEYS = cases.cell_keys()
MAIN_KEYS = [key for key in KEYS if cases.EXPECTED[key.split("|")[0]] == cases.MAIN]
ARRAY_KEYS = [key for key in KEYS if cases.EXPECTED[key.split("|")[0]] == cases.ARRAY]
INTEGER_KEYS = [key for key in KEYS if cases.EXPECTED[key.split("|")[0]] == cases.INTEGER]
ARRAY_REFUSAL = (
    "applyInPandas refuses column 'v' of declared type {declared}: the pandas result holds "
    "nanosecond datetimes (datetime64[ns]) inside a list, which would store wrong values. "
    'Cast the elements to microseconds in the function (for example .astype("datetime64[us]")) '
    "before returning."
)
INTEGER_REFUSAL = (
    "applyInPandas refuses column 'v' of declared type {declared}: the pandas result holds an "
    "integer that the group frame carried for a nanosecond timestamp (timestamp[ns]) inside a "
    "struct or map, which would store wrong values. Convert it to microseconds in the function "
    "(for example value // 1000) before returning."
)
FLOORS = [tick // 1000 for tick in doors.NANOS]


def test_the_fixture_holds_main_for_every_cell() -> None:
    """The fixture is main's and names exactly the cells of the matrix."""
    assert RECORDED["recorded_at_commit"] == MAIN_COMMIT
    assert sorted(MAIN_CELLS) == sorted(KEYS)


@pytest.mark.parametrize("key", [*ARRAY_KEYS, *INTEGER_KEYS])
def test_main_stored_a_value_where_the_door_now_refuses(key: str) -> None:
    """Every refused cell is one main answered with a stored value, not an error."""
    assert "error" not in MAIN_CELLS[key]


@pytest.mark.parametrize("key", MAIN_KEYS)
def test_main_answer_stands(tmp_path: Path, key: str) -> None:
    """A cell main stores right, refuses itself, or the door cannot judge answers as main."""
    cell, _error = cases.answer_in_fresh_session(tmp_path, key)
    assert cell == MAIN_CELLS[key]


@pytest.mark.parametrize("key", ARRAY_KEYS)
def test_nanosecond_array_under_a_timestamp_list_refuses(tmp_path: Path, key: str) -> None:
    """A numpy nanosecond array main stored as microseconds refuses with the cast hint."""
    cell, error = cases.answer_in_fresh_session(tmp_path, key)
    assert type(error) is PySparkTypeError, cell
    assert str(error) == ARRAY_REFUSAL.format(declared=MAIN_CELLS[key]["type"])


@pytest.mark.parametrize("key", INTEGER_KEYS)
def test_handed_nanosecond_integer_under_a_timestamp_refuses(tmp_path: Path, key: str) -> None:
    """An integer the group frame carried for a nanosecond timestamp refuses with the hint."""
    cell, error = cases.answer_in_fresh_session(tmp_path, key)
    assert type(error) is PySparkTypeError, cell
    assert str(error) == INTEGER_REFUSAL.format(declared=MAIN_CELLS[key]["type"])


@pytest.mark.parametrize("door", [cases.GROUPED, cases.COGROUPED])
@pytest.mark.parametrize(
    "name", ["table_array_ns_cast_in_function", "table_array_tzns_cast_in_function"]
)
def test_the_array_hint_stores_the_microsecond_floor(name: str, door: str) -> None:
    """Casting the arrays in the function, as the refusal says, stores every floor."""
    assert MAIN_CELLS[f"{name}|{door}|UTC"]["values"] == [[floor] for floor in FLOORS]


@pytest.mark.parametrize("door", [cases.GROUPED, cases.COGROUPED])
@pytest.mark.parametrize(
    "name", ["table_struct_ns_floor_divided", "table_struct_tzns_floor_divided"]
)
def test_the_integer_hint_stores_the_microsecond_floor(name: str, door: str) -> None:
    """Floor-dividing the integers in the function, as the refusal says, stores every floor."""
    assert MAIN_CELLS[f"{name}|{door}|UTC"]["values"] == [{"t": floor} for floor in FLOORS]


def test_a_tick_equal_to_its_floor_is_stored_right_by_main() -> None:
    """Zero and minus one nanoseconds are their own microsecond floors: main stores them right."""
    assert MAIN_CELLS["zero_minus_one_nat_ns|grouped|UTC"]["values"] == [[0, -1, None]] * 2
    assert MAIN_CELLS["table_array_ns_minus_one_only|grouped|UTC"]["values"] == [[-1]]
    assert MAIN_CELLS["table_struct_ns_minus_one_only|grouped|UTC"]["values"] == [{"t": -1}]


def test_main_stores_a_small_tick_wrong() -> None:
    """One nanosecond is stored as one microsecond by main, where the floor is zero."""
    assert MAIN_CELLS["ns_tick_one|grouped|UTC"]["values"] == [[1], [1]]
    assert MAIN_CELLS["ns_tick_minus_two|grouped|UTC"]["values"] == [[-2], [-2]]
    assert [1 // 1000, -2 // 1000] == [0, -1]


def test_the_open_cells_hold_what_main_stores() -> None:
    """The cells the door cannot judge keep main's stored ticks, recorded for a later unit."""
    plus_one = [{"t": tick + 1} for tick in doors.NANOS]
    assert MAIN_CELLS["open_table_struct_ns_plus_one|grouped|UTC"]["values"] == plus_one
    floor_row = [{"t": cases.NS // 1000}]
    assert MAIN_CELLS["open_frame_floor_tick_row_only|grouped|UTC"]["values"] == floor_row
    milliseconds = [[cases.NS // 1_000_000, cases.NS2 // 1_000_000]] * 2
    assert MAIN_CELLS["open_ms_stored_as_raw_ticks|grouped|UTC"]["values"] == milliseconds
