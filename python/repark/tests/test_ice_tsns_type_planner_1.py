"""ICE-TSNS-TYPE-PLANNER-1: the SQL type name decides the unit, on every door.

Every cell's expectation is derived by ``_ice_tsns_type_planner_1_rule`` from
the unit's three clauses; nothing here names an expected value. A statement
that answered on main's module must answer here, unless the cell is a ruled
refusal.

pins: ice-tsns-type-planner-1/C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import json
from pathlib import Path

import _ice_tsns_type_planner_1_cells as cells
import _ice_tsns_type_planner_1_pins as pins
import _ice_tsns_type_planner_1_repros as repros
import _ice_tsns_type_planner_1_rule as rule
import pytest

MAIN = Path(__file__).resolve().parent / "ice_tsns_type_planner_1_main.json"
DATED = Path(__file__).resolve().parent / "ice_tsns_type_planner_1_dated.json"
CENSUS_CELLS = 9600
CACHE_CELLS = 420
READ_CELLS = 117
CONSUMER_CELLS = 92
CARVE_OUT_CELLS = 108


def all_groups() -> list[rule.Group]:
    """Return every group of the fixture, in family order."""
    return [
        *cells.census_groups(),
        *cells.cache_groups(),
        *cells.reads_groups(),
        *cells.consumer_groups(),
        *repros.repro_groups(),
        *pins.lattice_groups(),
        *pins.position_groups(),
        *pins.door_groups(),
        *pins.postgres_groups(),
        *pins.q6_groups(),
        *pins.update_form_groups(),
    ]


def run_group(group: rule.Group, scratch: Path) -> tuple[dict[str, str], dict[str, bool]]:
    """Run one group and return its cells and which statements answered."""
    rule.ANSWERED.clear()
    results = group.run(scratch)
    return results, dict(rule.ANSWERED)


def _main_errored() -> frozenset[str]:
    return frozenset(json.loads(MAIN.read_text(encoding="utf-8"))["errored"])


def dated_rows() -> dict[str, dict[str, str]]:
    """Return the dated rows: cells ruled to stay red, each with its date and owner."""
    return json.loads(DATED.read_text(encoding="utf-8"))["rows"]


def _check(group: rule.Group, scratch: Path) -> None:
    results, answered = run_group(group, scratch)
    assert len(results) == group.cells, group.name
    dated = dated_rows()
    red = {cell: reason for cell, reason in results.items() if reason and reason != "skip"}
    open_red = {cell: reason for cell, reason in red.items() if cell not in dated}
    assert open_red == {}, f"{len(open_red)} of {len(results)} cells red in {group.name}"
    errored = _main_errored()
    lost = {cell for cell, ok in answered.items() if cell not in errored and not ok}
    open_lost = sorted(lost - dated.keys())
    assert open_lost == [], f"main answers, this module errors: {open_lost[:10]}"
    seen = results.keys() | answered.keys()
    retired = sorted(cell for cell in dated if cell in seen and cell not in red.keys() | lost)
    assert retired == [], f"dated rows now green, retire them: {retired[:10]}"


def test_family_sizes() -> None:
    """pins: ice-tsns-type-planner-1/C-006 — the fixture holds every named family."""
    sizes: dict[str, int] = {}
    for group in all_groups():
        sizes[group.family] = sizes.get(group.family, 0) + group.cells
    assert sizes["census"] == CENSUS_CELLS
    assert sizes["cache"] == CACHE_CELLS
    assert sizes["reads"] == READ_CELLS
    assert sizes["consumer"] == CONSUMER_CELLS
    assert sizes["lattice"] == 20 * 2 * 3


@pytest.mark.parametrize("group", cells.census_groups(), ids=lambda group: group.name)
def test_census_cell_equals_its_typed_twin(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-007 — a NULL beside a nanosecond value is nanoseconds."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", cells.cache_groups(), ids=lambda group: group.name)
def test_cached_frame_stores_what_the_frame_stores(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-007 — the cache is a pass-through."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", cells.reads_groups(), ids=lambda group: group.name)
def test_read_over_a_site_equals_its_typed_twin(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-008 — the 117 reads."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", cells.consumer_groups(), ids=lambda group: group.name)
def test_consumer_door_over_a_site_equals_the_column(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-008 — the consumer doors."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", repros.repro_groups(), ids=lambda group: group.name)
def test_parking_repros_of_the_seven_rounds(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-009 — R-017's park record, by name."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", pins.lattice_groups(), ids=lambda group: group.name)
def test_mixed_units_widen_to_the_finest_on_both_doors(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-010 — the twenty cells, both doors, three zones."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", pins.position_groups(), ids=lambda group: group.name)
def test_every_lattice_position_equals_its_typed_twin(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-010 — a NULL or a microsecond value in every position."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", pins.door_groups(), ids=lambda group: group.name)
def test_refusals_microsecond_pins_and_registry_rows(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-010 — H1, H2, P15, R-010, R-013, R-018(1)."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", pins.update_form_groups(), ids=lambda group: group.name)
def test_an_identity_update_stores_what_its_other_forms_store(
    group: rule.Group, tmp_path: Path
) -> None:
    """pins: ice-tsns-type-planner-1/C-019 — ruling 91, WHERE, no WHERE and a written CAST."""
    _check(group, tmp_path)


@pytest.mark.parametrize("group", pins.q6_groups(), ids=lambda group: group.name)
def test_repark_sql_plans_timestamp_as_the_wall(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-013 — ruling 94 measured on ``repark.sql()``."""
    _check(group, tmp_path)


def test_every_dated_row_names_its_date_and_owner() -> None:
    """pins: ice-tsns-type-planner-1/C-013 — a dated row carries its date, ruling and owner."""
    for cell, row in dated_rows().items():
        assert {"date", "ruling", "owner"} <= row.keys(), cell


def test_the_door_carve_out_is_ruling_38s_108_cells() -> None:
    """pins: ice-tsns-type-planner-1/C-019 — the pandas and Arrow doors stay out of U-1."""
    carved = [cell for cell, row in dated_rows().items() if row.get("class") == "carve-out"]
    assert len(carved) == CARVE_OUT_CELLS
    assert all(dated_rows()[cell]["ruling"] == "38" for cell in carved)


def test_every_residue_row_is_red_on_main() -> None:
    """pins: ice-tsns-type-planner-1/C-019 — a residue row needs a red pin on today's main."""
    red_on_main = json.loads(MAIN.read_text(encoding="utf-8"))["red"]
    residue = [cell for cell, row in dated_rows().items() if row.get("class") == "residue"]
    assert [cell for cell in residue if cell not in red_on_main] == []


@pytest.mark.parametrize("group", pins.postgres_groups(), ids=lambda group: group.name)
def test_postgres_timestamps_read_as_microseconds(group: rule.Group, tmp_path: Path) -> None:
    """pins: ice-tsns-type-planner-1/C-010 — the two Postgres read cells."""
    _check(group, tmp_path)
