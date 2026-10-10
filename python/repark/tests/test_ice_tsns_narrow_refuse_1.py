"""ICE-TSNS-NARROW-REFUSE-1: an inserted narrowing refuses, a written one stores.

``coalesce(ns, NULL)``, ``CASE … ELSE NULL END``, ``if(…, ns, NULL)`` and ``array(ns, NULL)[0]``
over a nanosecond value are typed microseconds, because type coercion reads the untyped NULL as
the SQL ``TIMESTAMP`` and narrows the value to match. Every write door refuses such a value
into a ``timestamp_ns`` or ``timestamptz_ns`` column by name and stores nothing, and so does a
value narrowed beside a NULL typed ``TIMESTAMP`` or beside a microsecond value. A ``CAST``, a
``TRY_CAST`` or a ``date_trunc`` the statement writes stores what the base stores, also over a
narrowed value where the call stores what it stores over the nanosecond value; a nanosecond
type the statement keeps, a microsecond target and every other cell answer what the base
answers, cell for cell.

Each test runs one row of the matrix (a zone, a target, a source, a spelling) through all 64
doors and holds each cell against ``ice_tsns_narrow_refuse_1_base.json``, the base measured
before the change. What a door stored is read from every Parquet file under the table.

pins: ice-tsns-narrow-refuse-1/C-001, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _ice_tsns_narrow_refuse_1_doors as doors
import _record_ice_tsns_narrow_refuse_1 as recorder
import pytest

BASE = recorder.unpack(json.loads(recorder.FIXTURE.read_text()))
CONTROL_ZONE = "America/New_York"
REFUSED_EVERYWHERE = ("coalesce", "case", "if", "elt")
REFUSED_MORE = (
    "case_null_first",
    "coalesce_null_first",
    "element_at",
    "recast_of_coalesce",
    "nested_case",
    "lambda",
    "greatest",
    "nullif",
    "struct_field",
    "case_three",
)
TYPED_A_STRING = ("nvl", "ifnull", "nvl2")
NANOSECOND_TYPED = (*doors.KEPT, "try_cast_ts")
NARROWED = (*REFUSED_EVERYWHERE, *REFUSED_MORE, *doors.BESIDE_VALUE)
MATERIALIZED = "cached_frame_append"
BESIDE_A_PLAIN_BRANCH = "union_insert"
NARROWS_A_PLAIN_BRANCH = {
    "cast_ts": doors.SOURCES,
    "cast_ntz": doors.SOURCES,
    "date_trunc": doors.SOURCES,
    "case_of_cast": doors.SOURCES,
    "coalesce_of_cast": doors.SOURCES,
    "if_of_trunc": doors.SOURCES,
    "cast_date": ("ns",),
    "nvl": doors.SOURCES,
    "ifnull": doors.SOURCES,
    **dict.fromkeys(doors.WRITTEN_OVER, doors.SOURCES),
}
REFUSAL_ROWS = [
    (zone, target, source, spelling)
    for zone in doors.ZONES
    for target in doors.NANOSECOND_TARGETS
    for source in doors.SOURCES
    for spelling in (NARROWED if zone == CONTROL_ZONE else ("coalesce", "typed_ts_null"))
]
WRITTEN_OVER_ROWS = [
    (zone, target, source, spelling)
    for zone in doors.ZONES
    for target in doors.NANOSECOND_TARGETS
    for source in doors.SOURCES
    for spelling in doors.WRITTEN_OVER
    if zone == CONTROL_ZONE or spelling == "trunc_of_coalesce"
]
STORING_ROWS = [
    (CONTROL_ZONE, target, source, spelling)
    for target in doors.NANOSECOND_TARGETS
    for source in doors.SOURCES
    for spelling in (*doors.WRITTEN, *doors.KEPT, *TYPED_A_STRING)
]
MICROSECOND_ROWS = [
    (CONTROL_ZONE, target, source, spelling)
    for target in ("ltz", "ntz")
    for source in doors.SOURCES
    for spelling in REFUSED_EVERYWHERE
]


def measure_row(
    warehouse: Path, zone: str, target: str, source: str, spelling: str
) -> dict[str, tuple[dict[str, Any], dict[str, Any], str]]:
    """Return, per door, the cell measured now, the base's cell and the base's class."""
    spark = doors.open_session(zone, warehouse)
    measured = {}
    try:
        for door in doors.DOORS:
            key = f"{zone}|{target}|{source}|{spelling}|{door}"
            table = f"ice.ns.x{doors.table_number(source, spelling, door)}"
            cell = doors.measure(spark, warehouse, table, (target, source, spelling, door))
            measured[door] = (cell, BASE[key], recorder.classify(BASE, key))
    finally:
        spark.stop()
    return measured


def hold(row: tuple[str, str, str, str], door: str, cells: tuple[Any, Any, str]) -> bool:
    """Hold one cell to the rule; return whether it is a cut value that now refuses.

    A cell the base stored in part (one branch of a ``UNION`` whole, the other cut) refuses
    like a cut one; a door that creates the table keeps the microsecond column it made.
    """
    _, target, source, spelling = row
    cell, base, kind = cells
    differs = spelling in doors.WRITTEN_OVER and source not in doors.EQUAL_OVER[spelling]
    plain_branch = door == BESIDE_A_PLAIN_BRANCH and source in NARROWS_A_PLAIN_BRANCH.get(
        spelling, ()
    )
    refuses = door != MATERIALIZED and (
        spelling in NARROWED
        or differs
        or plain_branch
        or (door in doors.UNION_NULL and spelling in NANOSECOND_TYPED)
    )
    partial = kind == "other" and door not in doors.CREATES
    if refuses and (kind in ("cut", "cut+wall") or partial):
        assert cell.get("refused"), (door, cell)
        assert cell["stored"] == [], (door, cell)
        named = f'Cannot safely cast `v` "TIMESTAMP" to "{doors.TARGETS[target].upper()}"'
        if door in doors.CREATES:
            assert doors.REFUSAL_HEAD.search(cell["error"]), (door, cell)
        else:
            assert named in cell["error"], (door, cell)
        return True
    if refuses and kind in ("error", "refused"):
        assert "error" in cell, (door, cell)
        assert cell["stored"] == [], (door, cell)
        return False
    assert doors.comparable(cell) == doors.comparable(base), door
    return False


@pytest.mark.parametrize("row", REFUSAL_ROWS, ids="|".join)
def test_every_door_refuses_a_value_narrowed_beside_an_untyped_null(
    tmp_path: Path, row: tuple[str, str, str, str]
) -> None:
    """A cell that stored a cut value on the base refuses by name and stores nothing."""
    measured = measure_row(tmp_path, *row)
    refused = sum(hold(row, door, cells) for door, cells in measured.items())
    assert refused >= 45, refused


@pytest.mark.parametrize("row", WRITTEN_OVER_ROWS, ids="|".join)
def test_a_written_call_over_a_narrowed_value_stores_where_it_is_equal(
    tmp_path: Path, row: tuple[str, str, str, str]
) -> None:
    """An equal arm stores the base's cell; a differing arm refuses on every door.

    The base's cell of an equal arm is the value the same call stores over the nanosecond
    value (the twin spelling of ``doors.WRITTEN_TWIN``), which the stored rows are held to.
    """
    zone, target, source, spelling = row
    measured = measure_row(tmp_path, *row)
    refused = sum(hold(row, door, cells) for door, cells in measured.items())
    if source not in doors.EQUAL_OVER[spelling]:
        assert refused >= 45, refused
        return
    assert refused == 1, refused
    twin = f"{zone}|{target}|{source}|{doors.WRITTEN_TWIN[spelling]}|insert_select"
    assert measured["insert_select"][0]["stored"] == BASE[twin]["stored"]


@pytest.mark.parametrize("row", STORING_ROWS, ids="|".join)
def test_a_written_narrowing_and_a_kept_type_answer_what_the_base_answers(
    tmp_path: Path, row: tuple[str, str, str, str]
) -> None:
    """No cell moves but a nanosecond branch of a UNION narrowed beside another branch.

    A nanosecond type the statement keeps refuses beside an untyped NULL branch; a
    microsecond value the statement wrote makes the plain nanosecond branch beside it refuse.
    """
    measured = measure_row(tmp_path, *row)
    refused = sum(hold(row, door, cells) for door, cells in measured.items())
    if row[3] in NANOSECOND_TYPED:
        expected = len(doors.UNION_NULL)
    else:
        expected = int(row[2] in NARROWS_A_PLAIN_BRANCH.get(row[3], ()))
    assert refused == expected, refused


@pytest.mark.parametrize("row", MICROSECOND_ROWS, ids="|".join)
def test_a_microsecond_target_does_not_move(tmp_path: Path, row: tuple[str, str, str, str]) -> None:
    """A ``TIMESTAMP`` or ``TIMESTAMP_NTZ`` column takes a narrowed value as on the base.

    The four doors that create the table from the query never see the target type; their
    cells are the nanosecond rows' and are held there.
    """
    stored = 0
    for door, (cell, base, _) in measure_row(tmp_path, *row).items():
        if door in doors.CREATES:
            continue
        assert doors.comparable(cell) == doors.comparable(base), door
        stored += bool(cell.get("stored"))
    assert stored >= 40, stored


def test_the_base_fixture_holds_the_counts_the_parity_row_records() -> None:
    """The 650-cell core of the base: 360 cut, 120 cut and moved, 40 refused, 130 errors."""
    counts = recorder.summarize(BASE)["core 650"]
    assert dict(counts) == {"cut": 360, "cut+wall": 120, "refused": 40, "error": 130}
    assert len(BASE) == 104_960
