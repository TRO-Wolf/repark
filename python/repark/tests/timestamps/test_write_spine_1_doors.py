"""WRITE-SPINE-1 step 3b: the door matrix on this head against the step-0 census fixtures."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import _write_spine_1_doors as doors
import _write_spine_1_matrix as matrix
import _write_spine_1_shapes as shapes
import _write_spine_1_spellings as corpus
import pytest
import test_write_spine_1_enumeration as enumeration
from pydantic import BaseModel, ConfigDict

from repark import _native

SPARK_DOORS = frozenset(f"D{number}" for number in range(1, 11))
EQUAL_STATUS = "EQUAL"
C010_SPELLINGS = frozenset(
    {
        "merge_update_whole",
        "merge_update_whole_literal",
        "merge_update_field",
        "merge_update_field_literal",
    }
)
C010_STATIC = "D8"
C010_RUNTIME = "D9"
STATIC = "static"
RUNTIME = "runtime"
NATIVE_TEXT_SPARK_READING: dict[str, str | None] = {
    "native_insert": "D1",
    "native_insert_conf": "D1",
    "native_insert_overwrite_partition": "D4",
    "native_ctas": "D6",
    "native_merge": "D8",
    "native_update": "D10",
    "native_delete": None,
}
NLS_1_MOVES = matrix.HERE / "write_spine_1_nls1_moves.json"
NLS_1_MOVE_COUNT = 52
ZS9_MARKERS: tuple[str, ...] = (
    "A value that is not provably a TIMESTAMP_NTZ wall time",
    "The session time zone is not UTC, and a nested TIMESTAMP_NTZ",
)
STORE_CAST_MARKER = "Cannot write incompatible data for the table"
SCALAR_SUBQUERY_MARKER = "SCALAR_SUBQUERY_IN_VALUES"
UPDATE_SUBQUERY_MARKER = "UPDATE with SET column = (SELECT ...) is not supported"
SAFE_CAST_MARKER = "CANNOT_SAFELY_CAST"
ARROW_MARKERS: tuple[str, ...] = (
    "column types must match schema types",
    "arguments need to have the same data type",
)
VALUES_SPELLINGS = frozenset({"insert_values", "insert_overwrite_values"})
RECOUNT_CELLS = 12720
RECOUNT_STATUSES = (7843, 1479, 2452, 946, 0)
RECOUNT_RED = 2649
RECOUNT_FAMILIES = {
    "F1": 240,
    "F2": 228,
    "F3": 596,
    "F4": 428,
    "F5": 52,
    "F6": 351,
    "F7": 318,
    "F8": 436,
}
SAMPLE_PER_CLASS = 1
ZS9_ROW = "ZS9-AHEAD-OF-DOOR"
D10_STORE_ROW = "D10-STORE-CAST-AHEAD-OF-DOOR"


class Divergence(BaseModel):
    """One recorded disagreement between a door reading and the corpus door, pinned as measured."""

    model_config = ConfigDict(frozen=True)

    row: str
    recorded: str
    reading: str
    doors: frozenset[str]
    markers: tuple[str, ...]
    measured: tuple[str, ...]
    cells: int
    reason: str


DIVERGENCES: tuple[Divergence, ...] = (
    Divergence(
        row=ZS9_ROW,
        recorded="2026-10-27",
        reading=RUNTIME,
        doors=frozenset({"D1", "D2", "D3", "D4", "D5", "D7", "D8", "D9", "D10"}),
        markers=ZS9_MARKERS,
        measured=(),
        cells=1048,
        reason=(
            "the ZS-9 nested-zone refusal raises at planning, before the router reaches the "
            "branch point of any door, so the record stays empty (1048 of 10,320 cells)"
        ),
    ),
    Divergence(
        row=D10_STORE_ROW,
        recorded="2026-10-27",
        reading=RUNTIME,
        doors=frozenset({"D10"}),
        markers=(STORE_CAST_MARKER,),
        measured=(),
        cells=672,
        reason=(
            "an UPDATE whose string value the store-assignment check refuses raises before "
            "D10 is recorded; an INSERT or a MERGE with the same refusal records its door "
            "(672 of 10,320 cells)"
        ),
    ),
)


def _holds(answer: dict[str, Any], markers: tuple[str, ...]) -> bool:
    """Return whether the answer raised with text that holds one of ``markers``."""
    text = f"{answer.get('error_class', '')} {answer.get('error', '')}"
    return "error" in answer and any(marker in text for marker in markers)


def _census_family(cell: shapes.Cell, main: dict[str, Any], spark: dict[str, Any]) -> str | None:
    """Return the census family F1 to F8 of one Spark-door red cell, first rule first."""
    kind = shapes.source_kind(cell.source)
    nested = cell.target.container != "leaf"
    outside_utc = cell.zone != "UTC"
    spark_raises = "error" in spark
    main_raises = "error" in main
    rules = (
        (
            "F1",
            cell.spelling.name in VALUES_SPELLINGS
            and shapes.is_column(cell.source)
            and _holds(spark, (SCALAR_SUBQUERY_MARKER,)),
        ),
        ("F2", _holds(main, (UPDATE_SUBQUERY_MARKER,))),
        (
            "F3",
            kind == "inst"
            and nested
            and cell.target.leaf == "ntz"
            and outside_utc
            and not spark_raises
            and _holds(main, ZS9_MARKERS[:1]),
        ),
        (
            "F4",
            kind == "wall"
            and nested
            and cell.target.leaf == "ltz"
            and outside_utc
            and not spark_raises
            and _holds(main, (SAFE_CAST_MARKER,)),
        ),
        (
            "F5",
            kind == "date"
            and nested
            and cell.target.leaf == "ltz"
            and outside_utc
            and not spark_raises
            and not main_raises,
        ),
        ("F6", kind == "str" and nested and spark_raises and not main_raises),
        ("F7", kind == "str" and spark_raises and main_raises),
        ("F8", not spark_raises and _holds(main, ARROW_MARKERS)),
    )
    return next((family for family, matches in rules if matches), None)


def _is_c010(cell: shapes.Cell) -> bool:
    """Return whether the cell is a MERGE whole-column update of a leaf column (C-010)."""
    return cell.spelling.name in C010_SPELLINGS and cell.target.container == "leaf"


def _marker_row(answer: dict[str, Any], reading: str) -> Divergence | None:
    """Return the first divergence row for ``reading`` whose markers the answer holds."""
    return next(
        (row for row in DIVERGENCES if row.reading == reading and _holds(answer, row.markers)),
        None,
    )


def _divergence_of(cell: shapes.Cell, answer: dict[str, Any], reading: str) -> Divergence | None:
    """Return the marker row of the cell's answer when the cell's door is one of its doors."""
    row = _marker_row(answer, reading)
    if row is None or cell.spelling.door not in row.doors:
        return None
    return row


def _live_cells() -> list[shapes.Cell]:
    """Return the census cells the facade session runs: ``spark_sql`` and ``dataframe``."""
    return [
        cell
        for cell in shapes.cells()
        if cell.spelling.reach in (corpus.SPARK_SQL, corpus.DATAFRAME)
    ]


def _find_runtime_disagreements(cells: list[shapes.Cell], tmp_path: Path) -> list[str]:
    """Run each cell on a facade session of its zone and return every disagreement found."""
    main = matrix.load_cells(matrix.MAIN_FIXTURE)
    failures: list[str] = []
    for zone in shapes.ZONES:
        zone_cells = [cell for cell in cells if cell.zone == zone]
        if not zone_cells:
            continue
        session = matrix.build_facade_session(zone, tmp_path / shapes.ZONE_TAGS[zone])
        try:
            for cell in zone_cells:
                recorded = matrix.runtime_doors(session, cell)
                row = _divergence_of(cell, main[cell.cell_id], RUNTIME)
                if _is_c010(cell):
                    expected_ok = recorded == [C010_RUNTIME]
                elif row is not None:
                    expected_ok = tuple(recorded) == row.measured
                else:
                    expected_ok = cell.spelling.door in recorded
                if not expected_ok:
                    failures.append(f"{cell.cell_id}: {recorded}")
                after = matrix.doors_after_select(session)
                if after:
                    failures.append(f"{cell.cell_id}: a SELECT recorded {after}")
        finally:
            session.stop()
    return failures


def test_fixture_cells_are_the_census() -> None:
    """Both fixtures hold the recount cells, status counts, Spark-door red set and families."""
    main = matrix.load_cells(matrix.MAIN_FIXTURE)
    spark = matrix.load_cells(matrix.SPARK_FIXTURE)
    axes = matrix.cells_by_id()
    assert len(main) == len(spark) == len(axes) == RECOUNT_CELLS
    assert set(main) == set(spark) == set(axes)
    measured = Counter(answer["status"] for answer in main.values())
    assert [measured[status] for status in matrix.recorder.STATUSES] == list(RECOUNT_STATUSES)
    red = [
        key
        for key, answer in main.items()
        if axes[key].spelling.door in SPARK_DOORS and answer["status"] != EQUAL_STATUS
    ]
    assert len(red) == RECOUNT_RED
    found = Counter(_census_family(axes[key], main[key], spark[key]) for key in red)
    assert dict(found) == RECOUNT_FAMILIES


def test_every_cell_spelling_names_its_corpus_door() -> None:
    """Each fixture cell's spelling is a corpus entry whose door is one of the eighteen."""
    engine = {door for door, _ in _native.write_doors()}
    assert len(engine) == len(doors.DOOR_SPELLINGS) == 18
    axes = matrix.cells_by_id()
    for key in matrix.load_cells(matrix.MAIN_FIXTURE):
        name = key.split(".", 1)[0]
        assert name in corpus.BY_NAME, key
        assert corpus.BY_NAME[name].door in engine, key
        assert axes[key].spelling == corpus.BY_NAME[name], key


def test_static_door_agrees_with_the_corpus_on_every_spark_sql_cell() -> None:
    """The static reading names the corpus door on every Spark SQL cell, C-010 class aside."""
    main = matrix.load_cells(matrix.MAIN_FIXTURE)
    cells = matrix.cells_of_reach(corpus.SPARK_SQL)
    assert cells
    failures: list[str] = []
    for cell in cells:
        door = matrix.static_door(cell)
        if _is_c010(cell):
            expected = C010_STATIC
        elif (row := _divergence_of(cell, main[cell.cell_id], STATIC)) is not None:
            expected = row.measured[0] if row.measured else None
        else:
            expected = cell.spelling.door
        if door is None or door != expected:
            failures.append(f"{cell.cell_id}: {door}")
    assert failures == []


def test_static_door_classifies_native_sql_cells() -> None:
    """Every native SQL cell answers its row of the table, the Spark reading of native text.

    The table is the Spark router's static reading of the native text, not the door the cell
    takes: ``repark.sql()`` runs every such cell through D14 (C-009).
    """
    cells = matrix.cells_of_reach(corpus.NATIVE_SQL)
    assert {cell.spelling.name for cell in cells} == set(NATIVE_TEXT_SPARK_READING)
    failures = [
        f"{cell.cell_id}: {door}"
        for cell in cells
        if (door := matrix.static_door(cell)) != NATIVE_TEXT_SPARK_READING[cell.spelling.name]
    ]
    assert failures == []


@pytest.mark.timeout(900)
def test_runtime_record_agrees_with_the_corpus_on_the_sample(tmp_path: Path) -> None:
    """The facade records the corpus door on each sampled Spark SQL cell; a SELECT records none."""
    cells = matrix.sample(matrix.cells_of_reach(corpus.SPARK_SQL), SAMPLE_PER_CLASS)
    assert len({(cell.spelling.name, cell.mode, cell.target.tag) for cell in cells}) == len(cells)
    assert {cell.zone for cell in cells} == set(shapes.ZONES)
    assert _find_runtime_disagreements(cells, tmp_path) == []


@pytest.mark.timeout(900)
def test_dataframe_writers_record_their_doors(tmp_path: Path) -> None:
    """Each DataFrame writer records its corpus door after the call, once per target."""
    cells = matrix.sample(matrix.cells_of_reach(corpus.DATAFRAME), SAMPLE_PER_CLASS)
    writers = {spelling.name for spelling in corpus.SPELLINGS if spelling.reach == corpus.DATAFRAME}
    assert {cell.spelling.name for cell in cells} == writers
    assert len(cells) == len(writers) * len(shapes.targets())
    assert _find_runtime_disagreements(cells, tmp_path) == []


@pytest.mark.parametrize("row", DIVERGENCES, ids=[row.row for row in DIVERGENCES])
def test_a_recorded_divergence_holds_as_measured(row: Divergence, tmp_path: Path) -> None:
    """Each row's markers select its cells over the whole census, and its doors are measured.

    Inside the row's doors a cell records the row's measured record; outside them a cell with
    the same markers records its own corpus door. Both run live on the first cell per spelling.
    """
    main = matrix.load_cells(matrix.MAIN_FIXTURE)
    assigned = [
        cell for cell in _live_cells() if _marker_row(main[cell.cell_id], row.reading) == row
    ]
    assert sum(1 for cell in assigned if cell.spelling.door in row.doors) == row.cells
    assert {cell.spelling.door for cell in assigned} >= row.doors
    firsts: dict[str, shapes.Cell] = {}
    for cell in assigned:
        firsts.setdefault(cell.spelling.name, cell)
    assert _find_runtime_disagreements(list(firsts.values()), tmp_path) == []


def test_excluded_reaches_are_exactly_the_corpus_says() -> None:
    """The doors no live test runs are D14 to D21, in three groups with their reasons."""
    live = {
        spelling.door
        for spelling in corpus.SPELLINGS
        if spelling.reach in (corpus.SPARK_SQL, corpus.DATAFRAME)
    }
    engine = [door for door, _ in _native.write_doors()]
    absent = {door for door in engine if door not in live}
    bare_session = {
        spelling.door
        for spelling in corpus.SPELLINGS
        if spelling.reach == corpus.NATIVE_SQL
        and doors.spelling_reach(spelling.name) == doors.PYTHON
    }
    rust_api = set(doors.rust_api_only_doors())
    no_reach = {spelling.door for spelling in corpus.SPELLINGS if spelling.reach == corpus.NONE}
    assert bare_session == {"D14"}
    assert rust_api == {f"D{number}" for number in range(15, 21)}
    assert no_reach == {"D21"}
    assert bare_session | rust_api | no_reach == absent
    assert len(bare_session) + len(rust_api) + len(no_reach) == len(absent)
    enumeration.test_every_rust_api_only_door_names_a_rust_test_that_exists()


def test_head_replay_moves_exactly_the_nested_literal_seam_1_cells() -> None:
    """A head replay differs from main exactly on the frozen seam moves, each EQUAL."""
    replays = sorted(matrix.HERE.glob(matrix.HEAD_FIXTURE_GLOB))
    if not replays:
        pytest.skip("replay document not present")
    main = matrix.load_cells(matrix.MAIN_FIXTURE)
    frozen = set(json.loads(NLS_1_MOVES.read_text(encoding="utf-8"))["moves"])
    assert len(frozen) == NLS_1_MOVE_COUNT
    for replay in replays:
        head = matrix.load_cells(replay)
        assert set(head) == set(main), replay.name
        differing = {key for key, answer in main.items() if head[key] != answer}
        assert differing == frozen, replay.name
        assert all(head[key]["status"] == "EQUAL" for key in differing), replay.name
