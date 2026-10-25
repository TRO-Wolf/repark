"""WRITE-SPINE-1 step 3b: the door matrix on this head against the step-0 census fixtures."""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any

import _write_spine_1_doors as doors
import _write_spine_1_matrix as matrix
import _write_spine_1_shapes as shapes
import _write_spine_1_spellings as corpus
import test_write_spine_1_enumeration as enumeration
from pydantic import BaseModel, ConfigDict

from repark import _native

CENSUS = Path(__file__).resolve().parents[4] / "task/wo/write-spine-1/census-2026-10-24.md"
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
STATIC = "static"
NATIVE_TEXT_SPARK_READING: dict[str, str | None] = {
    "native_insert": "D1",
    "native_insert_conf": "D1",
    "native_insert_overwrite_partition": "D4",
    "native_ctas": "D6",
    "native_merge": "D8",
    "native_update": "D10",
    "native_delete": None,
}
ZS9_MARKERS: tuple[str, ...] = (
    "A value that is not provably a TIMESTAMP_NTZ wall time",
    "The session time zone is not UTC, and a nested TIMESTAMP_NTZ",
)
SCALAR_SUBQUERY_MARKER = "SCALAR_SUBQUERY_IN_VALUES"
UPDATE_SUBQUERY_MARKER = "UPDATE with SET column = (SELECT ...) is not supported"
SAFE_CAST_MARKER = "CANNOT_SAFELY_CAST"
ARROW_MARKERS: tuple[str, ...] = (
    "column types must match schema types",
    "arguments need to have the same data type",
)
VALUES_SPELLINGS = frozenset({"insert_values", "insert_overwrite_values"})
FAMILY_ROW = re.compile(r"^\| (F\d) \| .+ \| ([\d,]+) \| (?:D\d+ \d+(?:, )?)+ \|$", re.MULTILINE)


class Divergence(BaseModel):
    """One recorded disagreement between a door reading and the corpus door, pinned as measured."""

    model_config = ConfigDict(frozen=True)

    row: str
    recorded: str
    reading: str
    doors: frozenset[str]
    markers: tuple[str, ...]
    measured: tuple[str, ...]
    reason: str


DIVERGENCES: tuple[Divergence, ...] = ()


def _number(text: str) -> int:
    """Return the integer a census number such as ``12,192`` writes."""
    return int(text.replace(",", ""))


def _census_number(pattern: str, text: str) -> list[int]:
    """Return the numbers one census regex captures; fail when it matches nothing."""
    found = re.search(pattern, text)
    assert found is not None, pattern
    return [_number(group) for group in found.groups()]


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


def _divergence_of(cell: shapes.Cell, answer: dict[str, Any], reading: str) -> Divergence | None:
    """Return the recorded divergence row a cell falls in for ``reading``, if any."""
    return next(
        (
            row
            for row in DIVERGENCES
            if row.reading == reading
            and cell.spelling.door in row.doors
            and _holds(answer, row.markers)
        ),
        None,
    )


def test_fixture_cells_are_the_census() -> None:
    """Both fixtures hold the census's cells, status counts, Spark-door red set and families."""
    text = CENSUS.read_text(encoding="utf-8")
    [cell_count] = _census_number(r"generates ([\d,]+) cells", text)
    status_counts = _census_number(
        r"\*\*([\d,]+) EQUAL, ([\d,]+) DIFFERENT, ([\d,]+) REFUSED, ([\d,]+) ERROR,\s+"
        r"([\d,]+) UNRECORDED\.\*\*",
        text,
    )
    [red_count] = _census_number(
        r"\*\*The Spark doors \(D1.D10, `spark_sql` and `dataframe` reach\), ([\d,]+) cells:\*\*",
        text,
    )
    families = {family: _number(count) for family, count in FAMILY_ROW.findall(text)}
    assert sorted(families) == [f"F{number}" for number in range(1, 9)]
    main = matrix.load_cells(matrix.MAIN_FIXTURE)
    spark = matrix.load_cells(matrix.SPARK_FIXTURE)
    axes = matrix.cells_by_id()
    assert len(main) == len(spark) == len(axes) == cell_count
    assert set(main) == set(spark) == set(axes)
    measured = Counter(answer["status"] for answer in main.values())
    assert [measured[status] for status in matrix.recorder.STATUSES] == status_counts
    red = [
        key
        for key, answer in main.items()
        if axes[key].spelling.door in SPARK_DOORS and answer["status"] != EQUAL_STATUS
    ]
    assert len(red) == red_count
    found = Counter(_census_family(axes[key], main[key], spark[key]) for key in red)
    assert dict(found) == families


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
