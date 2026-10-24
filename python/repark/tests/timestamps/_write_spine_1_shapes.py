"""WRITE-SPINE-1 step 0: the shape axis and the door-by-shape cells, generated deterministically.

NOT a ``test_`` module. A shape is a value source, a target column type and a session zone.
A cell is one spelling of ``_write_spine_1_spellings.py`` run on one shape, in copy-on-write
or merge-on-read mode when the spelling is row-level. The sources, the containers and the
zone tags are the ZONE-ORACLE-1 corpus's (``_zone_oracle_1_corpus.py``), so a cell id here
and an expression key there use one vocabulary. Nothing here is random or time-dependent.
Run it as a script to print the cell count and the corpus hash::

    python python/repark/tests/timestamps/_write_spine_1_shapes.py

Axes:

- sources: the instant, wall, string and date literals, and one column of each from the
  one-row temp view ``write_spine_1_source`` (``id`` plus the corpus's ``c_*`` columns);
- targets: a leaf ``TIMESTAMP`` and a leaf ``TIMESTAMP_NTZ``, and a struct, an array, a map
  and a struct inside an array holding each;
- zones: ``UTC``, ``America/New_York``, ``Asia/Kolkata``.

A field-assignment spelling writes a named leaf, so it runs only on the leaf and the struct
targets: Spark SQL has no assignment to one array element or one map value.
"""

from __future__ import annotations

import hashlib
import json

from _write_spine_1_spellings import FIELD_TARGETS, Spelling, measured_spellings
from _zone_oracle_1_corpus import COLUMNS, LITERALS, SHAPES, TARGETS, ZONE_TAGS
from pydantic import BaseModel, ConfigDict

ZONES: tuple[str, ...] = ("UTC", "America/New_York", "Asia/Kolkata")
VIEW_NAME = "write_spine_1_source"
LITERAL_SOURCES: tuple[str, ...] = ("inst", "wall", "str", "date")
COLUMN_SOURCES: tuple[str, ...] = ("col_inst", "col_wall", "col_str", "col_date")
SOURCES: tuple[str, ...] = (*LITERAL_SOURCES, *COLUMN_SOURCES)
SOURCE_TYPES: dict[str, str] = {
    "inst": "TIMESTAMP",
    "wall": "TIMESTAMP_NTZ",
    "str": "STRING",
    "date": "DATE",
}
LEAF_TAGS: tuple[str, ...] = ("ltz", "ntz")
CONTAINERS: tuple[str, ...] = ("leaf", "struct", "array", "map", "array_struct")
FIELD_CONTAINERS: tuple[str, ...] = ("leaf", "struct")
MODES_ROW_LEVEL: tuple[str, ...] = ("cow", "mor")
APPEND_MODE = "append"
VIEW_SQL = (
    f"CREATE OR REPLACE TEMP VIEW {VIEW_NAME} AS SELECT 1 AS id, "
    + ", ".join(f"{LITERALS[source]} AS {COLUMNS['col_' + source]}" for source in LITERAL_SOURCES)
    + " FROM range(1)"
)


class Target(BaseModel):
    """One target column: its container, its leaf type tag and its SQL type."""

    model_config = ConfigDict(frozen=True)

    container: str
    leaf: str

    @property
    def tag(self) -> str:
        """Return the target's id part, such as ``struct_ntz``."""
        return f"{self.container}_{self.leaf}"

    @property
    def sql_type(self) -> str:
        """Return the column type of the target table."""
        leaf_type = TARGETS[self.leaf]
        if self.container == "leaf":
            return leaf_type
        return SHAPES[self.container].cast_type.replace("{t}", leaf_type)

    def pack(self, value: str) -> str:
        """Return ``value`` wrapped in this target's container."""
        if self.container == "leaf":
            return value
        return SHAPES[self.container].wrap.replace("{x}", value)

    @property
    def field(self) -> str:
        """Return the leaf path a field assignment writes."""
        return "v" if self.container == "leaf" else "v.a"


class Cell(BaseModel):
    """One spelling run on one source, one target and one zone, in one row-level mode."""

    model_config = ConfigDict(frozen=True)

    cell_id: str
    spelling: Spelling
    mode: str
    source: str
    target: Target
    zone: str

    def holes(self, table: str) -> dict[str, str]:
        """Return every template hole of this cell for the target ``table``."""
        return fill_holes(self, table)

    def statement(self, template: str, table: str) -> str:
        """Return ``template`` with this cell's holes filled for ``table``."""
        return template.format(**self.holes(table))


def targets() -> list[Target]:
    """Return every target, container-major."""
    return [
        Target(container=container, leaf=leaf) for container in CONTAINERS for leaf in LEAF_TAGS
    ]


def is_column(source: str) -> bool:
    """Return whether ``source`` reads a column of the source view."""
    return source in COLUMN_SOURCES


def source_kind(source: str) -> str:
    """Return the literal kind a source holds: ``inst``, ``wall``, ``str`` or ``date``."""
    return source.removeprefix("col_")


def place_text(source: str, place: str) -> str:
    """Return the source at ``place``: ``src`` (over the view), ``bare`` or ``s`` (MERGE)."""
    if not is_column(source):
        return LITERALS[source]
    column = COLUMNS[source]
    if place == "s":
        return f"s.{column}"
    if place == "bare":
        return f"(SELECT {column} FROM {VIEW_NAME})"
    return column


def fill_holes(cell: Cell, table: str) -> dict[str, str]:
    """Return the template holes of ``cell``; see ``_write_spine_1_spellings`` for each name."""
    target = cell.target
    source = cell.source
    src = place_text(source, "src")
    value_src = target.pack(src)
    origin = f" FROM {VIEW_NAME}" if is_column(source) else ""
    if is_column(source):
        literal_using = f"(SELECT id, {COLUMNS[source]} FROM {VIEW_NAME}) s"
    else:
        literal_using = "(SELECT 1 AS id) s"
    return {
        "table": table,
        "view": VIEW_NAME,
        "target_type": target.sql_type,
        "src": src,
        "bare": place_text(source, "bare"),
        "s": place_text(source, "s"),
        "from": origin,
        "value_src": value_src,
        "value_bare": target.pack(place_text(source, "bare")),
        "value_s": target.pack(place_text(source, "s")),
        "value_c": target.pack("c"),
        "select": f"SELECT 1 AS id, {value_src} AS v, 0 AS k{origin}",
        "select_cast": (
            f"SELECT 1 AS id, CAST({value_src} AS {target.sql_type}) AS v, 0 AS k{origin}"
        ),
        "select_part": f"SELECT 1 AS id, {value_src} AS v{origin}",
        "using": f"{VIEW_NAME} s",
        "using_literal": literal_using,
        "field": target.field,
        "null": f"CAST(NULL AS {target.sql_type})",
    }


def table_columns(cell: Cell) -> str:
    """Return the column list of the target table of ``cell``."""
    columns = f"id INT, v {cell.target.sql_type}, k INT"
    if cell.spelling.sibling:
        columns += f", c {SOURCE_TYPES[source_kind(cell.source)]}"
    return columns


def modes_of(spelling: Spelling) -> tuple[str, ...]:
    """Return the row-level modes a spelling runs in."""
    return MODES_ROW_LEVEL if spelling.row_level_mode else (APPEND_MODE,)


def targets_of(spelling: Spelling) -> list[Target]:
    """Return the targets a spelling writes."""
    every = targets()
    if spelling.targets == FIELD_TARGETS:
        return [target for target in every if target.container in FIELD_CONTAINERS]
    return every


def cell_id(spelling: Spelling, mode: str, target: Target, source: str, zone: str) -> str:
    """Return the stable id ``<spelling>.<mode>.<target>.<source>.<zone tag>``."""
    return f"{spelling.name}.{mode}.{target.tag}.{source}.{ZONE_TAGS[zone]}"


def cells() -> list[Cell]:
    """Return every cell: zone-major, then spelling, mode, target and source."""
    out: list[Cell] = []
    for zone in ZONES:
        for spelling in measured_spellings():
            for mode in modes_of(spelling):
                for target in targets_of(spelling):
                    out.extend(
                        Cell(
                            cell_id=cell_id(spelling, mode, target, source, zone),
                            spelling=spelling,
                            mode=mode,
                            source=source,
                            target=target,
                            zone=zone,
                        )
                        for source in SOURCES
                    )
    identities = [cell.cell_id for cell in out]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate cell ids in the WRITE-SPINE-1 corpus")
    return out


def corpus_hash() -> str:
    """Return the SHA-256 of the view statement and every cell's id, zone, DDL and statements."""
    rows: list[object] = [VIEW_SQL]
    for cell in cells():
        spelling = cell.spelling
        rows.append(
            [
                cell.cell_id,
                cell.zone,
                spelling.reach,
                table_columns(cell),
                cell.statement(spelling.seed, "t"),
                cell.statement(spelling.template, "t"),
                list(spelling.session_conf),
            ]
        )
    payload = json.dumps(rows, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def main() -> int:
    """Print the cell count, the shape count and the corpus hash."""
    shapes = len(SOURCES) * len(targets()) * len(ZONES)
    print(f"cells={len(cells())} shapes={shapes} sha256={corpus_hash()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
