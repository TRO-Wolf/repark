"""WRITE-SPINE-1 step 3b: render a census cell and read the door it takes, statically and live.

NOT a ``test_`` module. A cell renders through the step-0 recorder's own functions, so the
statement a door test runs is the statement the fixtures recorded. The static door is
``repark._native.write_door_of`` on that text. The runtime doors are
``repark._native.last_write_doors`` on the facade session right after the cell's write.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _record_write_spine_1 as recorder
import _write_spine_1_shapes as shapes
import _write_spine_1_spellings as corpus

from repark import ReparkSession, _native

HERE = Path(__file__).resolve().parent
MAIN_FIXTURE = HERE / "write_spine_1_main_7a789fc5.json"
SPARK_FIXTURE = recorder.ORACLE_FILE
HEAD_FIXTURE_GLOB = "write_spine_1_head_*.json"
SELECT_SQL = f"SELECT id FROM {shapes.VIEW_NAME}"


def load_cells(path: Path) -> dict[str, dict[str, Any]]:
    """Return the ``cells`` map of one recorded fixture document."""
    return json.loads(path.read_text(encoding="utf-8"))["cells"]


def cells_by_id() -> dict[str, shapes.Cell]:
    """Return every census cell keyed by its id."""
    return {cell.cell_id: cell for cell in shapes.cells()}


def cells_of_reach(reach: str) -> list[shapes.Cell]:
    """Return the census cells whose spelling has ``reach``, in census order."""
    return [cell for cell in shapes.cells() if cell.spelling.reach == reach]


def table_of(cell: shapes.Cell) -> str:
    """Return the target table the recorder names for ``cell``."""
    return recorder.table_name(cell)


def render_statement(cell: shapes.Cell) -> str:
    """Return the cell's spelling template filled for its recorder table."""
    return cell.statement(cell.spelling.template, table_of(cell))


def static_door(cell: shapes.Cell) -> str | None:
    """Return the door ``write_door_of`` reads from the cell's rendered statement."""
    return _native.write_door_of(render_statement(cell), None)


def build_facade_session(zone: str, warehouse: Path) -> ReparkSession:
    """Return the recorder's facade session for ``zone``: ANSI on, a memory catalog, the view.

    The recorder also pins the process ``TZ``; this builder does not, because a door record
    does not read the process zone and a test process must keep its environment.
    """
    session = (
        ReparkSession.builder.appName("write-spine-1-door-matrix")
        .config("spark.sql.ansi.enabled", "true")
        .config(recorder.SESSION_ZONE_KEY, zone)
        .getOrCreate()
    )
    session.register_memory_catalog(recorder.CATALOG, str(warehouse))
    session.sql(f"CREATE NAMESPACE {recorder.CATALOG}.{recorder.NAMESPACE}").collect()
    session.sql(shapes.VIEW_SQL).collect()
    return session


def recorded_doors(session: ReparkSession) -> list[str]:
    """Return the doors the session's last statement recorded."""
    return _native.last_write_doors(session._ensure_alive())


def run_write(door: recorder.FacadeDoor, cell: shapes.Cell, table: str) -> None:
    """Run the cell's write step; a write error leaves the record the write made."""
    try:
        if cell.spelling.reach == corpus.DATAFRAME:
            door.write_frame(cell, table)
        else:
            door.run(cell.statement(cell.spelling.template, table))
    except Exception:
        return


def runtime_doors(session: ReparkSession, cell: shapes.Cell) -> list[str]:
    """Create, seed and write the cell's table on the facade session; return the write's doors."""
    door = recorder.FacadeDoor(session)
    table = table_of(cell)
    spelling = cell.spelling
    try:
        if not spelling.creates_table:
            door.create(cell, table)
        if spelling.seed:
            door.run(cell.statement(spelling.seed, table))
        run_write(door, cell, table)
        return recorded_doors(session)
    finally:
        door.drop(table)


def doors_after_select(session: ReparkSession) -> list[str]:
    """Run a SELECT on the facade session and return the doors it recorded."""
    session.sql(SELECT_SQL).collect()
    return recorded_doors(session)


def sample(cells: list[shapes.Cell], per_class: int) -> list[shapes.Cell]:
    """Return ``per_class`` cells per spelling, mode and target, rotating the zone and source.

    The ``i``-th pick takes zone ``i mod 3`` and source ``i mod 8``; the two counts share no
    factor, so every 24 consecutive picks cover each zone and source pair once.
    """
    groups: dict[tuple[str, str, str], dict[tuple[str, str], shapes.Cell]] = {}
    for cell in cells:
        key = (cell.spelling.name, cell.mode, cell.target.tag)
        groups.setdefault(key, {})[(cell.zone, cell.source)] = cell
    picked: list[shapes.Cell] = []
    index = 0
    for group in groups.values():
        for _ in range(per_class):
            zone = shapes.ZONES[index % len(shapes.ZONES)]
            source = shapes.SOURCES[index % len(shapes.SOURCES)]
            picked.append(group[(zone, source)])
            index += 1
    return picked
