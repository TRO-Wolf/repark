"""RP-64 fix round 1 — an id-less file admitted after the schema evolves reads its values.

Each history writes its rows with the facade's plain Parquet frame writer, which embeds no field
ids, admits them with ``add_files``, renames, adds or drops a column, and admits another id-less
file named after the evolved schema. The schema update keeps ``schema.name-mapping.default`` in
step with the table, so the later file's columns map to their field ids. Every read runs in the
same session and again in a fresh session that adopts the table from its newest metadata file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pydantic import BaseModel, ConfigDict
from test_ice_idless_struct_1 import (
    _QUALIFIED,
    _admit_frame,
    _open_session,
    _reopen_session,
)

from repark import ReparkSession

_MAPPING_KEY = "schema.name-mapping.default"
_V2 = "USING iceberg TBLPROPERTIES ('format-version'='2')"


class _Step(BaseModel):
    """One statement, or one id-less frame admitted with add_files."""

    model_config = ConfigDict(frozen=True)

    statement: str | None = None
    frame_schema: str | None = None
    frame_rows: tuple[tuple[Any, ...], ...] = ()


class _Read(BaseModel):
    """One query over the test table and the rows it answers."""

    model_config = ConfigDict(frozen=True)

    query: str
    rows: tuple[tuple[Any, ...], ...]


class _History(BaseModel):
    """A table history, the rows it reads, and the names its mapping holds per field id."""

    model_config = ConfigDict(frozen=True)

    create: str
    steps: tuple[_Step, ...]
    reads: tuple[_Read, ...]
    mapped: dict[int, tuple[str, ...]]


def _sql(statement: str) -> _Step:
    """A statement step."""
    return _Step(statement=statement)


def _file(frame_schema: str, *frame_rows: tuple[Any, ...]) -> _Step:
    """An id-less frame step."""
    return _Step(frame_schema=frame_schema, frame_rows=frame_rows)


def _read(query: str, *rows: tuple[Any, ...]) -> _Read:
    """A query and the rows it answers."""
    return _Read(query=query, rows=rows)


_SELECT_S = "SELECT id, s FROM {t} ORDER BY id"
_TOP_X_AFTER_DROP_RENAME = (
    _sql("ALTER TABLE {t} DROP COLUMN x"),
    _sql("ALTER TABLE {t} RENAME COLUMN y TO x"),
)
_NESTED_X_AFTER_DROP_RENAME = (
    _sql("ALTER TABLE {t} DROP COLUMN s.x"),
    _sql("ALTER TABLE {t} RENAME COLUMN s.y TO x"),
)

_HISTORIES: dict[str, _History] = {
    "j_rename_child_then_a_file_named_after_it": _History(
        create=f"CREATE TABLE {{t}} (id INT, s STRUCT<d: INT>) {_V2}",
        steps=(
            _file("id INT, s STRUCT<d: INT>", (1, (1,))),
            _sql("ALTER TABLE {t} RENAME COLUMN s.d TO v"),
            _file("id INT, s STRUCT<v: INT>", (2, (2,))),
        ),
        reads=(
            _read(_SELECT_S, (1, {"v": 1}), (2, {"v": 2})),
            _read("SELECT id FROM {t} WHERE s.v = 2 ORDER BY id", (2,)),
        ),
        mapped={3: ("d", "v")},
    ),
    "k_add_child_then_files_with_and_without_it": _History(
        create=f"CREATE TABLE {{t}} (id INT, s STRUCT<d: INT>) {_V2}",
        steps=(
            _file("id INT, s STRUCT<d: INT>", (1, (1,))),
            _sql("ALTER TABLE {t} ADD COLUMN s.k INT"),
            _file("id INT, s STRUCT<d: INT, k: INT>", (2, (2, 22))),
            _file("id INT, s STRUCT<d: INT>", (3, (3,))),
        ),
        reads=(
            _read(
                _SELECT_S,
                (1, {"d": 1, "k": None}),
                (2, {"d": 2, "k": 22}),
                (3, {"d": 3, "k": None}),
            ),
            _read("SELECT id FROM {t} WHERE s.k = 22 ORDER BY id", (2,)),
        ),
        mapped={3: ("d",), 4: ("k",)},
    ),
    "l_add_top_level_column_then_a_file_with_it": _History(
        create=f"CREATE TABLE {{t}} (id INT, x INT) {_V2}",
        steps=(
            _file("id INT, x INT", (1, 10)),
            _sql("ALTER TABLE {t} ADD COLUMN y INT"),
            _file("id INT, x INT, y INT", (2, 20, 7)),
        ),
        reads=(
            _read("SELECT id, x, y FROM {t} ORDER BY id", (1, 10, None), (2, 20, 7)),
            _read("SELECT id FROM {t} WHERE y = 7 ORDER BY id", (2,)),
        ),
        mapped={2: ("x",), 3: ("y",)},
    ),
    "m_drop_then_add_by_the_same_name": _History(
        create=f"CREATE TABLE {{t}} (id INT, s STRUCT<a: INT, b: INT>, c INT) {_V2}",
        steps=(
            _file("id INT, s STRUCT<a: INT, b: INT>, c INT", (1, (1, 2), 5)),
            _sql("ALTER TABLE {t} DROP COLUMN s.b"),
            _sql("ALTER TABLE {t} ADD COLUMN s.b INT"),
            _sql("ALTER TABLE {t} DROP COLUMN c"),
            _sql("ALTER TABLE {t} ADD COLUMN c INT"),
        ),
        reads=(_read("SELECT id, s, c FROM {t} ORDER BY id", (1, {"a": 1, "b": 2}, 5)),),
        mapped={3: (), 4: ("a",), 5: (), 6: ("b",), 7: ("c",)},
    ),
    "o_top_level_rename_onto_a_dropped_name": _History(
        create=f"CREATE TABLE {{t}} (id INT, x INT, y INT) {_V2}",
        steps=(_file("id INT, x INT, y INT", (1, 1, 2)), *_TOP_X_AFTER_DROP_RENAME),
        reads=(
            _read("SELECT id, x FROM {t} ORDER BY id", (1, 2)),
            _read("SELECT id FROM {t} WHERE x = 2 ORDER BY id", (1,)),
            _read("SELECT id FROM {t} WHERE x = 1 ORDER BY id"),
        ),
        mapped={2: (), 3: ("x", "y")},
    ),
    "o_top_level_rename_onto_a_dropped_name_then_a_file": _History(
        create=f"CREATE TABLE {{t}} (id INT, x INT, y INT) {_V2}",
        steps=(
            _file("id INT, x INT, y INT", (1, 1, 2)),
            *_TOP_X_AFTER_DROP_RENAME,
            _file("id INT, x INT", (2, 9)),
        ),
        reads=(
            _read("SELECT id, x FROM {t} ORDER BY id", (1, 2), (2, 9)),
            _read("SELECT id FROM {t} WHERE x = 9 ORDER BY id", (2,)),
            _read("SELECT id FROM {t} WHERE x = 2 ORDER BY id", (1,)),
            _read("SELECT id FROM {t} WHERE x = 1 ORDER BY id"),
        ),
        mapped={2: (), 3: ("x", "y")},
    ),
    "o_nested_rename_onto_a_dropped_name": _History(
        create=f"CREATE TABLE {{t}} (id INT, s STRUCT<x: INT, y: INT>) {_V2}",
        steps=(
            _file("id INT, s STRUCT<x: INT, y: INT>", (1, (1, 2))),
            *_NESTED_X_AFTER_DROP_RENAME,
        ),
        reads=(
            _read(_SELECT_S, (1, {"x": 2})),
            _read("SELECT id FROM {t} WHERE s.x = 2 ORDER BY id", (1,)),
            _read("SELECT id FROM {t} WHERE s.x = 1 ORDER BY id"),
        ),
        mapped={3: (), 4: ("x", "y")},
    ),
    "o_nested_rename_onto_a_dropped_name_then_a_file": _History(
        create=f"CREATE TABLE {{t}} (id INT, s STRUCT<x: INT, y: INT>) {_V2}",
        steps=(
            _file("id INT, s STRUCT<x: INT, y: INT>", (1, (1, 2))),
            *_NESTED_X_AFTER_DROP_RENAME,
            _file("id INT, s STRUCT<x: INT>", (2, (9,))),
        ),
        reads=(
            _read(_SELECT_S, (1, {"x": 2}), (2, {"x": 9})),
            _read("SELECT id FROM {t} WHERE s.x = 9 ORDER BY id", (2,)),
            _read("SELECT id FROM {t} WHERE s.x = 2 ORDER BY id", (1,)),
            _read("SELECT id FROM {t} WHERE s.x = 1 ORDER BY id"),
        ),
        mapped={3: (), 4: ("x", "y")},
    ),
}


def _answer(session: ReparkSession, query: str) -> list[tuple[Any, ...]]:
    """The rows `query` answers over the test table, structs as dicts."""
    table = session.sql(query.format(t=_QUALIFIED)).to_arrow()
    return [tuple(row.values()) for row in table.to_pylist()]


def _mapping_text(session: ReparkSession) -> str:
    """The table's `schema.name-mapping.default` property as SHOW TBLPROPERTIES answers it."""
    shown = session.sql(f"SHOW TBLPROPERTIES {_QUALIFIED} ('{_MAPPING_KEY}')").collect()
    assert [row["key"] for row in shown] == [_MAPPING_KEY]
    return str(shown[0]["value"])


def _collect_names(fields: list[dict[str, Any]], names: dict[int, tuple[str, ...]]) -> None:
    """Record the sorted names of every mapped field under `fields`, at every depth."""
    for field in fields:
        names[int(field["field-id"])] = tuple(sorted(field.get("names", [])))
        _collect_names(field.get("fields", []), names)


def _mapped_names(session: ReparkSession) -> dict[int, tuple[str, ...]]:
    """The sorted names the table's name mapping holds for each field id."""
    names: dict[int, tuple[str, ...]] = {}
    _collect_names(json.loads(_mapping_text(session)), names)
    return names


def _build_history(session: ReparkSession, history: _History, sources: Path) -> None:
    """Create the table and run every step, each frame from its own source directory."""
    session.sql(history.create.format(t=_QUALIFIED)).collect()
    for index, step in enumerate(history.steps):
        if step.statement is not None:
            session.sql(step.statement.format(t=_QUALIFIED)).collect()
        else:
            assert step.frame_schema is not None
            _admit_frame(session, sources / f"file-{index}", step.frame_schema, step.frame_rows)


def _assert_history(session: ReparkSession, history: _History, reader: str) -> None:
    """Every read answers its rows and the mapping holds the listed names."""
    for read in history.reads:
        assert _answer(session, read.query) == list(read.rows), f"{reader}: {read.query}"
    mapped = _mapped_names(session)
    for field_id, names in history.mapped.items():
        assert mapped.get(field_id) == names, f"{reader}: field {field_id} in {mapped}"


@pytest.mark.parametrize("history_name", sorted(_HISTORIES))
def test_late_idless_file_reads_its_values(tmp_path: Path, history_name: str) -> None:
    """Every history reads its rows and mapping in the same session and in a fresh one."""
    history = _HISTORIES[history_name]
    warehouse = tmp_path / "wh"
    session = _open_session(f"late-{history_name}", warehouse)
    try:
        _build_history(session, history, tmp_path / "sources")
        _assert_history(session, history, "same session")
    finally:
        session.stop()
    fresh = _reopen_session(f"late-{history_name}", warehouse)
    try:
        _assert_history(fresh, history, "fresh session")
    finally:
        fresh.stop()


def _struct_type(session: ReparkSession) -> pa.DataType:
    """The Arrow type of `s` as the test table reads it."""
    return session.sql(f"SELECT s FROM {_QUALIFIED}").to_arrow().schema.field("s").type


_MOVED = pa.struct([pa.field("e", pa.int32()), pa.field("d", pa.int32())])
_PROMOTED = pa.struct([pa.field("e", pa.int32()), pa.field("d", pa.int64())])
_CONTROL_READS = (
    _read(_SELECT_S, (1, {"d": 1, "e": 10}), (2, {"d": 2, "e": 20})),
    _read("SELECT id FROM {t} WHERE s.d = 2 ORDER BY id", (2,)),
)


def _assert_control(session: ReparkSession, mapping: str, reader: str) -> None:
    """The mapping property is byte-equal to `mapping` and the reads keep their values."""
    assert _mapping_text(session) == mapping, reader
    for read in _CONTROL_READS:
        assert _answer(session, read.query) == list(read.rows), f"{reader}: {read.query}"


def test_promotion_and_move_leave_the_mapping_and_the_reads_unchanged(tmp_path: Path) -> None:
    """A child move and an int to bigint promotion keep the mapping bytes and the values."""
    warehouse = tmp_path / "wh"
    session = _open_session("late-n", warehouse)
    try:
        session.sql(f"CREATE TABLE {_QUALIFIED} (id INT, s STRUCT<d: INT, e: INT>) {_V2}").collect()
        _admit_frame(
            session,
            tmp_path / "source",
            "id INT, s STRUCT<d: INT, e: INT>",
            ((1, (1, 10)), (2, (2, 20))),
        )
        mapping = _mapping_text(session)
        _assert_control(session, mapping, "admitted")
        session.sql(f"ALTER TABLE {_QUALIFIED} ALTER COLUMN s.e FIRST").collect()
        assert _struct_type(session) == _MOVED
        _assert_control(session, mapping, "moved")
        session.sql(f"ALTER TABLE {_QUALIFIED} ALTER COLUMN s.d TYPE BIGINT").collect()
        assert _struct_type(session) == _PROMOTED
        _assert_control(session, mapping, "promoted")
    finally:
        session.stop()
    fresh = _reopen_session("late-n", warehouse)
    try:
        assert _struct_type(fresh) == _PROMOTED
        _assert_control(fresh, mapping, "fresh session")
    finally:
        fresh.stop()
