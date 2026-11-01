"""RP-64 — Parquet files written without field ids read their struct children by id.

Each shape writes its rows with the facade's plain Parquet frame writer, which embeds no field
ids, admits the files into an Iceberg table with ``add_files``, evolves the struct, and reads it
in the same session and again in a fresh session that adopts the table from its newest metadata
file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import BaseModel, ConfigDict

from repark import ReparkSession

_CATALOG = "mem"
_NAMESPACE = "ns"
_TABLE = "t"
_QUALIFIED = f"{_CATALOG}.{_NAMESPACE}.{_TABLE}"
_FIELD_ID_KEY = b"PARQUET:field_id"
_CREATE_ONE_CHILD = (
    "CREATE TABLE {t} (id INT, s STRUCT<d: INT>) USING iceberg TBLPROPERTIES ('format-version'='2')"
)
_CREATE_TWO_CHILDREN = (
    "CREATE TABLE {t} (id INT, s STRUCT<d: INT, e: INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')"
)
_ONE_CHILD_FRAME = "id INT, s STRUCT<d: INT>"
_ADD_K = "ALTER TABLE {t} ADD COLUMN s.k INT"
_DROP_D = "ALTER TABLE {t} DROP COLUMN s.d"
_NULL_FREE_ROWS: tuple[tuple[int, tuple[int, ...] | None], ...] = ((1, (1,)), (2, (2,)))
_NULL_STRUCT_ROWS: tuple[tuple[int, tuple[int, ...] | None], ...] = ((1, (1,)), (2, None))
_K_ONLY = pa.struct([pa.field("k", pa.int32())])
_D_ONLY = pa.struct([pa.field("d", pa.int32())])


class _Shape(BaseModel):
    """One id-less file, one table history, and the rows RePark reads back after it."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    create: str
    frame_schema: str
    frame_rows: tuple[tuple[int, tuple[int, ...] | None], ...]
    statements: tuple[str, ...]
    rows: tuple[tuple[int, dict[str, Any] | None], ...]
    struct_type: pa.DataType


_SHAPES: dict[str, _Shape] = {
    "a_add_k_drop_d": _Shape(
        create=_CREATE_ONE_CHILD,
        frame_schema=_ONE_CHILD_FRAME,
        frame_rows=_NULL_FREE_ROWS,
        statements=(_ADD_K, _DROP_D),
        rows=((1, {"k": None}), (2, {"k": None})),
        struct_type=_K_ONLY,
    ),
    "b_add_k_drop_d_with_a_null_struct_row": _Shape(
        create=_CREATE_ONE_CHILD,
        frame_schema=_ONE_CHILD_FRAME,
        frame_rows=_NULL_STRUCT_ROWS,
        statements=(_ADD_K, _DROP_D),
        rows=((1, {"k": None}), (2, None)),
        struct_type=_K_ONLY,
    ),
    "c_add_k_alone": _Shape(
        create=_CREATE_ONE_CHILD,
        frame_schema=_ONE_CHILD_FRAME,
        frame_rows=_NULL_FREE_ROWS,
        statements=(_ADD_K,),
        rows=((1, {"d": 1, "k": None}), (2, {"d": 2, "k": None})),
        struct_type=pa.struct([pa.field("d", pa.int32()), pa.field("k", pa.int32())]),
    ),
    "d_rename_d_to_v": _Shape(
        create=_CREATE_ONE_CHILD,
        frame_schema=_ONE_CHILD_FRAME,
        frame_rows=_NULL_FREE_ROWS,
        statements=("ALTER TABLE {t} RENAME COLUMN s.d TO v",),
        rows=((1, {"v": 1}), (2, {"v": 2})),
        struct_type=pa.struct([pa.field("v", pa.int32())]),
    ),
    "e_file_children_in_another_order": _Shape(
        create=_CREATE_TWO_CHILDREN,
        frame_schema="id INT, s STRUCT<e: INT, d: INT>",
        frame_rows=((1, (10, 1)), (2, (20, 2))),
        statements=(),
        rows=((1, {"d": 1, "e": 10}), (2, {"d": 2, "e": 20})),
        struct_type=pa.struct([pa.field("d", pa.int32()), pa.field("e", pa.int32())]),
    ),
    "f_file_with_an_extra_child": _Shape(
        create=_CREATE_ONE_CHILD,
        frame_schema="id INT, s STRUCT<d: INT, x: INT>",
        frame_rows=((1, (1, 99)), (2, (2, 98))),
        statements=(),
        rows=((1, {"d": 1}), (2, {"d": 2})),
        struct_type=_D_ONLY,
    ),
    "i_every_child_dropped": _Shape(
        create=_CREATE_ONE_CHILD,
        frame_schema=_ONE_CHILD_FRAME,
        frame_rows=_NULL_STRUCT_ROWS,
        statements=(_DROP_D,),
        rows=((1, {}), (2, None)),
        struct_type=pa.struct([]),
    ),
}


class _FilterLayout(BaseModel):
    """One id-less file layout and the `(id, s)` row 2 reads before and after the evolution."""

    model_config = ConfigDict(frozen=True)

    create: str
    frame_schema: str
    frame_rows: tuple[tuple[Any, ...], ...]
    before: tuple[int, dict[str, Any]]
    after: tuple[int, dict[str, Any]]


_FILTER_LAYOUTS: dict[str, _FilterLayout] = {
    "id_first": _FilterLayout(
        create=_CREATE_ONE_CHILD,
        frame_schema=_ONE_CHILD_FRAME,
        frame_rows=_NULL_FREE_ROWS,
        before=(2, {"d": 2}),
        after=(2, {"k": None}),
    ),
    "id_after_a_two_child_struct": _FilterLayout(
        create=(
            "CREATE TABLE {t} (s STRUCT<d: INT, e: INT>, id INT) USING iceberg "
            "TBLPROPERTIES ('format-version'='2')"
        ),
        frame_schema="s STRUCT<d: INT, e: INT>, id INT",
        frame_rows=(((1, 10), 1), ((2, 20), 2)),
        before=(2, {"d": 2, "e": 20}),
        after=(2, {"e": 20, "k": None}),
    ),
}


def _open_session(test_name: str, warehouse: Path) -> ReparkSession:
    """A facade session with the memory catalog over `warehouse`."""
    session = ReparkSession.builder.appName(f"ice-idless-struct-1-{test_name}").getOrCreate()
    session.register_memory_catalog(_CATALOG, str(warehouse))
    session.sql(f"CREATE NAMESPACE {_CATALOG}.{_NAMESPACE}").collect()
    return session


def _newest_metadata(warehouse: Path) -> str:
    """The newest metadata file of the test table under `warehouse`."""
    candidates = sorted(warehouse.rglob(f"{_TABLE}/metadata/*.metadata.json"))
    assert candidates, f"no metadata files for {_TABLE} under the warehouse"
    return str(candidates[-1])


def _reopen_session(test_name: str, warehouse: Path) -> ReparkSession:
    """A fresh facade session that adopts the test table from its newest metadata file."""
    session = _open_session(f"{test_name}-fresh", warehouse)
    session.sql(
        f"CALL {_CATALOG}.system.register_table("
        f"table => '{_NAMESPACE}.{_TABLE}', metadata_file => '{_newest_metadata(warehouse)}')"
    ).collect()
    return session


def _fields_carrying_ids(field: pa.Field) -> list[str]:
    """The names under `field`, itself included, whose Parquet metadata embeds a field id."""
    carrying = [field.name] if field.metadata and _FIELD_ID_KEY in field.metadata else []
    if pa.types.is_struct(field.type):
        for child in field.type:
            carrying.extend(_fields_carrying_ids(child))
    return carrying


def _assert_written_without_ids(source: Path) -> None:
    """Every Parquet file under `source` embeds no field id at any level."""
    files = sorted(source.rglob("*.parquet"))
    assert files, "the frame writer wrote no Parquet file"
    for file in files:
        schema = pq.read_schema(file)
        carrying = [name for field in schema for name in _fields_carrying_ids(field)]
        assert carrying == [], f"{file.name} embeds field ids on {carrying}"


def _admit_frame(
    session: ReparkSession,
    source: Path,
    frame_schema: str,
    frame_rows: tuple[tuple[Any, ...], ...],
) -> None:
    """Write `frame_rows` with the plain Parquet frame writer and admit them with add_files."""
    frame = session.createDataFrame(list(frame_rows), frame_schema)
    frame.write.mode("overwrite").parquet(str(source))
    _assert_written_without_ids(source)
    session.sql(
        f"CALL {_CATALOG}.system.add_files("
        f"table => '{_NAMESPACE}.{_TABLE}', source_table => '`parquet`.`{source}`')"
    ).collect()


def _build_shape(session: ReparkSession, shape: _Shape, source: Path) -> None:
    """Create the table, admit the id-less file, and run the shape's statements."""
    session.sql(shape.create.format(t=_QUALIFIED)).collect()
    _admit_frame(session, source, shape.frame_schema, shape.frame_rows)
    for statement in shape.statements:
        session.sql(statement.format(t=_QUALIFIED)).collect()


def _assert_reads(
    session: ReparkSession,
    rows: tuple[tuple[int, dict[str, Any] | None], ...],
    struct_type: pa.DataType,
    reader: str,
) -> None:
    """`SELECT id, s`, `SELECT id` and `s IS NULL` answer `rows` on the Arrow path."""
    everything = session.sql(f"SELECT id, s FROM {_QUALIFIED} ORDER BY id").to_arrow()
    assert everything.column_names == ["id", "s"], reader
    assert everything.schema.field("s").type == struct_type, reader
    assert everything.schema.field("id").type == pa.int32(), reader
    assert [(row["id"], row["s"]) for row in everything.to_pylist()] == list(rows), reader
    ids = session.sql(f"SELECT id FROM {_QUALIFIED} ORDER BY id").to_arrow()
    assert ids.column("id").to_pylist() == [identifier for identifier, _ in rows], reader
    nulls = session.sql(f"SELECT id, s IS NULL AS absent FROM {_QUALIFIED} ORDER BY id").collect()
    assert [(row["id"], row["absent"]) for row in nulls] == [
        (identifier, value is None) for identifier, value in rows
    ], reader


def _filtered_rows(session: ReparkSession, predicate: str) -> list[tuple[Any, Any]]:
    """The `(id, s)` rows of the test table that satisfy `predicate`, ordered by id."""
    table = session.sql(f"SELECT id, s FROM {_QUALIFIED} WHERE {predicate} ORDER BY id").to_arrow()
    return [(row["id"], row["s"]) for row in table.to_pylist()]


@pytest.mark.parametrize("shape_name", sorted(_SHAPES))
def test_idless_struct_reads_by_id(tmp_path: Path, shape_name: str) -> None:
    """Every shape reads its rows in the same session and in a fresh one."""
    shape = _SHAPES[shape_name]
    warehouse = tmp_path / "wh"
    session = _open_session(shape_name, warehouse)
    try:
        _build_shape(session, shape, tmp_path / "source")
        _assert_reads(session, shape.rows, shape.struct_type, "same session")
    finally:
        session.stop()
    fresh = _reopen_session(shape_name, warehouse)
    try:
        _assert_reads(fresh, shape.rows, shape.struct_type, "fresh session")
    finally:
        fresh.stop()


def test_filter_on_the_added_child_matches_nothing_after_the_drop(tmp_path: Path) -> None:
    """`WHERE s.k = 1` matches no row once `d` is dropped, and `WHERE s.k IS NULL` matches both."""
    shape = _SHAPES["a_add_k_drop_d"]
    warehouse = tmp_path / "wh"
    session = _open_session("g", warehouse)
    try:
        _build_shape(session, shape, tmp_path / "source")
        assert _filtered_rows(session, "s.k = 1") == [], "same session"
        assert _filtered_rows(session, "s.k IS NULL") == list(shape.rows), "same session"
    finally:
        session.stop()
    fresh = _reopen_session("g", warehouse)
    try:
        assert _filtered_rows(fresh, "s.k = 1") == [], "fresh session"
        assert _filtered_rows(fresh, "s.k IS NULL") == list(shape.rows), "fresh session"
    finally:
        fresh.stop()


@pytest.mark.parametrize("layout_name", sorted(_FILTER_LAYOUTS))
def test_filter_on_an_idless_top_level_column_returns_its_row(
    tmp_path: Path, layout_name: str
) -> None:
    """`WHERE id = 2` over the id-less file returns row 2, before and after the struct evolves."""
    layout = _FILTER_LAYOUTS[layout_name]
    warehouse = tmp_path / "wh"
    session = _open_session(f"h-{layout_name}", warehouse)
    try:
        session.sql(layout.create.format(t=_QUALIFIED)).collect()
        _admit_frame(session, tmp_path / "source", layout.frame_schema, layout.frame_rows)
        assert _filtered_rows(session, "id = 2") == [layout.before], "same session"
        session.sql(_ADD_K.format(t=_QUALIFIED)).collect()
        session.sql(_DROP_D.format(t=_QUALIFIED)).collect()
        assert _filtered_rows(session, "id = 2") == [layout.after], "same session evolved"
    finally:
        session.stop()
    fresh = _reopen_session(f"h-{layout_name}", warehouse)
    try:
        assert _filtered_rows(fresh, "id = 2") == [layout.after], "fresh session"
    finally:
        fresh.stop()
