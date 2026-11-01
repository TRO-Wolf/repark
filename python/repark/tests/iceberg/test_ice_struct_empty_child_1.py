"""RP-63 — a struct emptied by column drops reads back per row through the Spark door.

Each shape builds its table on the facade, drops the struct's children, and reads it in the
same session and again in a fresh session that adopts the table from its newest metadata file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pydantic import BaseModel, ConfigDict

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException, PySparkException

_CATALOG = "mem"
_NAMESPACE = "ns"
_TABLE = "t"
_QUALIFIED = f"{_CATALOG}.{_NAMESPACE}.{_TABLE}"
_CREATE_ONE_CHILD = (
    "CREATE TABLE {t} (id INT, s STRUCT<k: INT>) USING iceberg TBLPROPERTIES ('format-version'='2')"
)
_CREATE_TWO_CHILDREN = (
    "CREATE TABLE {t} (id INT, s STRUCT<k: INT, j: INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')"
)
_CREATE_ID_ONLY = "CREATE TABLE {t} (id INT) USING iceberg TBLPROPERTIES ('format-version'='2')"
_DROP_K = "ALTER TABLE {t} DROP COLUMN s.k"
_EMPTY_STRUCT = pa.struct([])
_EMPTY_STRUCT_WRITE_REFUSAL = "Parquet does not support writing empty structs"


class _Shape(BaseModel):
    """One table history and the rows RePark reads back after it."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    statements: tuple[str, ...]
    rows: tuple[tuple[int, dict[str, Any] | None], ...]
    struct_type: pa.DataType = _EMPTY_STRUCT


_SHAPES: dict[str, _Shape] = {
    "a_one_child_dropped": _Shape(
        statements=(
            _CREATE_ONE_CHILD,
            "INSERT INTO {t} SELECT 1, named_struct('k', 10)",
            _DROP_K,
        ),
        rows=((1, {}),),
    ),
    "b_two_children_dropped_one_at_a_time": _Shape(
        statements=(
            _CREATE_TWO_CHILDREN,
            "INSERT INTO {t} SELECT 1, named_struct('k', 10, 'j', 20)",
            _DROP_K,
            "ALTER TABLE {t} DROP COLUMN s.j",
        ),
        rows=((1, {}),),
    ),
    "c_one_file_with_a_null_struct_row": _Shape(
        statements=(
            _CREATE_ONE_CHILD,
            "INSERT INTO {t} VALUES (1, named_struct('k', 10)), (2, NULL)",
            _DROP_K,
        ),
        rows=((1, {}), (2, None)),
    ),
    "e_struct_added_after_the_rows": _Shape(
        statements=(
            _CREATE_ID_ONLY,
            "INSERT INTO {t} VALUES (1), (2)",
            "ALTER TABLE {t} ADD COLUMN s STRUCT<k: INT>",
        ),
        rows=((1, None), (2, None)),
        struct_type=pa.struct([pa.field("k", pa.int32())]),
    ),
    "e_struct_added_after_the_rows_then_emptied": _Shape(
        statements=(
            _CREATE_ID_ONLY,
            "INSERT INTO {t} VALUES (1), (2)",
            "ALTER TABLE {t} ADD COLUMN s STRUCT<k: INT>",
            _DROP_K,
        ),
        rows=((1, None), (2, None)),
    ),
    "e_struct_added_then_written_then_emptied": _Shape(
        statements=(
            _CREATE_ID_ONLY,
            "INSERT INTO {t} VALUES (1), (2)",
            "ALTER TABLE {t} ADD COLUMN s STRUCT<k: INT>",
            "INSERT INTO {t} VALUES (3, named_struct('k', 30))",
            _DROP_K,
        ),
        rows=((1, None), (2, None), (3, {})),
    ),
    "f_two_files_with_a_null_struct_row": _Shape(
        statements=(
            _CREATE_ONE_CHILD,
            "INSERT INTO {t} VALUES (1, named_struct('k', 10))",
            "INSERT INTO {t} VALUES (2, named_struct('k', 20)), (3, NULL)",
            _DROP_K,
        ),
        rows=((1, {}), (2, {}), (3, None)),
    ),
    "g_one_file_with_a_null_child_row": _Shape(
        statements=(
            _CREATE_ONE_CHILD,
            "INSERT INTO {t} VALUES (1, named_struct('k', 10)), "
            "(2, named_struct('k', CAST(NULL AS INT)))",
            _DROP_K,
        ),
        rows=((1, {}), (2, {})),
    ),
}

_APPEND_AFTER_DROP: tuple[tuple[str, type[PySparkException], str], ...] = (
    (
        "INSERT INTO {t} SELECT 2, named_struct()",
        AnalysisException,
        "'named_struct' does not support zero arguments",
    ),
    ("INSERT INTO {t} SELECT 3, struct()", ParseException, "Expected: an expression"),
    ("INSERT INTO {t} VALUES (4, NULL)", PySparkException, _EMPTY_STRUCT_WRITE_REFUSAL),
    ("INSERT INTO {t} (id) VALUES (5)", PySparkException, _EMPTY_STRUCT_WRITE_REFUSAL),
)


def _open_session(test_name: str, warehouse: Path) -> ReparkSession:
    """A facade session with the memory catalog over `warehouse`."""
    session = ReparkSession.builder.appName(f"ice-struct-empty-child-1-{test_name}").getOrCreate()
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


def _assert_reads(
    session: ReparkSession,
    rows: tuple[tuple[int, dict[str, Any] | None], ...],
    struct_type: pa.DataType,
    reader: str,
) -> None:
    """`SELECT *`, `SELECT id` and `s IS NULL` answer `rows` on the Arrow path."""
    everything = session.sql(f"SELECT * FROM {_QUALIFIED} ORDER BY id").to_arrow()
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


@pytest.mark.parametrize("shape_name", sorted(_SHAPES))
def test_emptied_struct_reads_per_row(tmp_path: Path, shape_name: str) -> None:
    """Every shape reads its rows in the same session and in a fresh one, with no panic."""
    shape = _SHAPES[shape_name]
    warehouse = tmp_path / "wh"
    session = _open_session(shape_name, warehouse)
    try:
        for statement in shape.statements:
            session.sql(statement.format(t=_QUALIFIED)).collect()
        _assert_reads(session, shape.rows, shape.struct_type, "same session")
    finally:
        session.stop()
    fresh = _reopen_session(shape_name, warehouse)
    try:
        _assert_reads(fresh, shape.rows, shape.struct_type, "fresh session")
    finally:
        fresh.stop()


def test_append_after_the_drop_refuses_and_keeps_the_pre_drop_row(tmp_path: Path) -> None:
    """Every append spelling into the emptied struct refuses; the pre-drop row reads alone."""
    warehouse = tmp_path / "wh"
    session = _open_session("append", warehouse)
    try:
        session.sql(_CREATE_ONE_CHILD.format(t=_QUALIFIED)).collect()
        session.sql(f"INSERT INTO {_QUALIFIED} VALUES (1, named_struct('k', 10))").collect()
        session.sql(_DROP_K.format(t=_QUALIFIED)).collect()
        for statement, error_class, message in _APPEND_AFTER_DROP:
            with pytest.raises(error_class, match=message):
                session.sql(statement.format(t=_QUALIFIED)).collect()
            _assert_reads(session, ((1, {}),), _EMPTY_STRUCT, f"same session after {statement}")
        files = session.sql(f"SELECT count(*) AS files FROM {_QUALIFIED}.files").collect()
        assert files[0]["files"] == 1
    finally:
        session.stop()
    fresh = _reopen_session("append", warehouse)
    try:
        _assert_reads(fresh, ((1, {}),), _EMPTY_STRUCT, "fresh session")
    finally:
        fresh.stop()
