from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.spark import functions as spark_functions

_SEED = [
    "CREATE TABLE mem.ns.c (id INT, m MAP<STRUCT<k: INT, j: INT>, INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.c VALUES "
    "(1, map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8))",
    "INSERT INTO mem.ns.c VALUES (2, map(named_struct('k', 1, 'j', 5), 9))",
    "CREATE TABLE mem.ns.sc (id INT, mi MAP<INT, STRING>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.sc VALUES (1, map(1, 'one', 2, 'two')), (2, map(3, 'three'))",
]

_TABLES = ("c", "sc")

_PANIC_MARKER = "repark internal error"

_SELECT_ANSWERS: dict[str, tuple[Callable[[ReparkSession], Any], list[list[Any]]]] = {
    "expr_select_struct_key": (
        lambda session: (
            session.table("mem.ns.c")
            .select("id", spark_functions.expr("m[named_struct('j', 1, 'k', 2)]"))
            .orderBy("id")
        ),
        [[1, 7], [2, None]],
    ),
    "expr_select_int_key": (
        lambda session: (
            session.table("mem.ns.sc").select("id", spark_functions.expr("mi[1]")).orderBy("id")
        ),
        [[1, "one"], [2, None]],
    ),
}

_LIST_FAMILY_ANSWERS: list[tuple[str, list[list[Any]]]] = [
    ("SELECT id, ms[array(named_struct('j', 1, 'k', 2))] FROM lf ORDER BY id", [[1, 7], [2, None]]),
    (
        "SELECT id, ms[array(named_struct('j', 2, 'k', 1))] FROM lf ORDER BY id",
        [[1, None], [2, None]],
    ),
    ("SELECT id, fsm[fk] FROM lf ORDER BY id", [[1, 7], [2, 8]]),
    ("SELECT id, fsm[array(1, 2)] FROM lf ORDER BY id", [[1, 7], [2, None]]),
    ("SELECT id, m[array(1, 2)] FROM lf ORDER BY id", [[1, 7], [2, None]]),
    ("SELECT id, m[array(1L, 2L)] FROM lf ORDER BY id", [[1, 7], [2, None]]),
    ("SELECT id, lm[fk] FROM lf ORDER BY id", [[1, 7], [2, None]]),
    ("SELECT id, lm[lk] FROM lf ORDER BY id", [[1, 7], [2, None]]),
    ("SELECT id, ls_m[fs_s] FROM lf ORDER BY id", [[1, None], [2, 9]]),
]


def _open(warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("map-struct-key-subscript-2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", str(warehouse))
    session.sql("CREATE NAMESPACE mem.ns").collect()
    return session


def _newest_metadata(warehouse: Path, table: str) -> str:
    candidates = sorted(warehouse.rglob(f"{table}/metadata/*.metadata.json"))
    assert candidates, f"no metadata files for {table} under the warehouse"
    return str(candidates[-1])


def _reopen(warehouse: Path) -> ReparkSession:
    session = _open(warehouse)
    for table in _TABLES:
        session.sql(
            "CALL mem.system.register_table("
            f"table => 'ns.{table}', metadata_file => '{_newest_metadata(warehouse, table)}')"
        ).collect()
    return session


def _arrow_rows(build: Callable[[], Any]) -> list[list[Any]]:
    try:
        table = build().to_arrow()
    except Exception as error:
        assert _PANIC_MARKER not in str(error), str(error)
        raise
    return [[column[index].as_py() for column in table.columns] for index in range(table.num_rows)]


def _write_list_family_file(directory: Path) -> Path:
    struct_key = pa.struct([("k", pa.int32()), ("j", pa.int32())])
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int32()),
            "m": pa.array(
                [[([1, 2], 7), ([3], 8)], [([5], 9)]],
                pa.map_(pa.large_list(pa.int32()), pa.int32()),
            ),
            "ms": pa.array(
                [[([{"k": 1, "j": 2}], 7)], [([{"k": 5, "j": 6}], 9)]],
                pa.map_(pa.large_list(struct_key), pa.int32()),
            ),
            "fk": pa.array([[1, 2], [3, 4]], pa.list_(pa.int32(), 2)),
            "lk": pa.array([[1, 2], [3]], pa.large_list(pa.int32())),
            "fsm": pa.array(
                [[([1, 2], 7)], [([3, 4], 8)]], pa.map_(pa.list_(pa.int32(), 2), pa.int32())
            ),
            "lm": pa.array(
                [[([1, 2], 7), ([3], 8)], [([5], 9)]], pa.map_(pa.list_(pa.int32()), pa.int32())
            ),
            "fs_s": pa.array(
                [[{"k": 1, "j": 2}], [{"k": 3, "j": 4}]],
                pa.list_(pa.struct([("j", pa.int32()), ("k", pa.int32())]), 1),
            ),
            "ls_m": pa.array(
                [[([{"k": 1, "j": 2}], 7)], [([{"k": 4, "j": 3}], 9)]],
                pa.map_(pa.list_(struct_key), pa.int32()),
            ),
        }
    )
    path = directory / "list_family.parquet"
    pq.write_table(table, path)
    return path


@pytest.fixture
def seeded(tmp_path: Path) -> Iterator[Path]:
    session = _open(tmp_path)
    for statement in _SEED:
        session.sql(statement).collect()
    session.stop()
    yield tmp_path


@pytest.mark.parametrize("cell", sorted(_SELECT_ANSWERS))
def test_an_expr_map_subscript_in_a_select_answers_spark_in_the_writing_session(
    tmp_path: Path, cell: str
) -> None:
    build, expected = _SELECT_ANSWERS[cell]
    session = _open(tmp_path)
    try:
        for statement in _SEED:
            session.sql(statement).collect()
        assert _arrow_rows(lambda: build(session)) == expected, cell
    finally:
        session.stop()


@pytest.mark.parametrize("cell", sorted(_SELECT_ANSWERS))
def test_an_expr_map_subscript_in_a_select_answers_spark_in_a_fresh_session(
    seeded: Path, cell: str
) -> None:
    build, expected = _SELECT_ANSWERS[cell]
    session = _reopen(seeded)
    try:
        assert _arrow_rows(lambda: build(session)) == expected, cell
    finally:
        session.stop()


@pytest.mark.parametrize(("sql", "expected"), _LIST_FAMILY_ANSWERS)
def test_a_list_family_map_key_answers_spark_over_a_parquet_file(
    tmp_path: Path, sql: str, expected: list[list[Any]]
) -> None:
    path = _write_list_family_file(tmp_path)
    session = _open(tmp_path / "wh")
    try:
        session.read.parquet(str(path)).createOrReplaceTempView("lf")
        assert _arrow_rows(lambda: session.sql(sql)) == expected, sql
    finally:
        session.stop()
