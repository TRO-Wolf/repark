from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as spark_functions

_SEED = [
    "CREATE TABLE mem.ns.c (id INT, m MAP<STRUCT<k: INT, j: INT>, INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.c VALUES "
    "(1, map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8))",
    "INSERT INTO mem.ns.c VALUES (2, map(named_struct('k', 1, 'j', 5), 9))",
]

_TWO_ENTRY_LITERAL = (
    "SELECT map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8) AS m"
)

_SPARK_ANSWERS: list[tuple[str, list[list[Any]]]] = [
    (
        "SELECT id, m[named_struct('k', 1, 'j', 2)] FROM mem.ns.c ORDER BY id",
        [[1, 7], [2, None]],
    ),
    ("SELECT id, m[named_struct('k', 3, 'j', 2)] FROM mem.ns.c WHERE id = 1", [[1, 8]]),
    (
        "SELECT id, element_at(m, named_struct('k', 1, 'j', 2)) FROM mem.ns.c ORDER BY id",
        [[1, 7], [2, None]],
    ),
    (f"SELECT m[named_struct('k', 1, 'j', 2)] FROM ({_TWO_ENTRY_LITERAL})", [[7]]),
    (f"SELECT m[named_struct('k', 3, 'j', 2)] FROM ({_TWO_ENTRY_LITERAL})", [[8]]),
    (
        "SELECT m[named_struct('k', 1, 'j', 2)] FROM "
        "(SELECT map(named_struct('k', 1, 'j', 2), 7) AS m)",
        [[7]],
    ),
]

_PANIC_MARKER = "repark internal error"


def _renamed_key() -> Any:
    return spark_functions.struct(
        spark_functions.lit(1).alias("j"), spark_functions.lit(2).alias("k")
    )


_FRAME_ANSWERS: dict[str, tuple[Callable[[Any], Any], list[list[Any]]]] = {
    "getitem_renamed_struct_key": (
        lambda frame: frame.select("id", spark_functions.col("m")[_renamed_key()]).orderBy("id"),
        [[1, 7], [2, None]],
    ),
    "getItem_renamed_struct_key": (
        lambda frame: frame.select("id", spark_functions.col("m").getItem(_renamed_key())).orderBy(
            "id"
        ),
        [[1, 7], [2, None]],
    ),
    "expr_filter_struct_key": (
        lambda frame: frame.filter(
            spark_functions.expr("m[named_struct('k', 3, 'j', 2)] = 8")
        ).select("id"),
        [[1]],
    ),
}


def _open(warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("map-struct-key-subscript-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.register_memory_catalog("mem", str(warehouse))
    session.sql("CREATE NAMESPACE mem.ns").collect()
    return session


def _newest_metadata(warehouse: Path) -> str:
    candidates = sorted(warehouse.rglob("c/metadata/*.metadata.json"))
    assert candidates, "no metadata files for the table under the warehouse"
    return str(candidates[-1])


def _reopen(warehouse: Path) -> ReparkSession:
    session = _open(warehouse)
    session.sql(
        "CALL mem.system.register_table("
        f"table => 'ns.c', metadata_file => '{_newest_metadata(warehouse)}')"
    ).collect()
    return session


def _rows(session: ReparkSession, sql: str) -> list[list[Any]]:
    try:
        table = session.sql(sql).to_arrow()
    except Exception as error:
        assert _PANIC_MARKER not in str(error), str(error)
        raise
    return [list(row.values()) for row in table.to_pylist()]


@pytest.fixture
def seeded(tmp_path: Path) -> Iterator[Path]:
    session = _open(tmp_path)
    for statement in _SEED:
        session.sql(statement).collect()
    session.stop()
    yield tmp_path


@pytest.mark.parametrize(("sql", "expected"), _SPARK_ANSWERS)
def test_a_struct_key_subscript_answers_spark_in_the_writing_session(
    tmp_path: Path, sql: str, expected: list[list[Any]]
) -> None:
    session = _open(tmp_path)
    try:
        for statement in _SEED:
            session.sql(statement).collect()
        assert _rows(session, sql) == expected, sql
    finally:
        session.stop()


@pytest.mark.parametrize(("sql", "expected"), _SPARK_ANSWERS)
def test_a_struct_key_subscript_answers_spark_in_a_fresh_session(
    seeded: Path, sql: str, expected: list[list[Any]]
) -> None:
    session = _reopen(seeded)
    try:
        assert _rows(session, sql) == expected, sql
    finally:
        session.stop()


def _frame_rows(session: ReparkSession, cell: str) -> list[list[Any]]:
    build, _ = _FRAME_ANSWERS[cell]
    try:
        table = build(session.table("mem.ns.c")).to_arrow()
    except Exception as error:
        assert _PANIC_MARKER not in str(error), str(error)
        raise
    return [list(row.values()) for row in table.to_pylist()]


@pytest.mark.parametrize("cell", sorted(_FRAME_ANSWERS))
def test_a_frame_struct_key_lookup_answers_spark_in_the_writing_session(
    tmp_path: Path, cell: str
) -> None:
    session = _open(tmp_path)
    try:
        for statement in _SEED:
            session.sql(statement).collect()
        assert _frame_rows(session, cell) == _FRAME_ANSWERS[cell][1], cell
    finally:
        session.stop()


@pytest.mark.parametrize("cell", sorted(_FRAME_ANSWERS))
def test_a_frame_struct_key_lookup_answers_spark_in_a_fresh_session(
    seeded: Path, cell: str
) -> None:
    session = _reopen(seeded)
    try:
        assert _frame_rows(session, cell) == _FRAME_ANSWERS[cell][1], cell
    finally:
        session.stop()
