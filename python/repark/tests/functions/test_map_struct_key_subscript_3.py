from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

_SEED = [
    "CREATE TABLE mem.ns.r3sd (id INT, m MAP<STRUCT<k: DOUBLE, j: INT>, INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.r3sd VALUES (1, map(named_struct('k', CAST(0 AS DOUBLE), 'j', 2), 7, "
    "named_struct('k', CAST('NaN' AS DOUBLE), 'j', 2), 8)), "
    "(2, map(named_struct('k', CAST(1 AS DOUBLE), 'j', 2), 9))",
    "CREATE TABLE mem.ns.r3ad (id INT, m MAP<ARRAY<DOUBLE>, INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.r3ad VALUES (1, map(array(CAST(0 AS DOUBLE)), 7, "
    "array(CAST('NaN' AS DOUBLE)), 8)), (2, map(array(CAST(1 AS DOUBLE)), 9))",
    "CREATE TABLE mem.ns.r3tn (id INT, m MAP<STRUCT<t: TIMESTAMP_NTZ, j: INT>, INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.r3tn VALUES (1, map(named_struct('t', TIMESTAMP_NTZ '2024-01-01 00:00:00', "
    "'j', 2), 7, named_struct('t', TIMESTAMP_NTZ '2024-01-01 05:00:00', 'j', 2), 8)), "
    "(2, map(named_struct('t', TIMESTAMP_NTZ '2024-01-02 00:00:00', 'j', 2), 9, "
    "named_struct('t', TIMESTAMP_NTZ '2024-01-02 05:00:00', 'j', 2), 10))",
]

_TABLES = ("r3sd", "r3ad", "r3tn")

_ZONE = "America/New_York"

_PANIC_MARKER = "repark internal error"

_SPARK_ANSWERS: list[tuple[str, list[list[Any]]]] = [
    (
        "SELECT id, m[named_struct('k', CAST('-0.0' AS DOUBLE), 'j', 2)] FROM mem.ns.r3sd "
        "ORDER BY id",
        [[1, 7], [2, None]],
    ),
    (
        "SELECT id, m[named_struct('x', sqrt(CAST(-1 AS DOUBLE)), 'y', 2)] FROM mem.ns.r3sd "
        "ORDER BY id",
        [[1, 8], [2, None]],
    ),
    (
        "SELECT id, m[array(CAST('-0.0' AS DOUBLE))] FROM mem.ns.r3ad ORDER BY id",
        [[1, 7], [2, None]],
    ),
    (
        "SELECT id, m[array(sqrt(CAST(-1 AS DOUBLE)))] FROM mem.ns.r3ad ORDER BY id",
        [[1, 8], [2, None]],
    ),
    (
        "SELECT map(array(CAST(0 AS DOUBLE)), 1, array(CAST('NaN' AS DOUBLE)), 2)"
        "[array(sqrt(CAST(-1 AS DOUBLE)))]",
        [[2]],
    ),
    (
        "SELECT id, m[named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2)] FROM mem.ns.r3tn "
        "ORDER BY id",
        [[1, 7], [2, None]],
    ),
    (
        "SELECT id, m[named_struct('t', CAST(date_add(DATE '2023-12-31', id) AS TIMESTAMP), "
        "'j', 2)] FROM mem.ns.r3tn ORDER BY id",
        [[1, 7], [2, 9]],
    ),
    (
        "SELECT id, m[named_struct('t', DATE '2024-01-01', 'j', 2)] FROM mem.ns.r3tn ORDER BY id",
        [[1, 7], [2, None]],
    ),
    (
        "SELECT map(named_struct('t', TIMESTAMP_NTZ '2024-01-01 00:00:00', 'j', 2), 7, "
        "named_struct('t', TIMESTAMP_NTZ '2024-01-01 05:00:00', 'j', 2), 8)"
        "[named_struct('t', TIMESTAMP '2024-01-01 00:00:00', 'j', 2)]",
        [[7]],
    ),
]


def _open(warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("map-struct-key-subscript-3")
        .config("spark.sql.session.timeZone", _ZONE)
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


def _rows(session: ReparkSession, sql: str) -> list[list[Any]]:
    try:
        table = session.sql(sql).to_arrow()
    except Exception as error:
        assert _PANIC_MARKER not in str(error), str(error)
        raise
    return [[column[index].as_py() for column in table.columns] for index in range(table.num_rows)]


@pytest.fixture
def seeded(tmp_path: Path) -> Iterator[Path]:
    session = _open(tmp_path)
    for statement in _SEED:
        session.sql(statement).collect()
    session.stop()
    yield tmp_path


@pytest.mark.parametrize(("sql", "expected"), _SPARK_ANSWERS)
def test_a_float_or_zoned_struct_key_answers_spark_in_the_writing_session(
    tmp_path: Path, sql: str, expected: list[list[Any]]
) -> None:
    session = _open(tmp_path)
    try:
        for statement in _SEED:
            session.sql(statement).collect()
        assert session.conf.get("spark.sql.session.timeZone") == _ZONE
        assert _rows(session, sql) == expected, sql
    finally:
        session.stop()


@pytest.mark.parametrize(("sql", "expected"), _SPARK_ANSWERS)
def test_a_float_or_zoned_struct_key_answers_spark_in_a_fresh_session(
    seeded: Path, sql: str, expected: list[list[Any]]
) -> None:
    session = _reopen(seeded)
    try:
        assert _rows(session, sql) == expected, sql
    finally:
        session.stop()
