from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession
from repark.errors import AnalysisException

_SEED = [
    "CREATE TABLE mem.ns.c (id INT, m MAP<STRUCT<k: INT, j: INT>, INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.c VALUES "
    "(1, map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8))",
    "INSERT INTO mem.ns.c VALUES (2, map(named_struct('k', 1, 'j', 5), 9))",
]

_ANSI = "spark.sql.ansi.enabled"

_INEXACT_KEYS: list[tuple[str, str]] = [
    ("SELECT id, m[named_struct('j', 1, 'k', 2)] FROM mem.ns.c ORDER BY id", '"STRUCT<j: INT'),
    (
        "SELECT id, m[named_struct('k', CAST(1 AS BIGINT), 'j', 2)] FROM mem.ns.c ORDER BY id",
        '"STRUCT<k: BIGINT',
    ),
    ("SELECT id, m[named_struct('x', id, 'y', 2)] FROM mem.ns.c ORDER BY id", '"STRUCT<x: INT'),
    ("SELECT map(named_struct('k', 1, 'j', 2), 7)[named_struct('j', 1, 'k', 2)]", '"STRUCT<j: INT'),
]

_ANSI_ON_ANSWERS: list[list[list[Any]]] = [
    [[1, 7], [2, None]],
    [[1, 7], [2, None]],
    [[1, 7], [2, None]],
    [[7]],
]

_ALWAYS_ANSWERS: list[tuple[str, list[list[Any]]]] = [
    ("SELECT id, m[named_struct('k', 1, 'j', 2)] FROM mem.ns.c ORDER BY id", [[1, 7], [2, None]]),
    ("SELECT map(array(1), 7)[array(CAST(1 AS BIGINT))]", [[7]]),
]

_PANIC_MARKER = "repark internal error"


def _open(warehouse: Path, ansi: str) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("map-struct-key-subscript-4")
        .config("spark.sql.session.timeZone", "UTC")
        .config(_ANSI, ansi)
        .getOrCreate()
    )
    session.register_memory_catalog("mem", str(warehouse))
    session.sql("CREATE NAMESPACE mem.ns").collect()
    return session


def _newest_metadata(warehouse: Path) -> str:
    candidates = sorted(warehouse.rglob("c/metadata/*.metadata.json"))
    assert candidates, "no metadata files for the table under the warehouse"
    return str(candidates[-1])


def _reopen(warehouse: Path, ansi: str) -> ReparkSession:
    session = _open(warehouse, ansi)
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
    return [[column[index].as_py() for column in table.columns] for index in range(table.num_rows)]


def _assert_ansi_off(session: ReparkSession) -> None:
    for sql, given in _INEXACT_KEYS:
        with pytest.raises(AnalysisException) as caught:
            session.sql(sql).collect()
        text = str(caught.value)
        assert _PANIC_MARKER not in text, text
        assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in text, text
        assert given in text, text
    for sql, expected in _ALWAYS_ANSWERS:
        assert _rows(session, sql) == expected, sql


def _assert_ansi_on(session: ReparkSession) -> None:
    for (sql, _), expected in zip(_INEXACT_KEYS, _ANSI_ON_ANSWERS, strict=True):
        assert _rows(session, sql) == expected, sql
    for sql, expected in _ALWAYS_ANSWERS:
        assert _rows(session, sql) == expected, sql


@pytest.fixture
def seeded(tmp_path: Path) -> Iterator[Path]:
    session = _open(tmp_path, "true")
    for statement in _SEED:
        session.sql(statement).collect()
    session.stop()
    yield tmp_path


def test_ansi_off_in_the_builder_refuses_an_inexact_struct_key_in_the_writing_session(
    tmp_path: Path,
) -> None:
    session = _open(tmp_path, "false")
    try:
        for statement in _SEED:
            session.sql(statement).collect()
        _assert_ansi_off(session)
    finally:
        session.stop()


def test_ansi_off_in_the_builder_refuses_an_inexact_struct_key_in_a_fresh_session(
    seeded: Path,
) -> None:
    session = _reopen(seeded, "false")
    try:
        _assert_ansi_off(session)
    finally:
        session.stop()


def test_conf_set_gates_the_struct_key_cast_both_ways_in_the_writing_session(
    tmp_path: Path,
) -> None:
    session = _open(tmp_path, "true")
    try:
        for statement in _SEED:
            session.sql(statement).collect()
        _assert_ansi_on(session)
        session.conf.set(_ANSI, "false")
        _assert_ansi_off(session)
        session.conf.set(_ANSI, "true")
        _assert_ansi_on(session)
    finally:
        session.stop()


def test_conf_set_gates_the_struct_key_cast_both_ways_in_a_fresh_session(seeded: Path) -> None:
    session = _reopen(seeded, "true")
    try:
        _assert_ansi_on(session)
        session.conf.set(_ANSI, "false")
        _assert_ansi_off(session)
    finally:
        session.stop()


def test_the_native_door_keeps_its_ansi_default_and_casts_by_position() -> None:
    renamed = repark.sql(
        "SELECT map(named_struct('k', 1, 'j', 2), 7)[named_struct('j', 1, 'k', 2)] AS v"
    ).to_arrow()
    assert renamed.column("v").to_pylist() == [7]
    widened = repark.sql(
        "SELECT map(make_array(1), 7)[make_array(CAST(1 AS BIGINT))] AS v"
    ).to_arrow()
    assert widened.column("v").to_pylist() == [7]
