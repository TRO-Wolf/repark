from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as spark_functions

_SEED = [
    "CREATE TABLE mem.ns.c (id INT, m MAP<STRUCT<k: INT, j: INT>, INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.c VALUES "
    "(1, map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8))",
    "INSERT INTO mem.ns.c VALUES (2, map(named_struct('k', 1, 'j', 5), 9))",
    "CREATE TABLE mem.ns.sas "
    "(id INT, m MAP<STRUCT<a: INT, arr: ARRAY<STRUCT<x: STRING, y: INT>>>, INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "INSERT INTO mem.ns.sas VALUES "
    "(1, map(named_struct('a', 1, 'arr', array(named_struct('x', 'p', 'y', 2))), 7)), "
    "(2, map(named_struct('a', 2, 'arr', array(named_struct('x', 'q', 'y', 3), "
    "named_struct('x', 'r', 'y', 4))), 9))",
]

_TABLES = ("c", "sas")

_ANSI = "spark.sql.ansi.enabled"
_CASE_SENSITIVE = "spark.sql.caseSensitive"

_CASE_UPPER = "SELECT id, m[named_struct('K', 1, 'J', 2)] FROM mem.ns.c ORDER BY id"
_CASE_UPPER_LITERAL = "SELECT map(named_struct('k', 1, 'j', 2), 7)[named_struct('K', 1, 'J', 2)]"
_CASE_MIXED = "SELECT id, m[named_struct('K', 1, 'j', 2)] FROM mem.ns.c ORDER BY id"
_CASE_ARRAY = "SELECT map(array(named_struct('k', 1)), 7)[array(named_struct('K', 1))]"
_CASE_INNER = (
    "SELECT id, m[named_struct('a', 1, 'arr', array(named_struct('X', 'p', 'Y', 2)))] "
    "FROM mem.ns.sas ORDER BY id"
)
_EXACT = "SELECT id, m[named_struct('k', 1, 'j', 2)] FROM mem.ns.c ORDER BY id"

_ANSI_ON_ANSWERS: list[tuple[str, list[list[Any]]]] = [
    (_CASE_UPPER, [[1, 7], [2, None]]),
    (_CASE_MIXED, [[1, 7], [2, None]]),
    (_CASE_UPPER_LITERAL, [[7]]),
    (_CASE_INNER, [[1, 7], [2, None]]),
]

_ANSI_OFF_ANSWERS: list[tuple[str, list[list[Any]]]] = [
    *_ANSI_ON_ANSWERS,
    (_CASE_ARRAY, [[7]]),
    ("SELECT map(named_struct('k', 1, 'K', 2), 7)[named_struct('k', 1, 'K', 2)]", [[7]]),
]

_CASE_SENSITIVE_REFUSALS = [_CASE_UPPER, _CASE_UPPER_LITERAL]

_PANIC_MARKER = "repark internal error"


def _open(warehouse: Path, ansi: str, case_sensitive: str = "false") -> ReparkSession:
    session = (
        ReparkSession.builder.appName("map-struct-key-subscript-5")
        .config("spark.sql.session.timeZone", "UTC")
        .config(_ANSI, ansi)
        .config(_CASE_SENSITIVE, case_sensitive)
        .getOrCreate()
    )
    session.register_memory_catalog("mem", str(warehouse))
    session.sql("CREATE NAMESPACE mem.ns").collect()
    return session


def _newest_metadata(warehouse: Path, table: str) -> str:
    candidates = sorted(warehouse.rglob(f"{table}/metadata/*.metadata.json"))
    assert candidates, f"no metadata files for {table} under the warehouse"
    return str(candidates[-1])


def _reopen(warehouse: Path, ansi: str, case_sensitive: str = "false") -> ReparkSession:
    session = _open(warehouse, ansi, case_sensitive)
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


def _assert_answers(session: ReparkSession, answers: list[tuple[str, list[list[Any]]]]) -> None:
    for sql, expected in answers:
        assert _rows(session, sql) == expected, sql


def _assert_case_sensitive_refusals(session: ReparkSession) -> None:
    for sql in _CASE_SENSITIVE_REFUSALS:
        with pytest.raises(AnalysisException) as caught:
            session.sql(sql).collect()
        text = str(caught.value)
        assert _PANIC_MARKER not in text, text
        assert "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]" in text, text
        assert 'The second parameter requires the "STRUCT<k: INT' in text, text
        assert 'however the key has the type "STRUCT<K: INT, J: INT>"' in text, text
        assert "SQLSTATE: 42K09" in text, text
    assert _rows(session, _EXACT) == [[1, 7], [2, None]]


def _seed(session: ReparkSession) -> None:
    for statement in _SEED:
        session.sql(statement).collect()


@pytest.fixture
def seeded(tmp_path: Path) -> Iterator[Path]:
    session = _open(tmp_path, "true")
    _seed(session)
    session.stop()
    yield tmp_path


def test_ansi_off_matches_struct_key_field_names_ignoring_case_in_the_writing_session(
    tmp_path: Path,
) -> None:
    session = _open(tmp_path, "false")
    try:
        _seed(session)
        _assert_answers(session, _ANSI_OFF_ANSWERS)
    finally:
        session.stop()


def test_ansi_off_matches_struct_key_field_names_ignoring_case_in_a_fresh_session(
    seeded: Path,
) -> None:
    session = _reopen(seeded, "false")
    try:
        _assert_answers(session, _ANSI_OFF_ANSWERS)
    finally:
        session.stop()


def test_a_case_sensitive_session_refuses_a_key_in_another_case_and_conf_set_restores_the_match(
    tmp_path: Path,
) -> None:
    session = _open(tmp_path, "false")
    try:
        _seed(session)
        session.conf.set(_CASE_SENSITIVE, "true")
        _assert_case_sensitive_refusals(session)
        session.conf.set(_CASE_SENSITIVE, "false")
        _assert_answers(session, _ANSI_OFF_ANSWERS)
    finally:
        session.stop()


def test_case_sensitive_in_the_builder_refuses_a_key_in_another_case_in_a_fresh_session(
    seeded: Path,
) -> None:
    session = _reopen(seeded, "false", "true")
    try:
        _assert_case_sensitive_refusals(session)
    finally:
        session.stop()


def test_ansi_on_casts_a_key_in_another_case_by_position_under_either_case_setting(
    seeded: Path,
) -> None:
    session = _reopen(seeded, "true")
    try:
        _assert_answers(session, _ANSI_ON_ANSWERS)
        session.conf.set(_CASE_SENSITIVE, "true")
        assert _rows(session, _CASE_UPPER) == [[1, 7], [2, None]]
    finally:
        session.stop()


def test_an_expr_select_matches_a_key_in_another_case_with_ansi_off(seeded: Path) -> None:
    session = _reopen(seeded, "false")
    try:
        frame = (
            session.table("mem.ns.c")
            .select("id", spark_functions.expr("m[named_struct('K', 1, 'J', 2)]"))
            .orderBy("id")
        )
        table = frame.to_arrow()
        rows = [
            [column[index].as_py() for column in table.columns] for index in range(table.num_rows)
        ]
        assert rows == [[1, 7], [2, None]]
    finally:
        session.stop()
