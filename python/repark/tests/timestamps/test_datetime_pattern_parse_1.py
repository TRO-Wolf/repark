from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

ORACLE_PATH: Path = Path(__file__).with_name("datetime_pattern_parse_1_spark_oracle.json")
_CELLS: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))["cells"]


def _quote(text: str | None) -> str:
    return "NULL" if text is None else "'" + text.replace("'", "\\'") + "'"


def _open(cell: dict[str, Any], warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("datetime-pattern-parse-1")
        .config("spark.sql.session.timeZone", cell["zone"])
        .config("spark.sql.timestampType", cell["built"])
        .config("spark.sql.ansi.enabled", cell["ansi"])
        .getOrCreate()
    )
    if cell["runtime"]:
        session.conf.set("spark.sql.timestampType", cell["runtime"])
    session.register_memory_catalog("sc", str(warehouse))
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    return session


def _answer(
    session: ReparkSession, cell: dict[str, Any], text: str | None, pattern: str, table: str
) -> Any:
    fn = cell["fn"]
    if cell["op"] == "select":
        literal = f"{fn}({_quote(text)}, {_quote(pattern)})"
        row = session.sql(f"SELECT typeof({literal}), CAST({literal} AS STRING)").collect()[0]
        return [row[0], None if row[1] is None else str(row[1])]
    if cell["op"] == "store_ntz":
        session.sql(f"CREATE TABLE {table} (id INT, v TIMESTAMP_NTZ) USING iceberg")
        source = f"(SELECT {_quote(text)} AS s)"
        session.sql(f"INSERT INTO {table} SELECT 0, {fn}(s, {_quote(pattern)}) FROM {source}")
        rows = session.sql(f"SELECT CAST(v AS STRING) FROM {table}").collect()
        return [None if row[0] is None else str(row[0]) for row in rows]
    frame = session.createDataFrame(list(enumerate(cell["texts"])), "id INT, x STRING")
    frame.createOrReplaceTempView("pin_src")
    session.sql(f"CREATE TABLE {table} (id INT, v {cell['column']}) USING iceberg")
    session.sql(f"INSERT INTO {table} SELECT id, {fn}(x, {_quote(pattern)}) FROM pin_src")
    rows = session.sql(f"SELECT id, CAST(v AS STRING) FROM {table} ORDER BY id").collect()
    return [[int(row[0]), None if row[1] is None else str(row[1])] for row in rows]


def _check(session: ReparkSession, cell: dict[str, Any], expected: Any, table: str) -> None:
    if isinstance(expected, dict):
        with pytest.raises(Exception) as caught:
            _answer(session, cell, cell.get("text"), cell["pat"], table)
        assert expected["error"] in str(caught.value)
        if expected["index"] is not None:
            assert f"at index {expected['index']}" in str(caught.value)
        return
    assert _answer(session, cell, cell.get("text"), cell["pat"], table) == expected


@pytest.mark.parametrize(
    "name", sorted(name for name, cell in _CELLS.items() if cell["kind"] == "TARGET")
)
def test_pattern_parse_answers_what_spark_answers(name: str, tmp_path: Path) -> None:
    cell = _CELLS[name]
    session = _open(cell, tmp_path)
    try:
        _check(session, cell, cell["spark"], "sc.ns.t")
    finally:
        session.stop()


@pytest.mark.parametrize(
    "name", sorted(name for name, cell in _CELLS.items() if cell["kind"] == "PROTECTED")
)
def test_pattern_parse_keeps_the_answers_main_shares_with_spark(name: str, tmp_path: Path) -> None:
    cell = _CELLS[name]
    session = _open(cell, tmp_path)
    try:
        _check(session, cell, cell["spark"], "sc.ns.t")
    finally:
        session.stop()


@pytest.mark.parametrize(
    "name", sorted(name for name, cell in _CELLS.items() if cell["kind"] == "TWIN")
)
def test_ntz_session_newly_parsed_text_matches_mains_twin(name: str, tmp_path: Path) -> None:
    cell = _CELLS[name]
    session = _open(cell, tmp_path)
    try:
        fresh = _answer(session, cell, cell["text"], cell["pat"], "sc.ns.fresh")
        twin = _answer(session, cell, cell["twin"], cell.get("twin_pat", cell["pat"]), "sc.ns.twin")
        assert fresh == twin == cell["main_twin"]
    finally:
        session.stop()
