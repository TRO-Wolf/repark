from __future__ import annotations

import contextlib
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession

CELLS_PATH = Path(__file__).parent / "data" / "ident_dialect_1_cells.jsonl"
STRICT_SPELLINGS = frozenset(
    {"dq", "sq", "ns_dq", "cat_dq", "cat_sq", "catdq_bare", "multi", "unicode", "fullwidth"}
)
OVER_REFUSAL_SPELLINGS = frozenset({"digit", "trail_comment", "block_comment"})
UNIT_SPELLINGS = STRICT_SPELLINGS | OVER_REFUSAL_SPELLINGS
GRAMMAR_CONDITIONS = frozenset({"PARSE_SYNTAX_ERROR", "INVALID_IDENTIFIER"})
NAMES = ("t", "a", "`a``b`", "`1t`", "`ü1`", "`\uff54`")
VIEW_NAMES = ("t", "`a``b`", "`1t`", "`ü1`", "`\uff54`")
CAPABILITY_REFUSAL = "other:Cannot rename Hadoop tables"


def _load_cells() -> list[dict[str, Any]]:
    with CELLS_PATH.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


CELLS = _load_cells()
BY_KEY = {(cell["cat"], cell["head"], cell["spelling"], cell["view"]): cell for cell in CELLS}


def _cell_id(cell: dict[str, Any]) -> str:
    view = "view" if cell["view"] else "noview"
    return f"{cell['cat']}-{cell['head']}-{cell['spelling']}-{view}"


def _kind(answer: str) -> str:
    if answer == "ok":
        return "ok"
    if answer.startswith("rows "):
        return "rows"
    condition = re.search(r"\[([A-Z_.]+)\]", answer)
    if condition is not None:
        return condition.group(1)
    return "other:" + answer.removeprefix("ERR ")


def _rows(answer: str) -> list[list[str]]:
    return [[str(value) for value in row] for row in json.loads(answer.removeprefix("rows "))]


def _open(warehouse: Path) -> ReparkSession:
    spark = (
        ReparkSession.builder.appName("ident-dialect-1")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", str(warehouse / "hd"))
        .getOrCreate()
    )
    spark.register_memory_catalog("mem", str(warehouse / "mem"))
    return spark


@pytest.fixture
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    spark = _open(tmp_path)
    try:
        yield spark
    finally:
        spark.stop()


def _carried_rename_view(cell: dict[str, Any]) -> bool:
    return cell["view"] and cell["head"] == "alter_rename" and cell["spelling"] != "bare"


def _seed(spark: ReparkSession, catalog: str, view: bool, carried: bool) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    for name in (*NAMES, "t2"):
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.ns.{name}")
    spark.sql(f"CREATE TABLE {catalog}.ns.t (id INT, v STRING) USING iceberg")
    spark.sql(f"INSERT INTO {catalog}.ns.t VALUES (1,'a'),(2,'b')")
    spark.sql(f"CREATE TABLE {catalog}.ns.a (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {catalog}.ns.a VALUES (7)")
    for name in NAMES[2:]:
        spark.sql(f"CREATE TABLE {catalog}.ns.{name} (id INT, v STRING) USING iceberg")
        spark.sql(f"INSERT INTO {catalog}.ns.{name} VALUES (1,'a')")
    spark.sql(f"USE {catalog}.ns")
    for name in NAMES:
        with contextlib.suppress(Exception):
            spark.sql(f"DROP VIEW IF EXISTS {name}")
    if view:
        for name in VIEW_NAMES:
            spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS SELECT 99 AS id")
    if carried:
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW t2 AS SELECT 99 AS id")


def _state(spark: ReparkSession, catalog: str) -> dict[str, Any]:
    state: dict[str, Any] = {}
    for name in (*NAMES, "t2"):
        try:
            rows = spark.sql(f"SELECT * FROM {catalog}.ns.{name} ORDER BY 1").collect()
            state[name] = [list(map(str, row)) for row in rows]
        except Exception as error:
            found = re.search(r"\[([A-Z_.]+)\]", str(error))
            state[name] = "ERR " + (found.group(1) if found else "?")
    try:
        state["views"] = sorted(str(row[1]) for row in spark.sql("SHOW VIEWS").collect())
    except Exception:
        state["views"] = "ERR"
    return state


def _run(spark: ReparkSession, statement: str) -> str:
    try:
        rows = spark.sql(statement).collect()
    except Exception as error:
        text = str(error)
        found = re.search(r"\[([A-Z_.]+)\][^\n]*?SQLSTATE: [0-9A-Z]+", text)
        if found is not None:
            return "ERR " + found.group(0)
        return "ERR " + type(error).__name__ + ": " + text.splitlines()[0][:160]
    if not rows or len(rows[0]) == 0:
        return "ok"
    return "rows " + json.dumps([list(map(str, row)) for row in rows][:3])


def _expectation(cell: dict[str, Any]) -> str:
    if cell["spelling"] not in UNIT_SPELLINGS or cell["spark_kind"] == CAPABILITY_REFUSAL:
        return "measured"
    if cell["spark_kind"] == "ok":
        return "spark_acts"
    if cell["spelling"] in STRICT_SPELLINGS:
        return "spark_refuses"
    return "refuses"


def _assert_cell(cell: dict[str, Any], answer: str, changed: list[str]) -> None:
    expectation = _expectation(cell)
    measured = cell["repark"]
    shown = f"{cell['stmt']!r} answered {answer!r} changing {changed}"
    if expectation == "measured":
        assert _kind(answer) == _kind(measured), shown
        assert changed == cell["repark_changed"], shown
        if _kind(measured) == "rows":
            assert _rows(answer) == _rows(measured), shown
        return
    assert changed == cell["spark_changed"], shown
    if expectation == "spark_acts":
        if cell["spark"].startswith("rows "):
            assert _kind(answer) == "rows", shown
            assert _rows(answer) == _rows(cell["spark"]), shown
        else:
            control = BY_KEY[(cell["cat"], cell["head"], "bt", cell["view"])]["repark"]
            assert answer == "ok" or _kind(answer) == _kind(control) != "ok", shown
            assert not answer.startswith("ERR"), shown
        return
    assert answer.startswith("ERR"), shown
    if expectation == "spark_refuses":
        assert _kind(answer) == cell["spark_kind"], shown
        return
    assert _kind(answer) in {cell["spark_kind"], _kind(measured)}, shown


@pytest.mark.parametrize("cell", CELLS, ids=[_cell_id(cell) for cell in CELLS])
def test_identifier_cell_answers_like_the_measured_table(
    session: ReparkSession, cell: dict[str, Any]
) -> None:
    _seed(session, cell["cat"], cell["view"], _carried_rename_view(cell))
    before = _state(session, cell["cat"])
    answer = _run(session, cell["stmt"])
    after = _state(session, cell["cat"])
    changed = [key for key in before if before[key] != after[key]]
    _assert_cell(cell, answer, changed)


def test_the_table_covers_every_unit_spelling_on_every_head_and_catalog() -> None:
    keys = {(cell["cat"], cell["head"], cell["spelling"], cell["view"]) for cell in CELLS}
    assert len(keys) == len(CELLS) == 1144
    unit = [cell for cell in CELLS if cell["spelling"] in UNIT_SPELLINGS]
    assert len(unit) == 624
    assert {cell["spark_kind"] for cell in unit if cell["spelling"] in STRICT_SPELLINGS} <= (
        GRAMMAR_CONDITIONS | {"ok"}
    )
