"""SIGNED-ZERO-NORMALIZE-1 part A: a float comparison against a zero literal answers like Spark"""

from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession, functions
from repark._native import PyReparkSession

pytestmark = pytest.mark.xdist_group("test_signed_zero_normalize_1")

CELLS_PATH = Path(__file__).parent / "data" / "signed_zero_normalize_1_cells.jsonl"
TABLE = "ice.ns.t_split_cow"
COLUMNS = "id BIGINT, i INT, d DOUBLE, tf FLOAT, e DOUBLE, s STRING, flag STRING"
PROPS = (
    "'format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'"
)
ROWS = (
    "(1, 1, CAST('1.5' AS DOUBLE), CAST('1.5' AS FLOAT), NULL, 'a', 'f1')",
    "(2, NULL, NULL, NULL, NULL, NULL, 'f2')",
    "(3, NULL, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT), NULL, '', 'f3')",
    "(4, NULL, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT), NULL, 'a ', 'f4')",
    "(5, 2147483647, CAST('Infinity' AS DOUBLE), CAST('Infinity' AS FLOAT), NULL, NULL, 'f5')",
    "(6, -1, CAST('0.0' AS DOUBLE), CAST('0.0' AS FLOAT), NULL, 'A', 'f6')",
    "(7, NULL, NULL, NULL, NULL, 'bb', 'f7')",
    "(8, 1, CAST('1.5' AS DOUBLE), CAST('1.5' AS FLOAT), NULL, 'a', 'f8')",
)
NAMES_ONLY = frozenset({"D6-3", "D6-4", "D6-5", "D6-6"})
CELLS = [json.loads(line) for line in CELLS_PATH.read_text(encoding="utf-8").splitlines() if line]
ROUTES = ("spark-sql", "filter-str", "f-expr", "typed", "native")


def norm(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value in (float("inf"), float("-inf")):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0.0 and str(value) == "-0.0":
            return "-0.0"
        return repr(value)
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (list, tuple)):
        return [norm(item) for item in value]
    return value


def collect(frame: Any) -> tuple[list[str], list[list[Any]]]:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    rows = [list(values) for values in zip(*columns, strict=True)] if columns else []
    return list(table.column_names), [[norm(value) for value in row] for row in rows]


def keys(rows: list[list[Any]]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


@pytest.fixture
def facade(tmp_path: Path) -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("test_signed_zero_normalize_1").getOrCreate()
    session.register_memory_catalog("ice", str(tmp_path))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    session.sql(f"CREATE TABLE {TABLE} ({COLUMNS}) USING iceberg TBLPROPERTIES ({PROPS})").collect()
    for row in ROWS:
        session.sql(f"INSERT INTO {TABLE} VALUES {row}").collect()
    yield session
    session.stop()


@pytest.fixture
def native(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PyReparkSession:
    session = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    session.register_memory_catalog("ice", str(tmp_path))
    session.create_namespace("ice", "ns", str(tmp_path / "ns"))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", session)
    collect(repark.sql(f"CREATE TABLE {TABLE} ({COLUMNS})"))
    for row in ROWS:
        collect(repark.sql(f"INSERT INTO {TABLE} VALUES {row}"))
    return session


def run(route: str, text: str, facade: ReparkSession | None) -> tuple[list[str], list[list[Any]]]:
    if route == "native":
        result = None
        for step in text.split(" ; "):
            result = repark.sql(step)
        return collect(result)
    assert facade is not None
    if route == "spark-sql":
        return collect(facade.sql(text))
    frame = facade.table(TABLE)
    if route == "filter-str":
        return collect(frame.filter(text).select("id"))
    if route == "f-expr":
        return collect(frame.filter(functions.expr(text)).select("id"))
    return collect(eval(text, {"F": functions, "frame": frame}))


def mismatch(cell: dict[str, Any], facade: ReparkSession | None) -> str | None:
    try:
        names, rows = run(cell["route"], cell["text"], facade)
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            raise
        if cell["residue"] is not None:
            return None
        return f"{type(error).__name__}: {str(error)[:160]}"
    if cell["cols"] is not None and names != cell["cols"]:
        return f"names {names} != {cell['cols']}"
    if cell["cid"] in NAMES_ONLY:
        return None
    same = rows == cell["truth"] if cell["ordered"] else keys(rows) == keys(cell["truth"])
    return None if same else f"rows {rows} != {cell['truth']}"


@pytest.mark.parametrize("route", ROUTES)
def test_every_part_a_cell_reaches_the_spark_truth(
    route: str, request: pytest.FixtureRequest
) -> None:
    door = request.getfixturevalue("native" if route == "native" else "facade")
    facade = None if route == "native" else door
    cells = [cell for cell in CELLS if cell["route"] == route]
    assert cells
    failures = [
        f"{cell['cid']} {cell['text'][:80]}: {problem}"
        for cell in cells
        if (problem := mismatch(cell, facade)) is not None
    ]
    assert not failures, "\n".join(failures[:25])


def test_the_pins_cover_the_moved_rows() -> None:
    groups = {cell["group"] for cell in CELLS}
    assert groups == {"A1", "A2", "A3", "D2", "D3", "D4", "D5", "D6", "D13"}
    assert {cell["route"] for cell in CELLS} == set(ROUTES)
    assert sum(cell["base"] != "EQUAL" for cell in CELLS) == 245
    residues = {cell["residue"] for cell in CELLS if cell["residue"] is not None}
    assert residues == {"DECIMAL-EXPR-FLOAT-COMPARE-1"}
    assert sum(cell["residue"] is not None for cell in CELLS) == 8
