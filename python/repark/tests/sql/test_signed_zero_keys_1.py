"""SIGNED-ZERO-NORMALIZE-1 part B part 1: flat float keys answer like Spark on every door"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

import repark
from repark import ReparkSession, Window, functions
from repark._native import PyReparkSession

pytestmark = pytest.mark.xdist_group("test_signed_zero_keys_1")

CELLS_PATH = Path(__file__).parent / "data" / "signed_zero_keys_1_cells.jsonl"
LINES = [json.loads(line) for line in CELLS_PATH.read_text(encoding="utf-8").splitlines() if line]
SETUP = LINES[0]["setup"]
CELLS = LINES[1:]
ROUTES = ("spark-sql", "f-expr", "typed", "native")
CATALOG = "ice"
NAMESPACE = "szb"


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
    if isinstance(value, dict):
        return {key: norm(item) for key, item in value.items()}
    return value


def collect(frame: Any) -> list[list[Any]]:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    rows = [list(values) for values in zip(*columns, strict=True)] if columns else []
    return [[norm(value) for value in row] for row in rows]


def keys(rows: list[list[Any]]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


@pytest.fixture
def facade(tmp_path: Path) -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("test_signed_zero_keys_1").getOrCreate()
    session.register_memory_catalog(CATALOG, str(tmp_path / "facade"))
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}").collect()
    for step in SETUP:
        if step["door"] == "spark-sql":
            session.sql(step["text"]).collect()
    yield session
    session.stop()


@pytest.fixture
def native(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PyReparkSession:
    root = tmp_path / "native"
    root.mkdir()
    session = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    session.register_memory_catalog(CATALOG, str(root))
    session.create_namespace(CATALOG, NAMESPACE, str(root / NAMESPACE))
    monkeypatch.setattr(repark, "_ANSI_NATIVE", session)
    for step in SETUP:
        if step["door"] == "native":
            collect(repark.sql(step["text"]))
    return session


def run(cell: dict[str, Any], facade: ReparkSession | None) -> list[list[Any]]:
    route = cell["route"]
    if route == "native":
        return collect(repark.sql(cell["text"]))
    assert facade is not None
    if route == "spark-sql":
        return collect(facade.sql(cell["text"]))
    frames = {"t": facade.table(f"{CATALOG}.{NAMESPACE}.t_s")}
    if cell["build"]:
        frames["u"] = facade.table(f"{CATALOG}.{NAMESPACE}.{cell['build']}_s")
    return collect(eval(cell["text"], {"F": functions, "W": Window, **frames}))


def side(cell: dict[str, Any], key: str, facade: ReparkSession | None) -> None:
    for statement in cell[key]:
        if cell["route"] == "native":
            collect(repark.sql(statement))
        elif facade is not None:
            facade.sql(statement).collect()


def mismatch(cell: dict[str, Any], facade: ReparkSession | None) -> str | None:
    try:
        side(cell, "pre", facade)
        rows = run(cell, facade)
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            raise
        return f"{type(error).__name__}: {str(error)[:160]}"
    finally:
        side(cell, "post", facade)
    same = rows == cell["truth"] if cell["ordered"] else keys(rows) == keys(cell["truth"])
    return None if same else f"rows {rows} != {cell['truth']}"


@pytest.mark.parametrize("route", ROUTES)
def test_every_float_key_cell_reaches_the_spark_truth(
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
    assert {cell["route"] for cell in CELLS} == set(ROUTES)
    assert len(CELLS) == 780
    assert sum(cell["key_row"] for cell in CELLS) == 470


def test_a_set_on_either_door_cannot_turn_the_key_wrap_off(
    facade: ReparkSession, native: PyReparkSession
) -> None:
    text = "SELECT count(*) FROM (SELECT d FROM {t} GROUP BY d)"
    for statement in (
        "SET repark.sql.signedZero.normalizeKeys = false",
        "SET repark.sql.signedZero.normalizeLiterals = false",
    ):
        with contextlib.suppress(Exception):
            facade.sql(statement).collect()
        with contextlib.suppress(Exception):
            collect(repark.sql(statement))
    spark_rows = collect(facade.sql(text.format(t=f"{CATALOG}.{NAMESPACE}.t_s")))
    native_rows = collect(repark.sql(text.format(t=f"{CATALOG}.{NAMESPACE}.t_n")))
    assert spark_rows == native_rows == [[4]]
