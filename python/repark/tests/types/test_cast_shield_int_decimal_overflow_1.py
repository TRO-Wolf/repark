from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from _record_cast_shield_int_decimal_overflow_1 import (
    WRITE_SOURCES,
    WRITE_TABLES,
    collect,
    conjunction_cells,
    door_cells,
    fable_answer,
    fable_cells,
    fable_result,
    flatten_writes,
    guard_cells,
    kernel_cells,
    make_frames,
    neighbour_cells,
    placement_cells,
    prune_cells,
    pushdown_cells,
    repark_session,
    repark_source,
    same_answer,
    write_cells,
)

FIXTURE = Path(__file__).resolve().parent / "cast_shield_int_decimal_overflow_1_spark_oracle.json"
ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
ANSI_MODES = ("true", "false")
GROUPS = (
    "kernel",
    "place",
    "card",
    "door",
    "neighbour",
    "conj",
    "null",
    "prune",
    "pushdown",
    "writes",
)
pytestmark = pytest.mark.xdist_group("cast_shield_int_decimal_overflow_1")


def _group(key: str) -> str:
    head = key.split("/")[0]
    return "writes" if head in WRITE_TABLES else head


def _readback(session: Any) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for label in WRITE_SOURCES:
        for prefix in WRITE_TABLES.values():
            name = f"{prefix}_{label}"
            state = collect(lambda n=name: session.sql(f"SELECT * FROM ice.cs.{n}"), False)
            if state["kind"] == "ok":
                tables[name] = state
    return tables


def _measure(root: Path, ansi: str) -> dict[str, Any]:
    session = repark_session(root / "wh", root / "src_wh", ansi)
    out: dict[str, Any] = {"cells": {}, "explain": {}, "writes": {}}
    try:
        repark_source(session)
        make_frames(session, root)
        kernel_cells(session, out["cells"])
        placement_cells(session, out["cells"])
        door_cells(session, "repark", out["cells"])
        neighbour_cells(session, out["cells"])
        conjunction_cells(session, out["cells"])
        prune_cells(session, "repark", root, out)
        pushdown_cells(session, "repark", out)
        write_cells(session, out)
        readback = _readback(session)
    finally:
        session.stop()
    return {**out["cells"], **flatten_writes(out["writes"], readback)}


@pytest.fixture(scope="module", params=ANSI_MODES)
def measured(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Any:
    ansi = request.param
    return ansi, _measure(tmp_path_factory.mktemp(f"castshield-{ansi}"), ansi)


@pytest.fixture(scope="module")
def fable_measured(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("castshield-fable")
    session = repark_session(root / "wh", root / "src_wh", "true")
    try:
        return fable_cells(session, root)
    finally:
        session.stop()


def _expected(ansi: str) -> dict[str, Any]:
    mode = ORACLE["modes"][ansi]
    return {**mode["cells"], **flatten_writes(mode["writes"], mode["readback"])}


@pytest.mark.parametrize("kind", ["defect", "kept"])
@pytest.mark.parametrize("group", GROUPS)
def test_step0_cells_answer_as_spark(measured: Any, group: str, kind: str) -> None:
    ansi, cells = measured
    expected = _expected(ansi)
    keys = [key for key in ORACLE["pinned"][ansi][kind] if _group(key) == group]
    misses = {
        key: {"spark": expected[key], "repark": cells.get(key)}
        for key in keys
        if not same_answer(expected[key], cells.get(key))
    }
    assert not misses, json.dumps(misses, indent=1, sort_keys=True)[:4000]


@pytest.mark.parametrize("kind", ["defect", "kept"])
def test_conjunction_and_or_shapes_answer_as_spark(
    fable_measured: dict[str, Any], kind: str
) -> None:
    misses = {}
    for key in ORACLE["fable_pinned"][kind]:
        expected = fable_answer(ORACLE["fable"][key])
        got = fable_result(fable_measured[key])
        if expected != got:
            misses[key] = {"sql": ORACLE["fable"][key]["sql"], "spark": expected, "repark": got}
    assert not misses, json.dumps(misses, indent=1, sort_keys=True)[:4000]


@pytest.fixture(scope="module", params=ANSI_MODES)
def guard_measured(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> Any:
    ansi = request.param
    root = tmp_path_factory.mktemp(f"castshield-guard-{ansi}")
    session = repark_session(root / "wh", root / "src_wh", ansi)
    try:
        return ansi, guard_cells(session)
    finally:
        session.stop()


@pytest.mark.parametrize("kind", ["defect", "kept"])
def test_guard_shapes_answer_as_spark(guard_measured: Any, kind: str) -> None:
    ansi, cells = guard_measured
    expected = ORACLE["guard"][ansi]
    misses = {
        key: {"spark": expected[key], "repark": cells.get(key)}
        for key in ORACLE["guard_pinned"][ansi][kind]
        if not same_answer(expected[key], cells.get(key))
    }
    assert not misses, json.dumps(misses, indent=1, sort_keys=True)[:4000]
