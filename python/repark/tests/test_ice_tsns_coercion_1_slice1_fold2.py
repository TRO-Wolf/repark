"""Slice 1 fold 2 facade pins: nanosecond sites into consumers that refused them.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
from typing import Any

import _record_ice_tsns_coercion_1_slice1_fold2 as fold2
import pytest

EXPECTED_FOLD2_CELLS = 68
TWINS = {"ns": "ntz", "tzns": "ts"}
KIND_REFUSED = {
    "py|ns|map_in_arrow_ltz": (
        "mapInArrow schema mismatch on field 'v': expected type "
        "timestamp[us, tz=UTC] (timestamp), got timestamp[ns]"
    ),
}


def _load_oracle() -> dict[str, Any]:
    """Return the baseline-recorded fold-2 oracle."""
    text: str = (fold2.TESTS / "ice_tsns_coercion_1_slice1_fold2.json").read_text()
    return json.loads(text)


def _floored_ticks(value: Any) -> Any:
    """Return nanosecond ticks floored to microseconds, at any list depth."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value // 1000
    if isinstance(value, list):
        return [_floored_ticks(item) for item in value]
    step = value["step"]
    return {
        "count": value["count"],
        "first": value["first"] // 1000,
        "last": value["last"] // 1000,
        "step": None if step is None else step // 1000,
    }


def _as_microseconds(outcome: dict[str, Any]) -> dict[str, Any]:
    """Return a nanosecond outcome in its microsecond twin's unit."""
    if "error" in outcome:
        return {"error": outcome["error"].replace("timestamp[ns", "timestamp[us")}
    columns = []
    for column in outcome["columns"]:
        if "timestamp[ns" not in column["type"]:
            columns.append(column)
            continue
        columns.append(
            {
                **column,
                "type": column["type"].replace("timestamp[ns", "timestamp[us"),
                "values": _floored_ticks(column["values"]),
            }
        )
    return {"columns": columns}


def _twin_cell(cell: str) -> str:
    """Return the cell that reads the microsecond twin column."""
    zone, door, source, shape = cell.split("|")
    return f"{zone}|{door}|{TWINS[source]}|{shape}"


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> dict[str, dict[str, Any]]:
    """Return every nanosecond cell and every microsecond twin cell on this module."""
    outcomes = fold2.measure_all(tmp_path_factory.mktemp("site"), fold2.SOURCES)
    twins = fold2.measure_all(tmp_path_factory.mktemp("twin"), tuple(TWINS.values()))
    return {**outcomes, **twins}


def test_cells_main_answered_still_answer(measured: dict[str, dict[str, Any]]) -> None:
    """pins: ice-tsns-coercion-1/C-005 — a cell main answers is answered here."""
    oracle = _load_oracle()
    assert len(oracle) == EXPECTED_FOLD2_CELLS
    refused = []
    for cell, recorded in sorted(oracle.items()):
        if "error" in recorded:
            continue
        outcome = measured[cell]
        kind = cell.split("|", 1)[1]
        if kind in KIND_REFUSED:
            assert outcome == {"error": KIND_REFUSED[kind]}, cell
            continue
        if "error" in outcome:
            refused.append(f"{cell}: {outcome['error']}")
    assert refused == []


def test_nanosecond_cells_equal_the_microsecond_twin(
    measured: dict[str, dict[str, Any]],
) -> None:
    """pins: ice-tsns-coercion-1/C-005 — ticks floored to microseconds equal the twin."""
    oracle = _load_oracle()
    differing = []
    for cell in sorted(oracle):
        if _as_microseconds(measured[cell]) != measured[_twin_cell(cell)]:
            differing.append(cell)
    assert differing == []
