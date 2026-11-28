from __future__ import annotations

import json
from typing import Any

import _record_ntz_ltz_widening_1 as recorder
import pytest

from repark import ReparkSession
from repark.spark import functions

_CELLS: dict[str, Any] = json.loads(recorder.ORACLE_PATH.read_text(encoding="utf-8"))["cells"]


def _open(cell: dict[str, Any]) -> ReparkSession:
    return (
        ReparkSession.builder.appName("ntz-ltz-widening-1")
        .config("spark.sql.session.timeZone", cell["zone"])
        .config("spark.sql.timestampType", cell["timestamp_type"])
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )


def test_the_oracle_covers_every_recorded_cell() -> None:
    assert sorted(_CELLS) == sorted(recorder.CELLS)


@pytest.mark.parametrize("name", sorted(_CELLS))
def test_a_widened_timestamp_ntz_answers_as_spark(name: str) -> None:
    cell = _CELLS[name]
    session = _open(cell)
    try:
        assert recorder.measure(session, functions, cell) == cell["spark"]
    finally:
        session.stop()
