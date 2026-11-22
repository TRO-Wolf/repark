from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _record_unix_timestamp_truncate_1 import (
    ANSI_MODES,
    FIXTURE,
    FUNCTIONS,
    STORED_PAIRS,
    ZONES,
    answer,
    cell_key,
    cells,
    stored_minutes,
)

from repark import ReparkSession
from repark.spark import functions as spark_functions

_ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]
_GROUPS: list[tuple[str, str, str, str]] = [
    (function, kind, zone, ansi)
    for function, kind, _template, _values in FUNCTIONS
    for zone in ZONES
    for ansi in ANSI_MODES
]


@pytest.fixture
def session() -> Iterator[ReparkSession]:
    built = ReparkSession.builder.appName("unix-timestamp-truncate-1").getOrCreate()
    yield built
    built.stop()


def _configure(built: ReparkSession, zone: str, ansi: str) -> None:
    built.conf.set("spark.sql.session.timeZone", zone)
    built.conf.set("spark.sql.ansi.enabled", ansi)


def _same(expected: dict[str, Any], got: dict[str, Any]) -> bool:
    if "value" in expected:
        return got.get("value", "missing") == expected["value"]
    return got.get("error") == expected["error"]


def test_the_fixture_holds_every_cell_of_the_matrix() -> None:
    keys = {
        cell_key(f, k, seat, zone, ansi, value) for f, k, _t, seat, zone, ansi, value in cells()
    }
    assert set(_CELLS) == keys
    assert _ORACLE["spark"] == "4.1.2"
    assert set(_ORACLE["stored_minutes"]) == {
        f"{zone}|{ansi}" for zone in ZONES for ansi in ANSI_MODES
    }


@pytest.mark.parametrize(
    ("function", "kind", "zone", "ansi"),
    _GROUPS,
    ids=[f"{function}-{kind}-{zone}-ansi_{ansi}" for function, kind, zone, ansi in _GROUPS],
)
def test_every_cell_answers_as_spark(
    session: ReparkSession, function: str, kind: str, zone: str, ansi: str
) -> None:
    _configure(session, zone, ansi)
    misses = []
    for cell_function, cell_kind, template, seat, cell_zone, cell_ansi, value in cells():
        if (cell_function, cell_kind, cell_zone, cell_ansi) != (function, kind, zone, ansi):
            continue
        expected = _CELLS[cell_key(function, kind, seat, zone, ansi, value)]
        got = answer(session, spark_functions, function, kind, template, seat, value)
        if not _same(expected, got):
            misses.append(f"{seat} {value!r}: spark {expected} repark {got}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("zone", ZONES)
@pytest.mark.parametrize("ansi", ANSI_MODES)
def test_the_minute_difference_stored_in_parquet_reads_back_as_spark(
    session: ReparkSession, tmp_path: Path, zone: str, ansi: str
) -> None:
    _configure(session, zone, ansi)
    got = stored_minutes(session, spark_functions, str(tmp_path / "minutes"))
    assert len(got) == len(STORED_PAIRS)
    assert got == _ORACLE["stored_minutes"][f"{zone}|{ansi}"]


@pytest.mark.parametrize("function", ["unix_seconds", "unix_millis"])
def test_a_timestamp_ntz_column_refuses_with_sparks_text(
    session: ReparkSession, function: str
) -> None:
    _configure(session, "America/Los_Angeles", "true")
    for seat in ("sql-column", "expr", "typed"):
        key = cell_key(
            function, "ntz", seat, "America/Los_Angeles", "true", "1960-06-01 00:00:00.25"
        )
        expected = _CELLS[key]
        got = answer(
            session,
            spark_functions,
            function,
            "ntz",
            f"{function}({{x}})",
            seat,
            "1960-06-01 00:00:00.25",
        )
        assert expected["error"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
        assert got.get("error") == expected["error"], got
        assert f'Cannot resolve "{function}(ntz)" due to data type mismatch' in got["message"]
        assert 'however "ntz" has the type "TIMESTAMP_NTZ"' in got["message"]
