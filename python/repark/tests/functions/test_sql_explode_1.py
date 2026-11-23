from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _record_sql_explode_1 import (
    ANSI_MODES,
    CELLS,
    EXPR_SOURCES,
    FIXTURE,
    WRITES,
    ZONES,
    mode_key,
    seat_answers,
    seed,
    write_answers,
)

from repark import ReparkSession
from repark.spark import functions as spark_functions
from repark.spark.session import _reset_active_session_for_tests

_ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
_CATALOG = "loc"
_SEATS: tuple[str, ...] = ("sql", "expr", "select-expr")

pytestmark = pytest.mark.xdist_group("test_sql_explode_1")


def _session(root: Path, zone: str, ansi: str) -> ReparkSession:
    _reset_active_session_for_tests()
    built = (
        ReparkSession.builder.appName("test-sql-explode-1")
        .config(f"spark.sql.catalog.{_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{_CATALOG}.warehouse", str(root / "wh"))
        .getOrCreate()
    )
    built.conf.set("spark.sql.session.timeZone", zone)
    built.conf.set("spark.sql.ansi.enabled", ansi)
    built.sql(f"CREATE NAMESPACE IF NOT EXISTS {_CATALOG}.ns")
    return built


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, dict[str, Any]]]:
    root = tmp_path_factory.mktemp("sql-explode-1")
    found: dict[str, dict[str, Any]] = {}
    for zone in ZONES:
        for ansi in ANSI_MODES:
            writer = _session(root, zone, ansi)
            table = seed(writer, _CATALOG, zone, ansi)
            answers = seat_answers(writer, spark_functions, table)
            cold = _ColdReader(root, zone, ansi, writer)
            answers.update(write_answers(writer, _CATALOG, zone, ansi, cold.open))
            cold.close()
            found[mode_key(zone, ansi)] = answers
    _reset_active_session_for_tests()
    yield found


class _ColdReader:
    def __init__(self, root: Path, zone: str, ansi: str, writer: ReparkSession) -> None:
        self.root = root
        self.zone = zone
        self.ansi = ansi
        self.writer = writer
        self.reader: ReparkSession | None = None

    def open(self) -> ReparkSession:
        self.writer.stop()
        self.reader = _session(self.root, self.zone, self.ansi)
        return self.reader

    def close(self) -> None:
        if self.reader is not None:
            self.reader.stop()


def _literal(key: str) -> bool:
    return CELLS.get(key.split("|")[0], ("table", ()))[0] == "none"


def _normal(cell: dict[str, Any], literal: bool) -> dict[str, Any]:
    if "error" in cell:
        return {"error": True}
    rows = cell["rows"] if literal else sorted(cell["rows"], key=json.dumps)
    schema = [field[:2] if literal else field for field in cell["schema"]]
    return {"columns": cell["columns"], "schema": schema, "rows": rows}


def _scoped_keys(seat: str) -> list[str]:
    if seat == "write":
        return [f"{name}|write" for name in WRITES]
    return [
        f"{name}|{seat}"
        for name, (source, _items) in CELLS.items()
        if seat == "sql" or source in EXPR_SOURCES
    ]


def test_the_fixture_holds_every_scoped_cell_of_every_mode() -> None:
    assert _ORACLE["spark"] == "4.1.2"
    assert set(_ORACLE["modes"]) == {mode_key(zone, ansi) for zone in ZONES for ansi in ANSI_MODES}
    for answers in _ORACLE["modes"].values():
        for seat in (*_SEATS, "write"):
            for key in _scoped_keys(seat):
                assert key in answers, key


@pytest.mark.parametrize("seat", (*_SEATS, "write"))
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_explode_in_a_select_list_answers_as_spark(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str, seat: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    misses = []
    for key in _scoped_keys(seat):
        if _normal(got[key], _literal(key)) != _normal(expected[key], _literal(key)):
            misses.append(f"{key}: spark {expected[key]} repark {got[key]}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_a_literal_array_keeps_the_element_nullability_it_already_had(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    got = measured[mode_key(zone, ansi)]
    for name in ("bare", "seq-leap", "map"):
        sql = got[f"{name}|sql"]
        assert all(field[2] for field in sql["schema"]), sql
        assert (
            sql["schema"] == got[f"{name}|select-expr"]["schema"] == got[f"{name}|expr"]["schema"]
        )
    assert got["bare|sql"]["schema"] == got["bare|typed"]["schema"]
