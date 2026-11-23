from __future__ import annotations

import json
from collections.abc import Callable, Iterator
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

    def open(self) -> Callable[[str], Any]:
        self.writer.stop()
        self.reader = _session(self.root, self.zone, self.ansi)
        return self.read

    def read(self, table: str) -> Any:
        assert self.reader is not None
        leaf = table.rsplit(".", 1)[1]
        return self.reader.read.format("iceberg").load(str(self.root / "wh" / "ns" / leaf))

    def close(self) -> None:
        if self.reader is not None:
            self.reader.stop()


def _literal(key: str) -> bool:
    return CELLS.get(key.split("|")[0], ("table", ()))[0] == "none"


def _literal_constructor(key: str) -> bool:
    items = CELLS[key.split("|")[0]][1]
    return any(item.lower().startswith(("explode(array(", "explode(map(")) for item in items)


def _normal(cell: dict[str, Any], literal: bool) -> dict[str, Any]:
    if "error" in cell:
        return {"error": True}
    rows = cell["rows"] if literal else sorted(cell["rows"], key=json.dumps)
    schema = [field[:2] if literal or field[0] == "dom" else field for field in cell["schema"]]
    return {"columns": cell["columns"], "schema": schema, "rows": rows}


def _fexpr_residual(name: str) -> bool:
    source, items = CELLS[name]
    return source == "none" or any(" AS " not in item and "explode" in item for item in items)


def _scoped_keys(seat: str) -> list[str]:
    if seat == "write":
        return [f"{name}|write" for name in WRITES]
    return [
        f"{name}|{seat}"
        for name, (source, _items) in CELLS.items()
        if seat == "sql"
        or (source in EXPR_SOURCES and not (seat == "expr" and _fexpr_residual(name)))
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


@pytest.mark.parametrize("seat", _SEATS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_a_literal_cell_has_spark_nullability_but_for_array_and_map_literals(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str, seat: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    misses = []
    for key in _scoped_keys(seat):
        if not _literal(key) or "error" in got[key] or "error" in expected[key]:
            continue
        for want, have in zip(expected[key]["schema"], got[key]["schema"], strict=True):
            residual = _literal_constructor(key) and want[:2] == have[:2] and have[2] > want[2]
            if want != have and not residual:
                misses.append(f"{key}: spark {want} repark {have}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_an_array_literal_element_stays_nullable_as_the_typed_explode_has_it(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    assert (
        expected["bare|sql"]["schema"]
        == expected["bare|typed"]["schema"]
        == [["col", "int", False]]
    )
    assert got["bare|sql"]["schema"] == got["bare|typed"]["schema"] == [["col", "int", True]]
    assert (
        got["seq-leap|sql"]["schema"]
        == expected["seq-leap|sql"]["schema"]
        == [["d", "date", False]]
    )


@pytest.mark.parametrize("zone", ZONES)
def test_f_expr_over_a_range_frame_and_an_unaliased_f_expr_stay_as_recorded(
    measured: dict[str, dict[str, Any]], zone: str
) -> None:
    got = measured[mode_key(zone, "true")]
    assert "__repark_gen_col" in got["bare|expr"]["message"]
    assert got["view-bare|expr"]["columns"] == ["id", "explode(arr)"]
    assert got["view-bare|expr"]["schema"][1][0] == "col"
    assert got["view-bare|expr"]["rows"] == got["view-bare|sql"]["rows"]


@pytest.mark.parametrize("zone", ZONES)
def test_dayofmonth_over_an_exploded_date_is_nullable_where_spark_says_not(
    measured: dict[str, dict[str, Any]], zone: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, "true")]["projected-dates|sql"]["schema"]
    got = measured[mode_key(zone, "true")]["projected-dates|sql"]["schema"]
    assert expected == [["d", "date", False], ["dom", "int", False]]
    assert got == [["d", "date", False], ["dom", "int", True]]
