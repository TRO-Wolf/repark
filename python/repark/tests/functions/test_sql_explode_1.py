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
    NAMED,
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
    return CELLS.get(key.split("|")[0], ("table", ()))[0] in {"none", "subq-unaliased-lit"}


def _literal_constructor(key: str) -> bool:
    name = key.split("|")[0]
    source, items = CELLS[name]
    if source == "subq-unaliased-lit":
        return True
    return any(item.lower().startswith(("explode(array(", "explode(map(")) for item in items)


def _normal(cell: dict[str, Any], literal: bool) -> dict[str, Any]:
    if "error" in cell:
        return {"error": True}
    rows = cell["rows"] if literal else sorted(cell["rows"], key=json.dumps)
    schema = [field[:2] if literal or field[0] == "dom" else field for field in cell["schema"]]
    return {"columns": cell["columns"], "schema": schema, "rows": rows}


def _fexpr_residual(name: str) -> bool:
    return CELLS[name][0] == "none" or name in {"map-view", "map-view-outer"}


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
        if not _literal(key):
            continue
        if "error" in got[key] or "error" in expected[key]:
            if ("error" in got[key]) != ("error" in expected[key]) or got[key].get(
                "error"
            ) != expected[key].get("error"):
                misses.append(f"{key}: spark {expected[key]} repark {got[key]}")
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
def test_f_expr_over_a_range_frame_stays_as_recorded_and_view_f_expr_matches_spark(
    measured: dict[str, dict[str, Any]], zone: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, "true")]
    got = measured[mode_key(zone, "true")]
    assert "__repark_gen_col" in got["bare|expr"]["message"]
    assert _normal(got["view-bare|expr"], False) == _normal(expected["view-bare|expr"], False)
    for key in ("map-view|expr", "map-view-outer|expr"):
        assert got[key]["columns"] == ["id", "col"]
        assert got[key]["schema"] == expected[key]["schema"]
        assert sorted(got[key]["rows"], key=json.dumps) == sorted(
            expected[key]["rows"], key=json.dumps
        )


@pytest.mark.parametrize("zone", ZONES)
def test_dayofmonth_over_an_exploded_date_is_nullable_where_spark_says_not(
    measured: dict[str, dict[str, Any]], zone: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, "true")]["projected-dates|sql"]["schema"]
    got = measured[mode_key(zone, "true")]["projected-dates|sql"]["schema"]
    assert expected == [["d", "date", False], ["dom", "int", False]]
    assert got == [["d", "date", False], ["dom", "int", True]]


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_outer_generators_over_non_nullable_lists_answer_as_spark(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    for key in (
        "seq-date-outer|sql",
        "seq-date-outer|select-expr",
        "seq-int-outer|sql",
        "seq-ts-outer|sql",
        "split-outer|sql",
        "split-null-outer|sql",
        "posexplode-outer-seq|sql",
        "posexplode-outer-split-null|sql",
        "insert-seq-outer|write",
    ):
        assert got[key] == expected[key], key


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_unaliased_explode_exposes_col_in_subqueries_orderby_and_writes(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    for key in (
        "subq-star|sql",
        "subq-col|sql",
        "subq-where|sql",
        "subq-groupby|sql",
        "orderby-col|sql",
        "insert-unaliased|write",
        "insert-overwrite-unaliased|write",
        "ctas-unaliased|write",
    ):
        assert _normal(got[key], False) == _normal(expected[key], False), key
    assert _normal(got["subq-star-lit|sql"], True) == _normal(expected["subq-star-lit|sql"], True)


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_explicit_generator_prefix_aliases_are_kept_or_refused_as_spark(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    for key in (
        "alias-explode-custom|sql",
        "alias-explode-custom-orderby|sql",
        "alias-explode-arr|sql",
        "alias-explode-outer-custom|sql",
    ):
        assert _normal(got[key], False) == _normal(expected[key], False), key
    assert "error" in expected["posexplode-alias-custom|sql"]
    assert "error" in got["posexplode-alias-custom|sql"]
    assert "error" in expected["map-alias-col|sql"]
    assert "error" in got["map-alias-col|sql"]


def _shape(cell: dict[str, Any]) -> dict[str, Any]:
    if "error" in cell:
        return {"error": True}
    rows = sorted(cell["rows"], key=json.dumps)
    return {"columns": cell["columns"], "types": [f[:2] for f in cell["schema"]], "rows": rows}


def _shape_misses(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str, keys: list[str]
) -> list[str]:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    return [
        f"{key}: spark {expected[key]} repark {got[key]}"
        for key in keys
        if _shape(got[key]) != _shape(expected[key])
    ]


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_a_coerced_generator_argument_keeps_spark_names_and_aliases(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    keys = [f"{name}|named" for name in NAMED if name.startswith("r1-")]
    misses = _shape_misses(measured, zone, ansi, keys)
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_a_map_explode_beside_a_col_sibling_answers_key_value(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    keys = [f"{name}|named" for name in NAMED if name.startswith("r2-")]
    misses = _shape_misses(measured, zone, ansi, keys)
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_frame_operations_over_an_unaliased_f_expr_explode_answer_as_spark(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    keys = [key for key in _ORACLE["modes"][mode_key(zone, ansi)] if key.endswith("|frame")]
    assert len(keys) == 6
    misses = _shape_misses(measured, zone, ansi, keys)
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_array_repeat_over_an_array_still_fails_with_or_without_a_generator(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    for name in ("r4-array-repeat-explode-outer", "r4-array-repeat-alone"):
        assert "error" not in expected[f"{name}|named"]
        assert "ListArray expected data type" in got[f"{name}|named"]["message"]
