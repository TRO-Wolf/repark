from __future__ import annotations

import json
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest
from _record_extract_field_aliases_1 import (
    ANSI_MODES,
    CARD_FIELDS,
    FIXTURE,
    KINDS,
    LOWER_FIELDS,
    STRUCT_FIELD_CELLS,
    ZONES,
    cell_key,
    cells,
    lower_key,
    mode_key,
    seat_answers,
    seed,
    struct_field_key,
)

from repark import ReparkSession
from repark.spark import functions as spark_functions
from repark.spark.session import _reset_active_session_for_tests

_ORACLE: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
_SQL_SEATS: tuple[str, ...] = ("sql", "expr", "select-expr")
_SECOND_FAMILY: frozenset[str] = frozenset({"SECOND", "SECONDS", "S", "SEC", "SECS"})
_CANONICAL: dict[str, str] = {
    "Y": "YEAR",
    "YR": "YEAR",
    "YRS": "YEAR",
    "QTR": "QUARTER",
    "MON": "MONTH",
    "MONS": "MONTH",
    "W": "WEEK",
    "D": "DAY",
    "DOW": "DAYOFWEEK",
    "DOW_ISO": "DAYOFWEEK_ISO",
    "H": "HOUR",
    "HR": "HOUR",
    "HRS": "HOUR",
    "M": "MINUTE",
    "MIN": "MINUTE",
    "MINS": "MINUTE",
    "S": "SECOND",
    "SEC": "SECOND",
    "SECS": "SECOND",
}

pytestmark = pytest.mark.xdist_group("test_extract_field_aliases_1")


@pytest.fixture(scope="module")
def measured() -> Iterator[dict[str, dict[str, Any]]]:
    found: dict[str, dict[str, Any]] = {}
    for zone in ZONES:
        for ansi in ANSI_MODES:
            _reset_active_session_for_tests()
            session = ReparkSession.builder.appName("test-extract-field-aliases-1").getOrCreate()
            session.conf.set("spark.sql.session.timeZone", zone)
            session.conf.set("spark.sql.ansi.enabled", ansi)
            seed(session)
            found[mode_key(zone, ansi)] = seat_answers(session, spark_functions)
            session.stop()
    _reset_active_session_for_tests()
    yield found


def _whole(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("dec:"):
        return int(Decimal(value[4:]))
    return value


def _zone_bound(kind: str, zone: str) -> bool:
    return kind == "ts" and zone != "UTC"


def _miss(kind: str, field: str, zone: str, expected: dict[str, Any], got: dict[str, Any]) -> bool:
    if "error" in expected or "error" in got:
        return expected.get("error") != got.get("error")
    upper = field.upper()
    if _zone_bound(kind, zone):
        return got["types"] != expected["types"] and upper not in _SECOND_FAMILY
    if upper in _SECOND_FAMILY:
        return [_whole(value) for value in got["values"]] != [
            _whole(value) for value in expected["values"]
        ]
    return got != expected


def test_the_fixture_holds_every_cell_and_every_mode() -> None:
    assert _ORACLE["spark"] == "4.1.2"
    assert set(_ORACLE["modes"]) == {mode_key(zone, ansi) for zone in ZONES for ansi in ANSI_MODES}
    for answers in _ORACLE["modes"].values():
        for kind, field, form in cells():
            for seat in _SQL_SEATS:
                assert cell_key(kind, field, form, seat) in answers
        for kind in KINDS:
            for field in LOWER_FIELDS:
                for seat in _SQL_SEATS:
                    assert lower_key(kind, field, seat) in answers
        for name in STRUCT_FIELD_CELLS:
            for seat in _SQL_SEATS:
                assert struct_field_key(name, seat) in answers


@pytest.mark.parametrize("seat", _SQL_SEATS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_every_spark_field_spelling_answers_as_spark(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str, seat: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    misses = [
        f"{key}: spark {expected[key]} repark {got[key]}"
        for kind, field, form in cells()
        if _miss(
            kind,
            field,
            zone,
            expected[key := cell_key(kind, field, form, seat)],
            got[key],
        )
    ]
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("seat", _SQL_SEATS)
@pytest.mark.parametrize("zone", ZONES)
def test_an_alias_answers_as_its_canonical_field(
    measured: dict[str, dict[str, Any]], zone: str, seat: str
) -> None:
    got = measured[mode_key(zone, "true")]
    misses = []
    for kind, field, form in cells():
        if form == "smallint":
            continue
        alias = got[cell_key(kind, field, form, seat)]
        canonical = got[cell_key(kind, _CANONICAL.get(field, field), "date_part", seat)]
        if "error" in alias or alias["values"] != canonical["values"]:
            misses.append(f"{kind}|{field}|{form}: {alias} against {canonical}")
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("zone", ZONES)
def test_the_card_fields_cast_to_smallint_as_spark(
    measured: dict[str, dict[str, Any]], zone: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, "true")]
    got = measured[mode_key(zone, "true")]
    for field in CARD_FIELDS:
        key = cell_key("date", field, "smallint", "sql")
        assert got[key] == expected[key], key
        assert got[key]["types"] == ["smallint"]


@pytest.mark.parametrize("seat", _SQL_SEATS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_lowercase_fields_with_fractional_seconds_answer_as_spark(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str, seat: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    misses = [
        f"{key}: spark {expected[key]} repark {got[key]}"
        for kind in KINDS
        for field in LOWER_FIELDS
        if _miss(
            kind,
            field,
            zone,
            expected[key := lower_key(kind, field, seat)],
            got[key],
        )
    ]
    assert not misses, "\n".join(misses)


@pytest.mark.parametrize("seat", _SQL_SEATS)
@pytest.mark.parametrize("ansi", ANSI_MODES)
@pytest.mark.parametrize("zone", ZONES)
def test_extract_inside_named_struct_field_access_answers_as_spark(
    measured: dict[str, dict[str, Any]], zone: str, ansi: str, seat: str
) -> None:
    expected = _ORACLE["modes"][mode_key(zone, ansi)]
    got = measured[mode_key(zone, ansi)]
    misses = []
    for name, (kind, _expr) in STRUCT_FIELD_CELLS.items():
        key = struct_field_key(name, seat)
        if kind == "ts-lit" and zone != "UTC":
            if "error" in got[key] or got[key]["types"] != expected[key]["types"]:
                misses.append(f"{key}: spark {expected[key]} repark {got[key]}")
            continue
        if got[key] != expected[key]:
            misses.append(f"{key}: spark {expected[key]} repark {got[key]}")
    assert not misses, "\n".join(misses)
