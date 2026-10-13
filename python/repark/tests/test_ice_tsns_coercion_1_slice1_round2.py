"""Slice 1 round 2 facade pins, narrowed: QD, pin 8b and statement-role reads.

The carve-out, corrected-twin and remaining-twin pins are deleted: union doors,
frame doors and trigger-only statements are main's again, and the kept twin subset
is pinned by the narrow census. The corrected-twin fixture stays on disk unused.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import _record_ice_tsns_coercion_1_twins as recorder
from test_ice_tsns_coercion_1_slice1 import (
    PIN6_FORMS,
    PIN6_SOURCES,
    SIBLING,
    TARGET_FOR,
    _counts,
    _denoised,
    _load_fixture,
    _normalized_read,
    _role_outcome,
)

_QD_UPDATE_FORMS = ("array", "case", "coalesce", "if")
_PIN8B_DOORS = ("insert_select", "df_append")
_PIN8B_ZONES = ("UTC", "America/New_York")
EXPECTED_QD_CELLS = 155
EXPECTED_PIN8B_CELLS = 40
_STALE_COLUMN = re.compile(r"\bi\.tzns\b")
_STALE_BARE_COLUMN = re.compile(r"\btzns\b")


def _live_text(text: str) -> str:
    """Return recorded error text with the stale column name replaced."""
    return _STALE_BARE_COLUMN.sub("tzns_utc", _STALE_COLUMN.sub("i.tzns_utc", text))


def _in_qd(zone: str, target: str, source: str, form: str, door: str) -> bool:
    """Return whether a cell pins head against main's recorded statement."""
    if door == "update_scalar_subquery" and form in _QD_UPDATE_FORMS:
        return True
    if door != "explain_analyze_update":
        return False
    if form == "union":
        return True
    return form == "coalesce" and source == "tzns_utc" and target == "ts_ns"


def _iter_pin6_keys(
    fixture: dict[str, Any],
) -> Iterator[tuple[str, str, str, str, str, str]]:
    """Yield every pin-6-space key with its door slots."""
    for key in sorted(fixture["twin"]):
        zone, target, source, form, sibling = key.split("|")
        if form not in PIN6_FORMS or sibling != SIBLING:
            continue
        for door in fixture["doors"]:
            yield key, zone, target, source, form, door


def test_pin6_qd_cells_equal_main(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — 155 cells keep main's statement role."""
    fixture = _load_fixture()
    count = 0
    for index, zone in enumerate(fixture["zones"]):
        warehouse = tmp_path / f"qd-{index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for key, cell_zone, target, source, form, door in _iter_pin6_keys(fixture):
                if cell_zone != zone:
                    continue
                if not _in_qd(zone, target, source, form, door):
                    continue
                cell = f"{key}|{door}"
                outcome = doors.measure(
                    spark, warehouse, target, source, form, SIBLING, "statement", door
                )
                want = _role_outcome(
                    fixture, "statement", zone, target, source, form, SIBLING, door
                )
                if source == "tzns_utc" and "error" in want:
                    want = {**want, "error": _live_text(want["error"])}
                assert _denoised(outcome) == _denoised(want), cell
                count += 1
        finally:
            spark.stop()
    assert count == EXPECTED_QD_CELLS


def test_pin8b_no_fallback_on_plain_ns_cells(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — plain ns-target cells never fall back.

    Kept `insert_select` feeds (`coalesce`, `case`, `if`) store the twin; the `array`
    index, the `union` carrier and every frame-door cell are main's again.
    """
    fixture = _load_fixture()
    count = 0
    for door in _PIN8B_DOORS:
        for zone in _PIN8B_ZONES:
            warehouse = tmp_path / f"pin8b-{door}-{zone.replace('/', '_')}"
            warehouse.mkdir()
            spark = doors.open_session(zone, warehouse)
            try:
                for form in PIN6_FORMS:
                    for source in PIN6_SOURCES:
                        target = TARGET_FOR[source]
                        cell = f"{zone}|{target}|{source}|{form}|{door}"
                        before = _counts()[1]
                        outcome = doors.measure(
                            spark,
                            warehouse,
                            target,
                            source,
                            form,
                            SIBLING,
                            "statement",
                            door,
                        )
                        assert _counts()[1] - before == 0, cell
                        role = (
                            "twin"
                            if door == "insert_select" and form in ("coalesce", "case", "if")
                            else "statement"
                        )
                        want = _role_outcome(
                            fixture, role, zone, target, source, form, SIBLING, door
                        )
                        assert _denoised(outcome) == _denoised(want), cell
                        count += 1
            finally:
                spark.stop()
    assert count == EXPECTED_PIN8B_CELLS


def test_pin6_reads_equal_main_statement(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — 45 reads keep main's statement role."""
    fixture = _load_fixture()
    count = 0
    for index, zone in enumerate(fixture["zones"]):
        warehouse = tmp_path / f"pin6read-{index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for source in PIN6_SOURCES:
                for form in PIN6_FORMS:
                    if source == "tzns_utc" and form == "union":
                        continue
                    cell = f"read|{zone}|{source}|{form}"
                    statement = recorder.read_sql(spark, source, form, SIBLING, "statement")
                    read = _normalized_read(recorder.capture(spark, statement))
                    want = _normalized_read(
                        fixture["reads"][f"{zone}|{source}|{form}|{SIBLING}|statement"]
                    )
                    assert read == want, cell
                    count += 1
        finally:
            spark.stop()
    assert count == 45


def test_pin6_union_tzns_read_pins_field_id(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — the union read carries field id 4."""
    fixture = _load_fixture()
    count = 0
    for index, zone in enumerate(fixture["zones"]):
        warehouse = tmp_path / f"pin6field-{index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            cell = f"read|{zone}|tzns_utc|union"
            statement = recorder.read_sql(spark, "tzns_utc", "union", SIBLING, "statement")
            read = _normalized_read(recorder.capture(spark, statement))
            want = _normalized_read(fixture["reads"][f"{zone}|tzns_utc|union|{SIBLING}|statement"])
            assert read["rows"] == want["rows"], cell
            assert read["type"] == want["type"], cell
            assert read["nullable"] == want["nullable"], cell
            assert read["metadata"] == {"PARQUET:field_id": "4"}, cell
            count += 1
        finally:
            spark.stop()
    assert count == 5
