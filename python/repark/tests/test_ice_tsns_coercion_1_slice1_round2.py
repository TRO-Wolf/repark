"""Slice 1 round 2 facade pins: carve-out, corrected oracles, QD and pin 8b.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
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
    TESTS,
    _counts,
    _denoised,
    _load_fixture,
    _normalized_read,
    _role_outcome,
)

_ARRAY_LTZ_DOORS = frozenset(
    {
        "aggregate_insert",
        "branch_insert",
        "branch_merge",
        "branch_update",
        "cte_insert",
        "distinct_insert",
        "explain_analyze_insert",
        "explain_analyze_update",
        "insert_positional",
        "insert_reordered",
        "insert_replace_where",
        "insert_select",
        "insert_values",
        "join_insert",
        "lambda_first_insert",
        "merge_by_source",
        "merge_by_source_mor",
        "merge_update",
        "merge_update_mor",
        "merge_update_star",
        "merge_update_subquery",
        "merge_update_subquery_mor",
        "prepare_execute_insert",
        "scalar_subquery_insert",
        "sort_limit_insert",
        "subquery_insert",
        "union_insert",
        "union_null_insert",
        "update_nowhere",
        "update_nowhere_mor",
        "wap_insert",
        "window_insert",
    }
)
_CARVE_UPDATE_DOORS = frozenset(
    {"branch_update", "explain_analyze_update", "update_nowhere", "update_nowhere_mor"}
)
_EXPLAIN_PREPARE_DOORS = frozenset({"explain_analyze_insert", "prepare_execute_insert"})
_QE_UNION_DOORS = frozenset({"union_null_insert", "df_union_null_append"})
_QD_UPDATE_FORMS = ("array", "case", "coalesce", "if")
_PIN8B_DOORS = ("insert_select", "df_append")
_PIN8B_ZONES = ("UTC", "America/New_York")
EXPECTED_PIN6_CELLS = 8967
EXPECTED_CARVE_CELLS = 258
EXPECTED_QE_CELLS = 220
EXPECTED_QD_CELLS = 155
EXPECTED_PIN8B_CELLS = 40
_STALE_COLUMN = re.compile(r"\bi\.tzns\b")
_STALE_BARE_COLUMN = re.compile(r"\btzns\b")


def _load_corrected() -> dict[str, Any]:
    """Return the corrected-twin oracle fixture."""
    text: str = (TESTS / "ice_tsns_coercion_1_slice1_corrected_twins.json").read_text()
    return json.loads(text)


def _live_text(text: str) -> str:
    """Return recorded error text with the stale column name replaced."""
    return _STALE_BARE_COLUMN.sub("tzns_utc", _STALE_COLUMN.sub("i.tzns_utc", text))


def _in_carve(zone: str, target: str, source: str, form: str, door: str) -> bool:
    """Return whether a cell stays main's behind the fallback (Q-A)."""
    if door == "df_functions_append" and target == "ltz" and form == "array":
        return True
    if target == "ltz" and form == "array" and source == "ns":
        return door in _ARRAY_LTZ_DOORS and zone != "UTC"
    if door in _CARVE_UPDATE_DOORS and target == "ltz" and form == "coalesce":
        return True
    if door == "distinct_insert" and target == "ltz" and form == "union":
        return True
    if door in _EXPLAIN_PREPARE_DOORS and target == "tz_ns" and source == "ns":
        return True
    return (
        door == "explain_analyze_update"
        and target == "tz_ns"
        and source == "ns"
        and form != "union"
    )


def _in_qe(zone: str, target: str, source: str, form: str, door: str) -> bool:
    """Return whether a cell takes its oracle from the corrected twins."""
    if target not in ("ts_ns", "tz_ns"):
        return False
    if door in _QE_UNION_DOORS:
        return True
    return door == "df_functions_append" and form == "array"


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


def test_pin6_remaining_cells_equal_twin(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — 8,967 cells store their recorded twin."""
    fixture = _load_fixture()
    count = 0
    for index, zone in enumerate(fixture["zones"]):
        warehouse = tmp_path / f"pin6-{index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for key, cell_zone, target, source, form, door in _iter_pin6_keys(fixture):
                if cell_zone != zone:
                    continue
                if _in_carve(zone, target, source, form, door):
                    continue
                if _in_qe(zone, target, source, form, door):
                    continue
                if _in_qd(zone, target, source, form, door):
                    continue
                cell = f"{key}|{door}"
                outcome = doors.measure(
                    spark, warehouse, target, source, form, SIBLING, "statement", door
                )
                want = _role_outcome(fixture, "twin", zone, target, source, form, SIBLING, door)
                assert _denoised(outcome) == _denoised(want), cell
                count += 1
        finally:
            spark.stop()
    assert count == EXPECTED_PIN6_CELLS


def test_pin6_carve_out_stays_main_with_fallback(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — 258 DML-store-cast cells stay main's."""
    fixture = _load_fixture()
    count = 0
    for index, zone in enumerate(fixture["zones"]):
        warehouse = tmp_path / f"carve-{index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for key, cell_zone, target, source, form, door in _iter_pin6_keys(fixture):
                if cell_zone != zone:
                    continue
                if not _in_carve(zone, target, source, form, door):
                    continue
                cell = f"{key}|{door}"
                before = _counts()[1]
                outcome = doors.measure(
                    spark, warehouse, target, source, form, SIBLING, "statement", door
                )
                assert _counts()[1] - before >= 1, cell
                want = _role_outcome(
                    fixture, "statement", zone, target, source, form, SIBLING, door
                )
                assert _denoised(outcome) == _denoised(want), cell
                count += 1
        finally:
            spark.stop()
    assert count == EXPECTED_CARVE_CELLS


def test_pin6_qe_cells_equal_corrected_oracle(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — 220 cells store the corrected oracle."""
    fixture = _load_fixture()
    oracle = _load_corrected()
    count = 0
    kinds: dict[int, int] = {1: 0, 2: 0}
    for index, zone in enumerate(fixture["zones"]):
        warehouse = tmp_path / f"qe-{index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for key, cell_zone, target, source, form, door in _iter_pin6_keys(fixture):
                if cell_zone != zone:
                    continue
                if not _in_qe(zone, target, source, form, door):
                    continue
                cell = f"{key}|{door}"
                oracle_key = f"{zone}|{target}|{source}|{form}|{door}"
                outcome = doors.measure(
                    spark, warehouse, target, source, form, SIBLING, "statement", door
                )
                want = {k: v for k, v in oracle[oracle_key].items() if k != "oracle"}
                assert outcome == want, cell
                kinds[oracle[oracle_key]["oracle"]] += 1
                count += 1
        finally:
            spark.stop()
    assert count == EXPECTED_QE_CELLS
    assert kinds[1] == EXPECTED_QE_CELLS


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
    """pins: ice-tsns-coercion-1/C-005 — plain ns-target cells never fall back."""
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
                        want = _role_outcome(
                            fixture, "twin", zone, target, source, form, SIBLING, door
                        )
                        assert _denoised(outcome) == _denoised(want), cell
                        count += 1
            finally:
                spark.stop()
    assert count == EXPECTED_PIN8B_CELLS


def test_pin6_reads_equal_twin(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — 45 reads store their recorded twin."""
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
                        fixture["reads"][f"{zone}|{source}|{form}|{SIBLING}|twin"]
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
            want = _normalized_read(fixture["reads"][f"{zone}|tzns_utc|union|{SIBLING}|twin"])
            assert read["rows"] == want["rows"], cell
            assert read["type"] == want["type"], cell
            assert read["nullable"] == want["nullable"], cell
            assert read["metadata"] == {"PARQUET:field_id": "4"}, cell
            count += 1
        finally:
            spark.stop()
    assert count == 5
