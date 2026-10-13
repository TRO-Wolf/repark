"""Slice 1 facade pins: round 1 gate cases plus round 2 untyped-NULL rewrites.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import _record_ice_tsns_coercion_1_twins as recorder
import pandas as pd
import pyarrow as pa

from repark import ReparkSession, _native

TESTS = Path(__file__).resolve().parent
FIXTURE = TESTS / "ice_tsns_coercion_1_twins.json"
FORMS = ("coalesce", "array", "case")
SOURCES = ("ns", "tzns_utc")
TARGET_FOR = {"ns": "ts_ns", "tzns_utc": "tz_ns"}
DOOR_ZONE = (("insert_select", "UTC"), ("df_append", "America/New_York"))
SIBLING = "untyped_null"
PIN6_FORMS = ("coalesce", "array", "case", "if", "union")
PIN6_SOURCES = ("ns", "tzns_utc")
PIN7_FORMS = ("coalesce", "array", "case", "if", "nvl", "ifnull", "union")
PIN7_SIBLINGS = (
    "typed_null_ts",
    "typed_null_ntz",
    "us_column_ts",
    "us_column_ntz",
    "us_literal_ts",
    "us_literal_ntz",
    "date_literal",
)
PIN7_SWEEP_FORMS = ("coalesce", "nvl", "union")


def _load_fixture() -> dict[str, Any]:
    """Return the slice 0 twin fixture."""
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _counts() -> tuple[int, int]:
    """Return the gate-open and fallback counters."""
    return _native._tsns_gate_counts()


def _role_outcome(
    fixture: dict[str, Any],
    role: str,
    zone: str,
    target: str,
    source: str,
    form: str,
    sibling: str,
    door: str,
) -> dict[str, Any]:
    """Return the recorded role outcome for one cell."""
    key = f"{zone}|{target}|{source}|{form}|{sibling}"
    slot = fixture[role][key][fixture["doors"].index(door)]
    return fixture["outcomes"][slot]


def _pairs_for(
    fixture: dict[str, Any], forms: tuple[str, ...], sibling: str
) -> list[tuple[str, str]]:
    """Return the fixture's (source, target) pairs for ``forms`` and ``sibling``."""
    pairs = {
        (key.split("|")[2], key.split("|")[1])
        for key in fixture["twin"]
        if key.split("|")[3] in forms and key.split("|")[4] == sibling
    }
    return sorted(pairs)


def _normalized_read(read: dict[str, Any]) -> dict[str, Any]:
    """Return ``read`` with rows in a stable order; the read has no ORDER BY."""
    if "rows" not in read:
        return read
    return {**read, "rows": sorted(read["rows"], key=repr)}


_TABLE_NAME = re.compile(r"ice\.ns\.t\d+|__repark_tt_\d+")
_COMPARE_CHARS = 200


def _denoised(outcome: dict[str, Any]) -> dict[str, Any]:
    """Return ``outcome`` with session-local table counters removed.

    ``error_text`` already truncates to 240 chars, so a counter-width
    difference shifts the tail; the comparison keeps the stripped head.
    """
    if "error" not in outcome:
        return outcome
    text: str = outcome["error"]
    stripped: str = _TABLE_NAME.sub("", text)
    return {**outcome, "error": stripped[:_COMPARE_CHARS]}


def _open_session() -> Any:
    """Return one bare Spark-door session."""
    return ReparkSession.builder.appName("ice-tsns-coercion-1-slice1").getOrCreate()


def test_pandas_datetime64_ns_frame_scans_as_microseconds() -> None:
    """pins: ice-tsns-coercion-1/C-005 — a datetime64[ns] frame scans as Timestamp(us)."""
    pdf = pd.DataFrame(
        {
            "id": [1, 2],
            "t": pd.Series(
                [
                    pd.Timestamp("2024-01-15 12:30:00.123456789"),
                    pd.Timestamp("2020-06-01 00:00:00.987654321"),
                ],
                dtype="datetime64[ns]",
            ),
        }
    )
    spark = _open_session()
    try:
        spark.createDataFrame(pdf).createOrReplaceTempView("v_ns_frame")
        before_open, before_fallback = _counts()
        table = spark.sql("SELECT t FROM v_ns_frame").toArrow()
        opened, fallback = _counts()
        assert table.schema.types[0] == pa.timestamp("us")
        assert opened == before_open
        assert fallback == before_fallback
    finally:
        spark.stop()


def test_microsecond_frame_leaves_the_gate_closed() -> None:
    """pins: ice-tsns-coercion-1/C-005 — the closed control for the pandas case."""
    pdf = pd.DataFrame({"id": [1, 2], "n": [7, 8]})
    spark = _open_session()
    try:
        spark.createDataFrame(pdf).createOrReplaceTempView("v_us_frame")
        before_open, before_fallback = _counts()
        spark.sql("SELECT n FROM v_us_frame").toArrow()
        opened, fallback = _counts()
        assert opened == before_open
        assert fallback == before_fallback
    finally:
        spark.stop()


def test_twelve_statements_equal_the_recorded_twin_role(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — kept feeds store the twin, the rest main's."""
    fixture = _load_fixture()
    for door, zone in DOOR_ZONE:
        warehouse = tmp_path / ("utc" if zone == "UTC" else "ny")
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for form in FORMS:
                for source in SOURCES:
                    target = TARGET_FOR[source]
                    cell = f"{zone}|{target}|{source}|{form}|{door}"
                    role = (
                        "twin"
                        if door == "insert_select" and form in ("coalesce", "case")
                        else "statement"
                    )
                    outcome = doors.measure(
                        spark, warehouse, target, source, form, SIBLING, "statement", door
                    )
                    assert outcome == _role_outcome(
                        fixture, role, zone, target, source, form, SIBLING, door
                    ), cell
                    statement = recorder.read_sql(spark, source, form, SIBLING, "statement")
                    read = _normalized_read(recorder.capture(spark, statement))
                    want = _normalized_read(
                        fixture["reads"][f"{zone}|{source}|{form}|{SIBLING}|statement"]
                    )
                    assert read == want, cell
        finally:
            spark.stop()


def test_pin7_other_cells_equal_main_statement_role(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — every other sibling and form stays main's."""
    fixture = _load_fixture()
    pairs = _pairs_for(fixture, PIN7_FORMS, PIN7_SIBLINGS[0])
    assert pairs
    count = 0
    for index, (door, zone) in enumerate(DOOR_ZONE):
        warehouse = tmp_path / f"pin7-matrix-{index}"
        warehouse.mkdir()
        spark = doors.open_session(zone, warehouse)
        try:
            for sibling in PIN7_SIBLINGS:
                for form in PIN7_FORMS:
                    for source, target in pairs:
                        cell = f"{zone}|{target}|{source}|{form}|{sibling}|{door}"
                        outcome = doors.measure(
                            spark, warehouse, target, source, form, sibling, "statement", door
                        )
                        want = _role_outcome(
                            fixture, "statement", zone, target, source, form, sibling, door
                        )
                        assert _denoised(outcome) == _denoised(want), cell
                        count += 1
        finally:
            spark.stop()
    sweep = tmp_path / "pin7-sweep"
    sweep.mkdir()
    spark = doors.open_session("UTC", sweep)
    try:
        for form in PIN7_SWEEP_FORMS:
            for door in fixture["doors"]:
                cell = f"UTC|ts_ns|ns|{form}|typed_null_ts|{door}"
                outcome = doors.measure(
                    spark, sweep, "ts_ns", "ns", form, "typed_null_ts", "statement", door
                )
                want = _role_outcome(
                    fixture, "statement", "UTC", "ts_ns", "ns", form, "typed_null_ts", door
                )
                assert _denoised(outcome) == _denoised(want), cell
                count += 1
    finally:
        spark.stop()
    assert count > 0
