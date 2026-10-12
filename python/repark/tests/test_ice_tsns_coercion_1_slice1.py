"""Slice 1 round 1 facade pins: the pandas gate case and twelve unchanged answers.

pins: ice-tsns-coercion-1/C-005
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _ice_tsns_coercion_1_doors as doors
import _record_ice_tsns_coercion_1_twins as recorder
import pandas as pd

from repark import ReparkSession, _native

TESTS = Path(__file__).resolve().parent
FIXTURE = TESTS / "ice_tsns_coercion_1_twins.json"
FORMS = ("coalesce", "array", "case")
SOURCES = ("ns", "tzns_utc")
TARGET_FOR = {"ns": "ts_ns", "tzns_utc": "tz_ns"}
DOOR_ZONE = (("insert_select", "UTC"), ("df_append", "America/New_York"))
SIBLING = "untyped_null"


def _load_fixture() -> dict[str, Any]:
    """Return the slice 0 twin fixture."""
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _counts() -> tuple[int, int]:
    """Return the gate-open and fallback counters."""
    return _native._tsns_gate_counts()


def _statement_outcome(
    fixture: dict[str, Any],
    zone: str,
    target: str,
    source: str,
    form: str,
    door: str,
) -> dict[str, Any]:
    """Return the recorded statement-role outcome for one cell."""
    key = f"{zone}|{target}|{source}|{form}|{SIBLING}"
    slot = fixture["statement"][key][fixture["doors"].index(door)]
    return fixture["outcomes"][slot]


def _normalized_read(read: dict[str, Any]) -> dict[str, Any]:
    """Return ``read`` with rows in a stable order; the read has no ORDER BY."""
    if "rows" not in read:
        return read
    return {**read, "rows": sorted(read["rows"], key=repr)}


def _open_session() -> Any:
    """Return one bare Spark-door session."""
    return ReparkSession.builder.appName("ice-tsns-coercion-1-slice1").getOrCreate()


def test_pandas_datetime64_ns_frame_opens_the_gate() -> None:
    """pins: ice-tsns-coercion-1/C-005 — a datetime64[ns] frame scans as Timestamp(ns)."""
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
        spark.sql("SELECT t FROM v_ns_frame").toArrow()
        opened, fallback = _counts()
        assert opened - before_open >= 1
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


def test_twelve_statements_equal_the_recorded_statement_role(tmp_path: Path) -> None:
    """pins: ice-tsns-coercion-1/C-005 — this round changed no stored value or read schema."""
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
                    outcome = doors.measure(
                        spark, warehouse, target, source, form, SIBLING, "statement", door
                    )
                    assert outcome == _statement_outcome(
                        fixture, zone, target, source, form, door
                    ), cell
                    statement = recorder.read_sql(spark, source, form, SIBLING, "statement")
                    read = _normalized_read(recorder.capture(spark, statement))
                    want = _normalized_read(
                        fixture["reads"][f"{zone}|{source}|{form}|{SIBLING}|statement"]
                    )
                    assert read == want, cell
        finally:
            spark.stop()
