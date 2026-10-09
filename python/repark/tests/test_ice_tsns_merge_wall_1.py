"""ICE-TSNS-MERGE-WALL-1: every write door stores the same nanosecond wall.

A ``timestamp_ns`` column is a wall clock. An instant written into it stores its wall in the
session zone, a wall is stored as it is, and no digit below the microsecond is dropped, through
every door. Spark 4.1.2 with Iceberg 1.11.0 cannot read or write the type, so the expectation is
the rule INSERT already follows (ICE-TSNS-SQL-1), computed here with ``zoneinfo``.

Every other target type is a control: its cells must answer exactly what main ``40fc916f``
answered, recorded in ``ice_tsns_merge_wall_1_main.json`` for one zone, the one with a DST rule.
The carries and the other zones' controls are pinned at the Rust door
(``crates/repark-spark/tests/timestamp_ns_wall_doors.rs``).

pins: ice-tsns-merge-wall-1/C-004, C-005, C-006, C-007, C-010, C-011, C-012, C-016, C-019
pins: ice-tsns-merge-wall-1/C-023, C-025
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _ice_tsns_merge_wall_1_doors as doors
import pyarrow as pa
import pytest

from repark import ReparkSession

MAIN: dict[str, Any] = json.loads(
    Path(__file__).with_name("ice_tsns_merge_wall_1_main.json").read_text(encoding="utf-8")
)["cells"]
NS_WALL = "timestamp[ns]"
INSTANT_SOURCES = ("l", "tzns")
INSTANT_WRONG_DOORS = (
    "insert_overwrite",
    "merge_insert",
    "merge_insert_star",
    "merge_insert_literal",
    "merge_update",
    "merge_update_star",
    "merge_update_literal",
    "df_overwrite_partitions",
    "df_insert_into_overwrite",
)
UNFILTERED_UPDATE_SOURCES = ("l", "n", "tzns")
CONTROL_TARGETS = tuple(target for target in doors.TARGETS if target != "ts_ns")
CONTROL_ZONE = doors.CONTROL_ZONE


def main_cell(zone: str, target: str, door: str, part: str) -> dict[str, Any]:
    """Return what main answered in one cell."""
    return MAIN[doors.cell_key(zone, target, door, part)]


def rule_cell(zone: str, source: str) -> dict[str, Any]:
    """Return the cell the rule demands of a ``timestamp_ns`` target."""
    return {
        "type": NS_WALL,
        "ids": list(doors.IDS),
        "values": doors.expected_wall(source, zone),
    }


def main_was_wrong(zone: str, door: str, source: str) -> bool:
    """Return whether main stored a wrong ``timestamp_ns`` value in this cell."""
    if zone == "UTC" or source not in INSTANT_SOURCES:
        return False
    return door in INSTANT_WRONG_DOORS or (door == "update_literal" and source == "l")


def main_refused_an_answer(door: str, source: str) -> bool:
    """Return whether main raised the raw Arrow error where this unit now answers."""
    return door == "update_column" and source in UNFILTERED_UPDATE_SOURCES


def expected_wall_cell(zone: str, door: str, source: str) -> dict[str, Any]:
    """Return the expected ``timestamp_ns`` cell: the rule, or main's ratified refusal."""
    recorded = main_cell(zone, "ts_ns", door, source)
    if "error" in recorded and not main_refused_an_answer(door, source):
        return recorded
    return rule_cell(zone, source)


def measured(tmp_path: Path, zone: str, target: str, door: str) -> dict[str, Any]:
    """Run one door in a fresh session and return part -> cell."""
    spark = doors.open_session(zone, tmp_path)
    try:
        return doors.measure(spark, target, door)
    finally:
        spark.stop()


def test_main_recorded_the_rule_wherever_it_answered_right() -> None:
    """The fixture is main's: right on 178 door cells, wrong on 38, a raw Arrow error on 9."""
    right = wrong = raw_errors = 0
    for zone in doors.ZONES:
        for door in doors.DOORS:
            for source in doors.SOURCES:
                recorded = main_cell(zone, "ts_ns", door, source)
                if "error" in recorded:
                    raw_errors += main_refused_an_answer(door, source)
                    continue
                if main_was_wrong(zone, door, source):
                    assert recorded["values"] != doors.expected_wall(source, zone), (zone, door)
                    wrong += 1
                else:
                    assert recorded == rule_cell(zone, source), (zone, door, source)
                    right += 1
    assert (right, wrong, raw_errors) == (178, 38, 9)


@pytest.mark.parametrize("door", list(doors.DOORS))
@pytest.mark.parametrize("zone", doors.ZONES)
def test_every_door_stores_the_session_wall_in_timestamp_ns(
    tmp_path: Path, zone: str, door: str
) -> None:
    """Each source type through ``door`` stores the rule's nanoseconds, or main's refusal."""
    cells = measured(tmp_path, zone, "ts_ns", door)
    for source in doors.SOURCES:
        assert cells[source] == expected_wall_cell(zone, door, source), (zone, door, source)


@pytest.mark.parametrize("door", list(doors.DOORS))
@pytest.mark.parametrize("target", CONTROL_TARGETS)
def test_control_targets_answer_what_main_answered(tmp_path: Path, target: str, door: str) -> None:
    """A ``timestamptz_ns`` target and the microsecond targets, v3 and v2, do not move."""
    cells = measured(tmp_path, CONTROL_ZONE, target, door)
    for source in doors.SOURCES:
        recorded = main_cell(CONTROL_ZONE, target, door, source)
        assert cells[source] == recorded, (target, door, source)


@pytest.mark.parametrize("zone", doors.ZONES)
def test_true_nanosecond_inputs_keep_every_digit(tmp_path: Path, zone: str) -> None:
    """A nanosecond source keeps its digits below the microsecond through every door."""
    for door in ("insert_select", "merge_insert", "merge_update", "update_column", "df_arrow"):
        warehouse = tmp_path / door
        warehouse.mkdir()
        cells = measured(warehouse, zone, "ts_ns", door)
        for source in ("ns", "tzns"):
            stored = cells[source]["values"]
            assert [value % 1_000 for value in stored] == [
                value % 1_000 for value in doors.NANOS
            ], (zone, door, source)


FAR = "TIMESTAMP '3000-01-01 00:00:00.000001+00:00'"
NEAR = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'"
NEAR_NEW_YORK_WALL = 1_767_305_045_123_456_000
OVERFLOW_DOORS = {
    "insert_select": "INSERT INTO ice.ns.t SELECT id, c FROM ice.ns.far",
    "insert_overwrite": "INSERT OVERWRITE ice.ns.t SELECT id, c FROM ice.ns.far",
    "merge_insert": (
        "MERGE INTO ice.ns.t t USING ice.ns.far s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.c)"
    ),
    "merge_update": (
        "MERGE INTO ice.ns.t t USING ice.ns.far s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.c"
    ),
}


def open_edge_session(tmp_path: Path, ansi: str) -> Any:
    """Open a New York session with a ``TIMESTAMP`` source holding a value past 2262."""
    spark = (
        ReparkSession.builder.appName("ice-tsns-merge-wall-1-edge")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(tmp_path))
    spark.sql("CREATE NAMESPACE ice.ns")
    v3 = "USING iceberg TBLPROPERTIES ('format-version' = '3')"
    spark.sql(f"CREATE TABLE ice.ns.far (id INT, c TIMESTAMP) {v3}")
    spark.sql(f"INSERT INTO ice.ns.far VALUES (1, {FAR}), (2, {NEAR}), (3, NULL)")
    spark.sql(f"CREATE TABLE ice.ns.t (id INT, v timestamp_ns) {v3}")
    return spark


def stored_ticks(spark: Any) -> list[int | None]:
    """Return the int64 ticks of ``ice.ns.t.v`` ordered by ``id``."""
    table = spark.sql("SELECT v FROM ice.ns.t ORDER BY id").to_arrow()
    assert str(table.column("v").type) == NS_WALL
    return table.column("v").cast("int64").to_pylist()


@pytest.mark.parametrize("door", list(OVERFLOW_DOORS))
def test_an_instant_past_2262_refuses_like_insert_under_ansi(tmp_path: Path, door: str) -> None:
    """Under ANSI every door raises INSERT's ``CAST_OVERFLOW``, not a raw Arrow error."""
    spark = open_edge_session(tmp_path, "true")
    try:
        if door == "merge_update":
            spark.sql("INSERT INTO ice.ns.t VALUES (1, NULL), (2, NULL), (3, NULL)")
        with pytest.raises(Exception, match=r"\[CAST_OVERFLOW\].*\"TIMESTAMP_NS\""):
            spark.sql(OVERFLOW_DOORS[door])
    finally:
        spark.stop()


@pytest.mark.parametrize("door", list(OVERFLOW_DOORS))
def test_an_instant_past_2262_is_null_like_insert_without_ansi(tmp_path: Path, door: str) -> None:
    """Without ANSI every door stores INSERT's NULL for the overflow and the wall beside it."""
    spark = open_edge_session(tmp_path, "false")
    try:
        if door == "merge_update":
            spark.sql("INSERT INTO ice.ns.t VALUES (1, NULL), (2, NULL), (3, NULL)")
        spark.sql(OVERFLOW_DOORS[door])
        assert stored_ticks(spark) == [None, NEAR_NEW_YORK_WALL, None]
    finally:
        spark.stop()


@pytest.mark.parametrize(
    ("assigned", "expected"), [(NEAR, NEAR_NEW_YORK_WALL), ("NULL", None)], ids=["instant", "null"]
)
def test_update_with_no_where_stores_a_literal(
    tmp_path: Path, assigned: str, expected: int | None
) -> None:
    """``UPDATE … SET v = <literal>`` with no ``WHERE`` answers; main raised a raw Arrow error."""
    spark = open_edge_session(tmp_path, "true")
    try:
        spark.sql("INSERT INTO ice.ns.t VALUES (1, TIMESTAMP '2020-01-01 00:00:00')")
        spark.sql(f"UPDATE ice.ns.t SET v = {assigned}")
        assert stored_ticks(spark) == [expected]
    finally:
        spark.stop()


NESTED_TYPE = "STRUCT<v: timestamp_ns, n: INT>"
NESTED_FRAME_DOORS = {
    "df_append": lambda frame: frame.writeTo("ice.ns.t").append(),
    "df_overwrite_partitions": lambda frame: frame.writeTo("ice.ns.t").overwritePartitions(),
    "df_insert_into": lambda frame: frame.write.insertInto("ice.ns.t"),
    "df_insert_into_overwrite": lambda frame: frame.write.insertInto("ice.ns.t", overwrite=True),
    "df_save_append": lambda frame: frame.write.mode("append").saveAsTable("ice.ns.t"),
}


@pytest.mark.parametrize("door", list(NESTED_FRAME_DOORS))
def test_a_nested_field_stores_the_session_wall_through_the_dataframe_doors(
    tmp_path: Path, door: str
) -> None:
    """A ``struct<v: timestamp_ns>`` field stores the wall the top-level column stores."""
    spark = open_edge_session(tmp_path, "true")
    try:
        v3 = "USING iceberg TBLPROPERTIES ('format-version' = '3')"
        spark.sql("DROP TABLE ice.ns.t")
        spark.sql(f"CREATE TABLE ice.ns.t (id INT, st {NESTED_TYPE}, top timestamp_ns) {v3}")
        frame = spark.table("ice.ns.far").where("id = 2")
        NESTED_FRAME_DOORS[door](
            frame.selectExpr("id", "named_struct('v', c, 'n', 1) AS st", "c AS top")
        )
        stored = spark.sql("SELECT st.v AS nested, t.top FROM ice.ns.t t").to_arrow()
        assert str(stored.column("nested").type) == NS_WALL
        assert stored.column("nested").cast("int64").to_pylist() == [NEAR_NEW_YORK_WALL]
        assert stored.column("top").cast("int64").to_pylist() == [NEAR_NEW_YORK_WALL]
    finally:
        spark.stop()


WALL_SOURCES = {
    "n": (pa.timestamp("us"), 32_503_680_000_000_001, 1_767_323_045_123_456),
    "d": (pa.date32(), 376_200, 20_455),
}
WALL_SOURCE_NORMAL = {"n": 1_767_323_045_123_456_000, "d": 1_767_312_000_000_000_000}
FRAME_OVERWRITE_DOORS = {
    "df_overwrite_partitions": lambda frame: frame.writeTo("ice.ns.t").overwritePartitions(),
    "df_insert_into_overwrite": lambda frame: frame.write.insertInto("ice.ns.t", overwrite=True),
}


def wall_source_frame(spark: Any, source: str) -> Any:
    """Return a frame of one value past the nanosecond range and one inside it."""
    arrow_type, far, normal = WALL_SOURCES[source]
    width = pa.int32() if source == "d" else pa.int64()
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int32()),
            "v": pa.array([far, normal], width).cast(arrow_type),
        }
    )
    return doors.frame_of(spark, table)


@pytest.mark.parametrize("door", list(FRAME_OVERWRITE_DOORS))
@pytest.mark.parametrize("source", list(WALL_SOURCES))
@pytest.mark.parametrize("ansi", ["true", "false"])
def test_a_wall_source_past_the_range_answers_like_insert_on_the_overwrite_doors(
    tmp_path: Path, ansi: str, source: str, door: str
) -> None:
    """A ``TIMESTAMP_NTZ`` or ``DATE`` past 2262 is ``CAST_OVERFLOW`` under ANSI, else NULL."""
    spark = open_edge_session(tmp_path, ansi)
    try:
        frame = wall_source_frame(spark, source)
        if ansi == "true":
            with pytest.raises(Exception, match=r"\[CAST_OVERFLOW\].*\"TIMESTAMP_NS\""):
                FRAME_OVERWRITE_DOORS[door](frame)
        else:
            FRAME_OVERWRITE_DOORS[door](frame)
            assert stored_ticks(spark) == [None, WALL_SOURCE_NORMAL[source]]
    finally:
        spark.stop()
