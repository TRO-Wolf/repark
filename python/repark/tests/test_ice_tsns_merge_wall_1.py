"""ICE-TSNS-MERGE-WALL-1: every write door stores the same nanosecond wall.

A ``timestamp_ns`` column is a wall clock. An instant written into it stores its wall in the
session zone, a wall is stored as it is, and no digit below the microsecond is dropped, through
every door. Spark 4.1.2 with Iceberg 1.11.0 cannot read or write the type, so the expectation is
the rule INSERT already follows (ICE-TSNS-SQL-1), computed here with ``zoneinfo``.

Every other target type is a control: its cells must answer exactly what main ``40fc916f``
answered, recorded in ``ice_tsns_merge_wall_1_main.json`` for one zone, the one with a DST rule.
The carries and the other zones' controls are pinned at the Rust door
(``crates/repark-spark/tests/timestamp_ns_wall_doors.rs``).

A ``timestamp_ns`` leaf below the top level is not written yet: every DataFrame route refuses
it by name and writes no file (ICE-TSNS-NESTED-1 lifts that).

pins: ice-tsns-merge-wall-1/C-004, C-005, C-006, C-007, C-010, C-011, C-012, C-019, C-021
pins: ice-tsns-merge-wall-1/C-023, C-025, C-035, C-036, C-040, C-044, C-045, C-046, C-048
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _ice_tsns_merge_wall_1_doors as doors
import pyarrow as pa
import pyarrow.parquet as pq
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


NEAR_MICROS = 1_767_323_045_123_456
INSTANT_ARROW = pa.timestamp("us", tz="UTC")
ONE_INSTANT = pa.array([NEAR_MICROS], INSTANT_ARROW)
ONE_INT = pa.array([1], pa.int32())
ONE_PLACE = pa.array([0], pa.int32())
ONE_WIDE_PLACE = pa.array([0], pa.int64())
NESTED_LAYOUTS = {
    "struct": (
        "STRUCT<v: timestamp_ns, n: INT>",
        lambda: pa.StructArray.from_arrays([ONE_INSTANT, ONE_INT], ["v", "n"]),
        "`st`.`v`",
    ),
    "struct_one_name_differs": (
        "STRUCT<a: timestamp_ns, b: TIMESTAMP>",
        lambda: pa.StructArray.from_arrays([ONE_INSTANT, ONE_INSTANT], ["q", "b"]),
        "`st`.`a`",
    ),
    "struct_in_struct": (
        "STRUCT<i: STRUCT<v: timestamp_ns, n: INT>, m: INT>",
        lambda: pa.StructArray.from_arrays(
            [pa.StructArray.from_arrays([ONE_INSTANT, ONE_INT], ["v", "n"]), ONE_INT], ["i", "m"]
        ),
        "`st`.`i`.`v`",
    ),
    "list": (
        "ARRAY<timestamp_ns>",
        lambda: pa.array([[NEAR_MICROS]], pa.list_(INSTANT_ARROW)),
        "`st`.`element`",
    ),
    "large_list": (
        "ARRAY<timestamp_ns>",
        lambda: pa.array([[NEAR_MICROS]], pa.large_list(INSTANT_ARROW)),
        "`st`.`element`",
    ),
    "list_view": (
        "ARRAY<timestamp_ns>",
        lambda: pa.ListViewArray.from_arrays(ONE_PLACE, ONE_INT, ONE_INSTANT),
        "`st`.`element`",
    ),
    "large_list_view": (
        "ARRAY<timestamp_ns>",
        lambda: pa.LargeListViewArray.from_arrays(
            ONE_WIDE_PLACE, pa.array([1], pa.int64()), ONE_INSTANT
        ),
        "`st`.`element`",
    ),
    "fixed_size_list": (
        "ARRAY<timestamp_ns>",
        lambda: pa.array([[NEAR_MICROS]], pa.list_(INSTANT_ARROW, 1)),
        "`st`.`element`",
    ),
    "list_of_struct": (
        "ARRAY<STRUCT<v: timestamp_ns, n: INT>>",
        lambda: pa.ListArray.from_arrays(
            pa.array([0, 1], pa.int32()),
            pa.StructArray.from_arrays([ONE_INSTANT, ONE_INT], ["v", "n"]),
        ),
        "`st`.`element`.`v`",
    ),
    "struct_of_list": (
        "STRUCT<a: ARRAY<timestamp_ns>, n: INT>",
        lambda: pa.StructArray.from_arrays(
            [pa.array([[NEAR_MICROS]], pa.list_(INSTANT_ARROW)), ONE_INT], ["a", "n"]
        ),
        "`st`.`a`.`element`",
    ),
    "dictionary_child": (
        "STRUCT<v: timestamp_ns, n: INT>",
        lambda: pa.StructArray.from_arrays([ONE_INSTANT.dictionary_encode(), ONE_INT], ["v", "n"]),
        "`st`.`v`",
    ),
    "run_end_encoded_child": (
        "STRUCT<v: timestamp_ns, n: INT>",
        lambda: pa.StructArray.from_arrays(
            [pa.RunEndEncodedArray.from_arrays(ONE_INT, ONE_INSTANT), ONE_INT], ["v", "n"]
        ),
        "`st`.`v`",
    ),
    "map_value": (
        "MAP<STRING, timestamp_ns>",
        lambda: pa.array([[("k", NEAR_MICROS)]], pa.map_(pa.string(), INSTANT_ARROW)),
        "`st`.`value`",
    ),
    "map_key": (
        "MAP<timestamp_ns, INT>",
        lambda: pa.array([[(NEAR_MICROS, 1)]], pa.map_(INSTANT_ARROW, pa.int32())),
        "`st`.`key`",
    ),
}


def write_from_a_temporary_view(spark: Any, frame: Any) -> None:
    """Hold ``frame`` as a SQL temporary view and INSERT from it."""
    frame.createOrReplaceTempView("held")
    spark.sql("INSERT INTO ice.ns.t SELECT id, st FROM held")


NESTED_DOORS = {
    "df_append": lambda _, frame: frame.writeTo("ice.ns.t").append(),
    "df_overwrite_partitions": lambda _, frame: frame.writeTo("ice.ns.t").overwritePartitions(),
    "df_overwrite_where": lambda _, frame: frame.writeTo("ice.ns.t").overwrite(
        doors.functions.col("id") >= 0
    ),
    "df_insert_into": lambda _, frame: frame.write.insertInto("ice.ns.t"),
    "df_insert_into_overwrite": lambda _, frame: frame.write.insertInto("ice.ns.t", overwrite=True),
    "df_save_append": lambda _, frame: frame.write.mode("append").saveAsTable("ice.ns.t"),
    "cached_frame": lambda _, frame: frame.cache().writeTo("ice.ns.t").append(),
    "temporary_view": write_from_a_temporary_view,
}
NOT_WRITABLE_YET = (
    r"\[INCOMPATIBLE_DATA_FOR_TABLE\.CANNOT_SAFELY_CAST\] Cannot write incompatible data for "
    r"the table `ice`\.`ns`\.`t`: Cannot safely cast {path} to \"TIMESTAMP_NS\"\. A nested "
    r"timestamp_ns leaf is not writable yet"
)


def files_under(root: Path) -> list[str]:
    """Return every file below ``root``."""
    return sorted(str(path) for path in root.rglob("*") if path.is_file())


@pytest.mark.parametrize("door", list(NESTED_DOORS))
@pytest.mark.parametrize("layout", list(NESTED_LAYOUTS))
def test_a_nested_timestamp_ns_leaf_is_refused_by_name_and_writes_nothing(
    tmp_path: Path, layout: str, door: str
) -> None:
    """Every Arrow layout of every container refuses at every DataFrame door, with no file."""
    spark = open_edge_session(tmp_path, "true")
    try:
        column_type, build, path = NESTED_LAYOUTS[layout]
        spark.sql("DROP TABLE ice.ns.t")
        v3 = "USING iceberg TBLPROPERTIES ('format-version' = '3')"
        spark.sql(f"CREATE TABLE ice.ns.t (id INT, st {column_type}) {v3}")
        frame = doors.frame_of(spark, pa.table({"id": ONE_INT, "st": build()}))
        before = files_under(tmp_path)
        with pytest.raises(Exception, match=NOT_WRITABLE_YET.format(path=path.replace(".", r"\."))):
            NESTED_DOORS[door](spark, frame)
        assert files_under(tmp_path) == before
        assert spark.sql("SELECT * FROM ice.ns.t.snapshots").to_arrow().num_rows == 0
    finally:
        spark.stop()


BRANCH = "ice.ns.t.branch_b1"
STRUCT_FRAME = "SELECT 5 AS id, named_struct('v', c, 'n', 1) AS st FROM ice.ns.far WHERE id = 2"


def under(key: str, value: str, write: Any) -> Any:
    """Return a route that runs ``write`` with one WAP session setting, then clears it."""

    def route(spark: Any) -> None:
        spark.conf.set(key, value)
        try:
            write(spark)
        finally:
            spark.conf.unset(key)

    return route


def plain_insert(spark: Any) -> None:
    """INSERT … SELECT of the struct into the table's own name."""
    spark.sql(f"INSERT INTO ice.ns.t {STRUCT_FRAME}")


def plain_append(spark: Any) -> None:
    """DataFrameWriterV2 append of the struct into the table's own name."""
    spark.sql(STRUCT_FRAME).writeTo("ice.ns.t").append()


BRANCH_AND_WAP_ROUTES = {
    "insert_branch": lambda spark: spark.sql(f"INSERT INTO {BRANCH} {STRUCT_FRAME}"),
    "insert_branch_values": lambda spark: spark.sql(
        f"INSERT INTO {BRANCH} VALUES (5, named_struct('v', {NEAR}, 'n', 1))"
    ),
    "insert_branch_main": lambda spark: spark.sql(
        f"INSERT INTO ice.ns.t.branch_main {STRUCT_FRAME}"
    ),
    "insert_branch_cols": lambda spark: spark.sql(f"INSERT INTO {BRANCH} (id, st) {STRUCT_FRAME}"),
    "df_append_branch": lambda spark: spark.sql(STRUCT_FRAME).writeTo(BRANCH).append(),
    "df_insert_into_branch": lambda spark: spark.sql(STRUCT_FRAME).write.insertInto(BRANCH),
    "wap_id_insert": under("spark.wap.id", "a1", plain_insert),
    "wap_id_df": under("spark.wap.id", "a2", plain_append),
    "wap_branch_insert": under("spark.wap.branch", "b1", plain_insert),
    "wap_branch_df": under("spark.wap.branch", "b1", plain_append),
    "update_branch": lambda spark: spark.sql(
        f"UPDATE {BRANCH} SET st = named_struct('v', {NEAR}, 'n', 1) WHERE id = 1"
    ),
    "update_branch_no_where": lambda spark: spark.sql(
        f"UPDATE {BRANCH} SET st = named_struct('v', {NEAR}, 'n', 1)"
    ),
    "merge_branch": lambda spark: spark.sql(
        f"MERGE INTO {BRANCH} t USING ({STRUCT_FRAME}) s ON t.id = s.id + 1 "
        "WHEN MATCHED THEN UPDATE SET st = s.st"
    ),
}


@pytest.mark.parametrize("route", list(BRANCH_AND_WAP_ROUTES))
def test_a_branch_or_wap_write_of_a_nested_leaf_is_refused_and_writes_nothing(
    tmp_path: Path, route: str
) -> None:
    """A branch reference or a WAP setting does not take a write around the refusal."""
    spark = open_edge_session(tmp_path, "true")
    try:
        spark.sql("DROP TABLE ice.ns.t")
        spark.sql(
            f"CREATE TABLE ice.ns.t (id INT, st {NESTED_LAYOUTS['struct'][0]}) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3', 'write.wap.enabled' = 'true')"
        )
        spark.sql("INSERT INTO ice.ns.t (id) VALUES (1), (6)")
        spark.sql("ALTER TABLE ice.ns.t CREATE BRANCH b1")
        before = files_under(tmp_path)
        with pytest.raises(Exception, match=NOT_WRITABLE_YET.format(path=r"`st`\.`v`")):
            BRANCH_AND_WAP_ROUTES[route](spark)
        assert files_under(tmp_path) == before
        assert spark.sql("SELECT * FROM ice.ns.t.snapshots").to_arrow().num_rows == 1
        assert spark.sql("SELECT * FROM ice.ns.t.refs").to_arrow().num_rows == 2
    finally:
        spark.stop()


STRUCT_INSERT = f"INSERT INTO ice.ns.t {STRUCT_FRAME}"
STRUCT_UPDATE = f"UPDATE ice.ns.t SET st = named_struct('v', {NEAR}, 'n', 1)"
WRAPPED_WRITES = {
    "explain_analyze": f"EXPLAIN ANALYZE {STRUCT_INSERT}",
    "explain_analyze_cols": f"EXPLAIN ANALYZE INSERT INTO ice.ns.t (id, st) {STRUCT_FRAME}",
    "explain_analyze_values": (
        f"EXPLAIN ANALYZE INSERT INTO ice.ns.t VALUES (5, named_struct('v', {NEAR}, 'n', 1))"
    ),
    "explain_analyze_verbose": f"EXPLAIN ANALYZE VERBOSE {STRUCT_INSERT}",
    "explain_analyze_overwrite": f"EXPLAIN ANALYZE INSERT OVERWRITE ice.ns.t {STRUCT_FRAME}",
    "explain_analyze_lower": f"explain analyze insert into ice.ns.t {STRUCT_FRAME}",
    "explain_analyze_update_where": f"EXPLAIN ANALYZE {STRUCT_UPDATE} WHERE id = 1",
    "explain_analyze_update": f"EXPLAIN ANALYZE {STRUCT_UPDATE}",
    "prepare": f"PREPARE supplied AS {STRUCT_INSERT}",
    "create_table_as_insert": f"CREATE TABLE ice.ns.made USING iceberg AS {STRUCT_INSERT}",
}


@pytest.mark.parametrize("wrapper", list(WRAPPED_WRITES))
def test_a_statement_that_wraps_a_nested_write_is_refused_and_writes_nothing(
    tmp_path: Path, wrapper: str
) -> None:
    """EXPLAIN ANALYZE, PREPARE and CREATE TABLE AS around a write are decided as that write."""
    spark = open_edge_session(tmp_path, "true")
    try:
        spark.sql("DROP TABLE ice.ns.t")
        spark.sql(
            f"CREATE TABLE ice.ns.t (id INT, st {NESTED_LAYOUTS['struct'][0]}) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3')"
        )
        spark.sql("INSERT INTO ice.ns.t (id) VALUES (1), (6)")
        before = files_under(tmp_path)
        with pytest.raises(Exception, match=NOT_WRITABLE_YET.format(path=r"`st`\.`v`")):
            spark.sql(WRAPPED_WRITES[wrapper]).collect()
        assert files_under(tmp_path) == before
        assert spark.sql("SELECT * FROM ice.ns.t.snapshots").to_arrow().num_rows == 1
        spark.sql(f"EXPLAIN {STRUCT_INSERT}").collect()
        assert spark.sql("SELECT * FROM ice.ns.t.snapshots").to_arrow().num_rows == 1
    finally:
        spark.stop()


def data_file_ticks(spark: Any, table: str) -> list[int | None]:
    """Return the int64 ticks of ``v`` read from the live Parquet data files of ``table``."""
    files = spark.sql(f"SELECT content, file_path FROM {table}.files").to_arrow().to_pylist()
    ticks: list[int | None] = []
    for file in files:
        if file["content"] == 0:
            column = pq.read_table(file["file_path"], columns=["v"]).column("v")
            assert str(column.type) == NS_WALL
            ticks.extend(column.cast(pa.int64()).to_pylist())
    return ticks


@pytest.mark.parametrize("door", list(doors.DOORS))
@pytest.mark.parametrize("zone", doors.ZONES)
def test_every_door_writes_the_digits_below_the_microsecond_into_the_parquet_file(
    tmp_path: Path, zone: str, door: str
) -> None:
    """The Parquet file, read with pyarrow, holds every nanosecond digit of the session wall."""
    spark = doors.open_session(zone, tmp_path)
    try:
        for source in ("ns", "tzns"):
            expected = expected_wall_cell(zone, door, source)
            if "error" in expected:
                continue
            table = f"ice.ns.t_{source}"
            extra = f", c {doors.SOURCE_SQL_TYPES[source]}" if door == "update_column" else ""
            doors.create_target(spark, table, "ts_ns", extra)
            doors.DOORS[door](spark, table, source)
            written = data_file_ticks(spark, table)
            assert sorted(written) == sorted(expected["values"]), (zone, door, source)
            assert all(value % 1_000 for value in written), (zone, door, source)
    finally:
        spark.stop()


RUN_INSTANTS = (1_767_323_045_123_456_789, 1_772_955_000_000_000_001)
RUN_NEW_YORK_WALLS = [1_767_305_045_123_456_789, 1_772_940_600_000_000_001]


def write_runs_by_sql(statement: str) -> Any:
    """Return a door that holds the frame as a temporary view and runs ``statement``."""

    def write(spark: Any, frame: Any) -> None:
        frame.createOrReplaceTempView("runs")
        spark.sql(statement)

    return write


RUN_END_DOORS = {
    "insert_overwrite": write_runs_by_sql("INSERT OVERWRITE ice.ns.t SELECT id, v FROM runs"),
    "insert_by_name": write_runs_by_sql("INSERT INTO ice.ns.t BY NAME SELECT v, id FROM runs"),
    "df_overwrite_partitions": lambda _, frame: frame.writeTo("ice.ns.t").overwritePartitions(),
    "df_insert_into_overwrite": lambda _, frame: frame.write.insertInto("ice.ns.t", overwrite=True),
}


@pytest.mark.parametrize("door", list(RUN_END_DOORS))
@pytest.mark.parametrize("encoding", ["run_end_encoded", "dictionary"])
def test_an_encoded_instant_source_stores_the_session_wall(
    tmp_path: Path, encoding: str, door: str
) -> None:
    """A run-end or dictionary encoded instant stores the New York wall; main stored UTC's."""
    spark = open_edge_session(tmp_path, "true")
    try:
        instants = pa.array(list(RUN_INSTANTS), pa.timestamp("ns", tz="UTC"))
        if encoding == "dictionary":
            encoded = instants.dictionary_encode()
        else:
            encoded = pa.RunEndEncodedArray.from_arrays(pa.array([1, 2], pa.int32()), instants)
        table = pa.table({"id": pa.array([1, 2], pa.int32()), "v": encoded})
        RUN_END_DOORS[door](spark, doors.frame_of(spark, table))
        assert sorted(data_file_ticks(spark, "ice.ns.t")) == RUN_NEW_YORK_WALLS
        assert stored_ticks(spark) == RUN_NEW_YORK_WALLS
    finally:
        spark.stop()
