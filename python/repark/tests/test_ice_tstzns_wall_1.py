"""ICE-TSTZNS-WALL-1: every write door stores the same instant into ``timestamptz_ns``.

A ``timestamptz_ns`` column holds instants. An instant written into it is kept, a wall
(``TIMESTAMP_NTZ``, ``timestamp_ns``, ``DATE``) is localised in the session zone, and no digit
below the microsecond is dropped, through every door. That is what INSERT already did; the
overwrite, MERGE and ``UPDATE`` doors read a wall as UTC, and ``UPDATE`` with no ``WHERE``
raised a raw Arrow error. Spark 4.1.2 with Iceberg 1.11.0 cannot write the type, so the
expectation is INSERT's rule, computed here with ``zoneinfo`` and checked against INSERT in
every zone. Every value is read from the Parquet data files with ``pyarrow``.

pins: ice-tstzns-wall-1/C-001, C-002, C-003, C-004, C-005, C-006, C-010, C-011, C-012
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import _ice_tsns_merge_wall_1_doors as doors
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession

ZONES = ("UTC", "America/New_York", "Asia/Kolkata", "Asia/Kathmandu", "Australia/Lord_Howe")
ZONED_NS = "timestamp[ns, tz=UTC]"
RULE_SOURCES = ("l", "n", "ns", "tzns")
STILL_REFUSED = {("update_literal", "ns"), ("update_literal", "tzns")}
SECOND = 1_000_000_000


def instant_of_wall(wall: int, zone: str) -> int:
    """Return the instant INSERT stores for the wall ``wall`` (nanoseconds) in ``zone``."""
    local = (doors.EPOCH + timedelta(seconds=wall // SECOND)).replace(tzinfo=ZoneInfo(zone))
    offset = local.utcoffset()
    assert offset is not None
    return wall - int(offset.total_seconds()) * SECOND


def expected_instants(source: str, zone: str) -> list[int]:
    """Return the rule's instants for every moment written from ``source`` in ``zone``."""
    if source == "tzns":
        return list(doors.NANOS)
    if source == "l":
        return [value // 1_000 * 1_000 for value in doors.NANOS]
    if source == "n":
        return [instant_of_wall(value // 1_000 * 1_000, zone) for value in doors.NANOS]
    return [instant_of_wall(value, zone) for value in doors.NANOS]


def data_file_instants(spark: Any, table: str) -> list[int | None]:
    """Return the int64 ticks of ``v`` read from the live Parquet data files of ``table``."""
    files = spark.sql(f"SELECT content, file_path FROM {table}.files").to_arrow().to_pylist()
    ticks: list[int | None] = []
    for file in files:
        if file["content"] == 0:
            column = pq.read_table(file["file_path"], columns=["v"]).column("v")
            assert str(column.type) == ZONED_NS
            ticks.extend(column.cast(pa.int64()).to_pylist())
    return ticks


@pytest.mark.parametrize("door", list(doors.DOORS))
@pytest.mark.parametrize("zone", ZONES)
def test_every_door_stores_inserts_instant_in_timestamptz_ns(
    tmp_path: Path, zone: str, door: str
) -> None:
    """Each source type through ``door`` stores the rule's instant, read from the file."""
    spark = doors.open_session(zone, tmp_path)
    try:
        for source in RULE_SOURCES:
            table = f"ice.ns.t_{source}"
            extra = f", c {doors.SOURCE_SQL_TYPES[source]}" if door == "update_column" else ""
            doors.create_target(spark, table, "tz_ns", extra)
            if (door, source) in STILL_REFUSED:
                with pytest.raises(Exception, match="Unsupported SQL type"):
                    doors.DOORS[door](spark, table, source)
                continue
            doors.DOORS[door](spark, table, source)
            written = data_file_instants(spark, table)
            assert sorted(written) == sorted(expected_instants(source, zone)), (door, source)
            if source in ("ns", "tzns"):
                assert all(value % 1_000 for value in written), (door, source)
    finally:
        spark.stop()


def test_the_rule_is_what_insert_stores_and_main_broke_it_on_38_cells() -> None:
    """The fixture is main's: INSERT follows the rule, 38 cells read a wall as UTC, 6 are raw."""
    main = doors_fixture()
    zone = doors.CONTROL_ZONE
    wrong = raw = 0
    for door in doors.DOORS:
        for source in RULE_SOURCES:
            recorded = main[doors.cell_key(zone, "tz_ns", door, source)]
            if "error" in recorded:
                raw += "Arrow error" in recorded["message"]
            elif recorded["values"] != expected_instants(source, zone):
                wrong += 1
    for source in RULE_SOURCES:
        recorded = main[doors.cell_key(zone, "tz_ns", "insert_select", source)]
        assert recorded["values"] == expected_instants(source, zone), source
    assert (wrong, raw) == (19, 2)


def doors_fixture() -> dict[str, Any]:
    """Return main's recorded cells of the ICE-TSNS-MERGE-WALL-1 matrix."""
    path = Path(__file__).with_name("ice_tsns_merge_wall_1_main.json")
    return json.loads(path.read_text(encoding="utf-8"))["cells"]


EXTRA_WALLS = (
    "2026-10-04 02:15:00.000000001",
    "2026-04-05 01:45:00.000000001",
    "2026-03-08 02:30:00.000000001",
    "2026-11-01 01:30:00.000000001",
    "1969-12-31 23:59:59.999999999",
    "2026-01-02 03:04:05.123456789",
)
EXTRA_NANOS = tuple(doors.nanos_of(text) for text in EXTRA_WALLS)
WALL_SELECT = "SELECT id, w AS v, 0 AS k, w AS c FROM ice.ns.walls"
WALL_ROUTES = {
    "insert_select": lambda spark: spark.sql(f"INSERT INTO ice.ns.t {WALL_SELECT}"),
    "insert_overwrite": lambda spark: spark.sql(f"INSERT OVERWRITE ice.ns.t {WALL_SELECT}"),
    "insert_by_name": lambda spark: spark.sql(
        "INSERT INTO ice.ns.t BY NAME SELECT w AS c, 0 AS k, w AS v, id FROM ice.ns.walls"
    ),
    "merge_insert": lambda spark: spark.sql(
        f"MERGE INTO ice.ns.t t USING ({WALL_SELECT}) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT *"
    ),
    "merge_update": lambda spark: (
        spark.sql("INSERT INTO ice.ns.t (id, k, c) SELECT id, 0, w FROM ice.ns.walls"),
        spark.sql(
            f"MERGE INTO ice.ns.t t USING ({WALL_SELECT}) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET v = s.v"
        ),
    ),
    "update_no_where": lambda spark: (
        spark.sql("INSERT INTO ice.ns.t (id, k, c) SELECT id, 0, w FROM ice.ns.walls"),
        spark.sql("UPDATE ice.ns.t SET v = c"),
    ),
    "df_overwrite_partitions": lambda spark: (
        spark.sql(WALL_SELECT).writeTo("ice.ns.t").overwritePartitions()
    ),
    "df_insert_into_overwrite": lambda spark: spark.sql(WALL_SELECT).write.insertInto(
        "ice.ns.t", overwrite=True
    ),
}


@pytest.mark.parametrize("route", list(WALL_ROUTES))
@pytest.mark.parametrize("zone", ["Australia/Lord_Howe", "Asia/Kathmandu", "America/New_York"])
def test_a_gap_or_overlap_wall_of_each_zone_is_localised_as_insert_localises_it(
    tmp_path: Path, zone: str, route: str
) -> None:
    """Lord Howe's half-hour gap and overlap and New York's are stored as INSERT stores them."""
    spark = (
        ReparkSession.builder.appName("ice-tstzns-wall-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", str(tmp_path))
        spark.sql("CREATE NAMESPACE ice.ns")
        v3 = "USING iceberg TBLPROPERTIES ('format-version' = '3')"
        spark.sql(f"CREATE TABLE ice.ns.walls (id INT, w timestamp_ns) {v3}")
        walls = pa.table(
            {
                "id": pa.array(range(len(EXTRA_NANOS)), pa.int32()),
                "w": pa.array(list(EXTRA_NANOS), pa.timestamp("ns")),
            }
        )
        doors.frame_of(spark, walls).writeTo("ice.ns.walls").append()
        spark.sql(f"CREATE TABLE ice.ns.t (id INT, v timestamptz_ns, k INT, c timestamp_ns) {v3}")
        WALL_ROUTES[route](spark)
        expected = [instant_of_wall(wall, zone) for wall in EXTRA_NANOS]
        assert sorted(data_file_instants(spark, "ice.ns.t")) == sorted(expected)
    finally:
        spark.stop()


FAR_DATE = pa.array([376_200], pa.int32()).cast(pa.date32())
FAR_ROUTES = {
    "df_overwrite_partitions": lambda frame: frame.writeTo("ice.ns.t").overwritePartitions(),
    "df_insert_into_overwrite": lambda frame: frame.write.insertInto("ice.ns.t", overwrite=True),
    "df_append": lambda frame: frame.writeTo("ice.ns.t").append(),
}


@pytest.mark.parametrize("route", list(FAR_ROUTES))
@pytest.mark.parametrize("required", ["", " NOT NULL"])
@pytest.mark.parametrize("ansi", ["true", "false"])
def test_a_date_past_the_range_never_panics_on_the_dataframe_doors(
    tmp_path: Path, ansi: str, required: str, route: str
) -> None:
    """ANSI raises ``CAST_OVERFLOW``; without it a nullable column stores NULL as INSERT does."""
    spark = (
        ReparkSession.builder.appName("ice-tstzns-wall-1-far")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    try:
        spark.register_memory_catalog("ice", str(tmp_path))
        spark.sql("CREATE NAMESPACE ice.ns")
        spark.sql(
            f"CREATE TABLE ice.ns.t (id INT, v timestamptz_ns{required}) USING iceberg "
            "TBLPROPERTIES ('format-version' = '3')"
        )
        frame = doors.frame_of(spark, pa.table({"id": pa.array([1], pa.int32()), "v": FAR_DATE}))
        if ansi == "true":
            with pytest.raises(Exception, match=r"\[CAST_OVERFLOW\].*\"TIMESTAMPTZ_NS\""):
                FAR_ROUTES[route](frame)
        elif required:
            with pytest.raises(Exception, match="non-nullable") as refused:
                FAR_ROUTES[route](frame)
            assert "panic" not in str(refused.value)
        else:
            FAR_ROUTES[route](frame)
            assert data_file_instants(spark, "ice.ns.t") == [None]
    finally:
        spark.stop()
