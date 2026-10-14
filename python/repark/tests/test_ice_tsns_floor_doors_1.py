from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import _ice_tsns_merge_wall_1_doors as doors
import _record_ice_tsns_floor_doors_1_main as recorder
import pyarrow as pa
import pytest

from repark import ReparkSession, functions

FSRC = recorder.FSRC
TICKS = tuple(recorder.value_ticks())
IDS = tuple(range(1, len(TICKS) + 1))
NANO_SOURCES = recorder.SOURCES
MICRO_SOURCES = ("n", "l")
SOURCES = (*NANO_SOURCES, *MICRO_SOURCES)
SOURCE_SQL_TYPES = {
    "ns": "timestamp_ns",
    "tzns": "timestamptz_ns",
    "n": "TIMESTAMP_NTZ",
    "l": "TIMESTAMP",
}
SOURCE_ARROW_TYPES = {
    "ns": pa.timestamp("ns"),
    "tzns": pa.timestamp("ns", tz="UTC"),
    "n": pa.timestamp("us"),
    "l": pa.timestamp("us", tz="UTC"),
}
NANO_TWIN = {"ns": "ns", "tzns": "tzns", "n": "ns", "l": "tzns"}
TARGETS = recorder.TARGETS
TARGET_SQL_TYPES = {"ltz": "TIMESTAMP", "ntz": "TIMESTAMP_NTZ"}
EPOCH = datetime(1970, 1, 1)
V2_REFUSAL = r"not supported until v3"
MICRO_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
LAYOUTS = {
    "plain_cow": ("", ""),
    "plain_mor": ("", doors.MODES["mor"]),
    "part_cow": ("PARTITIONED BY (k) ", ""),
    "part_mor": ("PARTITIONED BY (k) ", doors.MODES["mor"]),
}
PARTITIONED_LAYOUTS = ("part_cow", "part_mor")


def source_ticks(source: str) -> list[int]:
    if source in NANO_SOURCES:
        return list(TICKS)
    return [tick // 1_000 for tick in TICKS]


def rule_values(source: str, target: str, zone: str) -> list[int]:
    if source in NANO_SOURCES:
        return [recorder.rule_expected(source, target, tick, zone) for tick in TICKS]
    return [
        recorder.rule_expected(NANO_TWIN[source], target, micros * 1_000, zone)
        for micros in source_ticks(source)
    ]


def refuses_on_v2(target: str, door: str, source: str) -> bool:
    return door == "update" and target.endswith("_v2") and source in NANO_SOURCES


def open_probe_session(zone: str, warehouse: Path) -> Any:
    spark = (
        ReparkSession.builder.appName("ice-tsns-floor-doors-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    columns = ", ".join(f"{name} {SOURCE_SQL_TYPES[name]}" for name in SOURCES)
    spark.sql(
        f"CREATE TABLE {FSRC} (id INT, {columns}) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    arrays = {"id": pa.array(list(IDS), pa.int32())}
    arrays.update(
        {name: pa.array(source_ticks(name), SOURCE_ARROW_TYPES[name]) for name in SOURCES}
    )
    doors.frame_of(spark, pa.table(arrays)).writeTo(FSRC).append()
    return spark


def create_layout_target(spark: Any, table: str, target: str, layout: str) -> None:
    sql_type = TARGET_SQL_TYPES[target.split("_")[0]]
    version = target.split("_v")[1]
    partitioning, properties = LAYOUTS[layout]
    spark.sql(
        f"CREATE TABLE {table} (id INT, v {sql_type}, k INT) USING iceberg {partitioning}"
        f"TBLPROPERTIES ('format-version' = '{version}'{properties})"
    )


def tick_text(tick: int, unit: str) -> str:
    if unit == "ns":
        seconds, fraction = divmod(tick, 1_000_000_000)
        whole = EPOCH + timedelta(seconds=seconds)
        return f"{whole:%Y-%m-%d %H:%M:%S}.{fraction:09d}"
    seconds, fraction = divmod(tick, 1_000_000)
    whole = EPOCH + timedelta(seconds=seconds)
    return f"{whole:%Y-%m-%d %H:%M:%S}.{fraction:06d}"


def literal(source: str, index: int) -> str:
    tick = source_ticks(source)[index]
    if source == "ns":
        return f"CAST('{tick_text(tick, 'ns')}' AS timestamp_ns)"
    if source == "tzns":
        return f"CAST('{tick_text(tick, 'ns')}+00:00' AS timestamptz_ns)"
    if source == "n":
        return f"to_timestamp_ntz('{tick_text(tick, 'us')}', '{MICRO_FORMAT}')"
    return f"TIMESTAMP '{tick_text(tick, 'us')}+00:00'"


def select_source(source: str) -> str:
    return f"SELECT id, {source} AS v, 0 AS k FROM {FSRC}"


def source_frame(spark: Any, source: str) -> Any:
    return spark.table(FSRC).selectExpr("id", f"{source} AS v", "0 AS k")


def door_insert_by_name(spark: Any, table: str, source: str) -> None:
    spark.sql(f"INSERT INTO {table} BY NAME SELECT 0 AS k, {source} AS v, id FROM {FSRC}")


def door_insert_overwrite_by_name(spark: Any, table: str, source: str) -> None:
    recorder.seed_eleven(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} BY NAME SELECT 0 AS k, {source} AS v, id FROM {FSRC}")


def door_insert_overwrite_columns(spark: Any, table: str, source: str) -> None:
    recorder.seed_eleven(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} (k, v, id) SELECT 0, {source}, id FROM {FSRC}")


def door_insert_overwrite_partition(spark: Any, table: str, source: str) -> None:
    recorder.seed_eleven(spark, table)
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (k = 0) SELECT id, {source} AS v FROM {FSRC}")


def door_insert_replace_where(spark: Any, table: str, source: str) -> None:
    recorder.seed_eleven(spark, table)
    spark.sql(f"INSERT INTO {table} REPLACE WHERE id >= 0 {select_source(source)}")


def door_df_overwrite_where(spark: Any, table: str, source: str) -> None:
    recorder.seed_eleven(spark, table)
    source_frame(spark, source).writeTo(table).overwrite(functions.col("id") >= 0)


def door_df_save_append(spark: Any, table: str, source: str) -> None:
    source_frame(spark, source).write.mode("append").saveAsTable(table)


def door_merge_insert_star(spark: Any, table: str, source: str) -> None:
    spark.sql(
        f"MERGE INTO {table} t USING ({select_source(source)}) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def door_merge_insert_literal(spark: Any, table: str, source: str) -> None:
    for index, row in enumerate(IDS):
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT {row} AS id) s ON t.id = s.id "
            f"WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {literal(source, index)}, 0)"
        )


def door_merge_update_star(spark: Any, table: str, source: str) -> None:
    recorder.seed_eleven(spark, table)
    spark.sql(
        f"MERGE INTO {table} t USING ({select_source(source)}) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET *"
    )


def door_merge_update_literal(spark: Any, table: str, source: str) -> None:
    recorder.seed_eleven(spark, table)
    for index, row in enumerate(IDS):
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT {row} AS id) s ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET v = {literal(source, index)}"
        )


ADDED_DOORS: dict[str, Callable[[Any, str, str], None]] = {
    "insert_by_name": door_insert_by_name,
    "insert_overwrite_by_name": door_insert_overwrite_by_name,
    "insert_overwrite_columns": door_insert_overwrite_columns,
    "insert_overwrite_partition": door_insert_overwrite_partition,
    "insert_replace_where": door_insert_replace_where,
    "df_overwrite_where": door_df_overwrite_where,
    "df_save_append": door_df_save_append,
    "merge_insert_star": door_merge_insert_star,
    "merge_insert_literal": door_merge_insert_literal,
    "merge_update_star": door_merge_update_star,
    "merge_update_literal": door_merge_update_literal,
}


def door_layouts(door: str) -> tuple[str, ...]:
    if door == "insert_overwrite_partition":
        return PARTITIONED_LAYOUTS
    return tuple(LAYOUTS)


def stored_values(spark: Any, table: str) -> list[int | None]:
    back = doors.read_back(spark, table)
    assert back["ids"] == list(IDS), table
    return back["values"]


def run_step0_cell(spark: Any, target: str, door: str, source: str) -> list[int | None]:
    table = f"ice.ns.t_{source}"
    extra = f", c {SOURCE_SQL_TYPES[source]}" if door == "update" else ""
    doors.create_target(spark, table, target, extra)
    recorder.DOORS[door](spark, table, source)
    return stored_values(spark, table)


def run_added_cell(
    spark: Any, target: str, door: str, source: str, layout: str
) -> list[int | None]:
    table = f"ice.ns.t_{source}_{layout}"
    create_layout_target(spark, table, target, layout)
    ADDED_DOORS[door](spark, table, source)
    return stored_values(spark, table)


def misses(observed: list[int | None], expected: list[int]) -> list[tuple[int, Any, int]]:
    return [
        (tick, seen, want)
        for tick, seen, want in zip(TICKS, observed, expected, strict=True)
        if seen != want
    ]


@pytest.mark.parametrize("door", recorder.DOOR_NAMES)
@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("zone", doors.ZONES)
def test_every_step0_door_stores_the_floor_in_the_session_zone(
    tmp_path: Path, zone: str, target: str, door: str
) -> None:
    spark = open_probe_session(zone, tmp_path)
    try:
        wrong: dict[str, Any] = {}
        for source in SOURCES:
            if refuses_on_v2(target, door, source):
                with pytest.raises(Exception, match=V2_REFUSAL):
                    run_step0_cell(spark, target, door, source)
                continue
            observed = run_step0_cell(spark, target, door, source)
            missed = misses(observed, rule_values(source, target, zone))
            if missed:
                wrong[source] = missed
        assert not wrong, (zone, target, door, wrong)
    finally:
        spark.stop()


@pytest.mark.parametrize("door", list(ADDED_DOORS))
@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("zone", doors.ZONES)
def test_every_added_door_and_layout_stores_the_floor_in_the_session_zone(
    tmp_path: Path, zone: str, target: str, door: str
) -> None:
    spark = open_probe_session(zone, tmp_path)
    try:
        wrong: dict[str, Any] = {}
        for layout in door_layouts(door):
            for source in SOURCES:
                observed = run_added_cell(spark, target, door, source, layout)
                missed = misses(observed, rule_values(source, target, zone))
                if missed:
                    wrong[f"{layout}|{source}"] = missed
        assert not wrong, (zone, target, door, wrong)
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", doors.ZONES)
def test_the_ntz_read_cast_of_a_nanosecond_source_floors(tmp_path: Path, zone: str) -> None:
    spark = open_probe_session(zone, tmp_path)
    try:
        for source in NANO_SOURCES:
            read = spark.sql(
                f"SELECT id, CAST({source} AS TIMESTAMP_NTZ) AS v FROM {FSRC} ORDER BY id"
            ).to_arrow()
            observed = read.column("v").cast(pa.int64()).to_pylist()
            expected = rule_values(source, "ntz_v3", zone)
            assert not misses(observed, expected), (zone, source)
    finally:
        spark.stop()


@pytest.mark.parametrize("zone", doors.ZONES)
def test_the_ltz_read_cast_of_a_nanosecond_source_floors(tmp_path: Path, zone: str) -> None:
    spark = open_probe_session(zone, tmp_path)
    try:
        for source in NANO_SOURCES:
            read = spark.sql(
                f"SELECT id, CAST({source} AS TIMESTAMP) AS v FROM {FSRC} ORDER BY id"
            ).to_arrow()
            observed = read.column("v").cast(pa.int64()).to_pylist()
            expected = rule_values(source, "ltz_v3", zone)
            assert not misses(observed, expected), (zone, source)
    finally:
        spark.stop()
