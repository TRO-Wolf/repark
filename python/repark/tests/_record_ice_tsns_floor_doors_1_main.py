"""ICE-TSNS-FLOOR-DOORS-1 step 0 recorder of main microsecond narrowing per door."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import _ice_tsns_merge_wall_1_doors as doors
import pyarrow as pa

SOURCES = ("ns", "tzns")
TARGETS = ("ltz_v2", "ltz_v3", "ntz_v2", "ntz_v3")
DOOR_NAMES = (
    "insert",
    "insert_overwrite",
    "merge_insert",
    "merge_update",
    "update",
    "overwrite_partitions",
    "insert_into_overwrite",
    "writeto_append",
    "insert_into_append",
)
FSRC = "ice.ns.fsrc"
EPOCH = datetime(1970, 1, 1)
INT64_MIN = -9223372036854775808
INT64_MAX = 9223372036854775807
SMALL_TICKS = (-1, -1001, -999, 0, 1, 999, 1001)
VALUE_LABELS = (
    "neg1ns",
    "neg1001ns",
    "neg999ns",
    "zero",
    "pos1ns",
    "pos999ns",
    "pos1001ns",
    "repeat_hour",
    "skip_hour",
    "min_ns",
    "max_ns",
)


def value_ticks() -> list[int]:
    """Return the eleven probe instants in nanoseconds."""
    repeat = doors.nanos_of("2026-11-01 01:30:00.000000001")
    skip = doors.nanos_of("2026-03-08 02:30:00.000000001")
    return [int(v) for v in SMALL_TICKS] + [repeat, skip, INT64_MIN, INT64_MAX]


def floor_micros(tick: int) -> int:
    """Return tick narrowed to microseconds by flooring."""
    return tick // 1000


def trunc_micros(tick: int) -> int:
    """Return tick narrowed to microseconds by truncation toward zero."""
    if tick >= 0:
        return tick // 1000
    return -((-tick) // 1000)


def wall_to_instant_micros(wall_ns: int, zone: str) -> int:
    """Return the instant micros of a naive wall in the session zone."""
    wall_micros = wall_ns // 1000
    naive = EPOCH + timedelta(microseconds=wall_micros)
    zoned = naive.replace(tzinfo=ZoneInfo(zone))
    instant = zoned.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return int((instant - EPOCH) // timedelta(microseconds=1))


def instant_to_wall_micros(instant_ns: int, zone: str) -> int:
    """Return the wall micros of an instant in the session zone."""
    return doors.wall_nanos(instant_ns, zone) // 1000


def rule_expected(source: str, target: str, tick: int, zone: str) -> int:
    """Return the rule value of one tick through one source and target."""
    if source == "ns" and target.startswith("ntz"):
        return floor_micros(tick)
    if source == "tzns" and target.startswith("ltz"):
        return floor_micros(tick)
    if source == "ns":
        return wall_to_instant_micros(tick, zone)
    return instant_to_wall_micros(tick, zone)


def utc_expected(source: str, tick: int) -> int:
    """Return the UTC reading of one tick through one source."""
    return floor_micros(tick)


def label_cell(source: str, target: str, tick: int, zone: str, observed: Any) -> str:
    """Return the label of one observed tick against the rule."""
    if not isinstance(observed, int):
        return "error"
    expected = rule_expected(source, target, tick, zone)
    if observed == expected:
        return "EQUAL-RULE"
    utc_floor = utc_expected(source, tick)
    utc_trunc = trunc_micros(tick)
    zone_trunc: int | None = None
    if source == "ns" and target.startswith("ltz"):
        wall_trunc = trunc_micros(tick)
        try:
            naive = EPOCH + timedelta(microseconds=wall_trunc)
            zoned = naive.replace(tzinfo=ZoneInfo(zone))
            instant = zoned.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
            zone_trunc = int((instant - EPOCH) // timedelta(microseconds=1))
        except Exception:
            zone_trunc = None
    if source == "tzns" and target.startswith("ntz"):
        zone_trunc = trunc_micros(doors.wall_nanos(tick, "UTC"))
        try:
            zone_trunc = trunc_micros(doors.wall_nanos(tick, zone))
        except Exception:
            zone_trunc = None
    trunc_differs = utc_trunc != utc_floor
    wall_differs = utc_floor != expected
    if observed == utc_trunc and trunc_differs and wall_differs:
        return "DIFFERS-both"
    if observed == utc_floor and wall_differs:
        return "DIFFERS-wall-as-UTC"
    if observed in (utc_trunc, zone_trunc):
        return "DIFFERS-truncated"
    return "DIFFERS-other"


def source_frame_for(spark: Any, source: str) -> Any:
    """Return the DataFrame projecting the probe source as target columns."""
    return spark.table(FSRC).selectExpr("id", source + " AS v", "0 AS k")


def run_insert(spark: Any, table: str, source: str) -> None:
    """Run the plain INSERT door."""
    spark.sql("INSERT INTO " + table + " SELECT id, " + source + " AS v, 0 AS k FROM " + FSRC)


def run_insert_overwrite(spark: Any, table: str, source: str) -> None:
    """Run the INSERT OVERWRITE door."""
    spark.sql("INSERT OVERWRITE " + table + " SELECT id, " + source + " AS v, 0 AS k FROM " + FSRC)


def run_merge_insert(spark: Any, table: str, source: str) -> None:
    """Run the MERGE insert arm door."""
    spark.sql(
        "MERGE INTO "
        + table
        + " t USING "
        + FSRC
        + " s ON t.id = s.id "
        + "WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, s."
        + source
        + ", 0)"
    )


def seed_eleven(spark: Any, table: str) -> None:
    """Write one NULL-valued row per probe value into the table."""
    rows = ", ".join("(" + str(index) + ", NULL, 0)" for index in range(1, 12))
    spark.sql("INSERT INTO " + table + " VALUES " + rows)


def run_merge_update(spark: Any, table: str, source: str) -> None:
    """Run the MERGE update arm door."""
    seed_eleven(spark, table)
    spark.sql(
        "MERGE INTO "
        + table
        + " t USING "
        + FSRC
        + " s ON t.id = s.id "
        + "WHEN MATCHED THEN UPDATE SET v = s."
        + source
    )


def run_update(spark: Any, table: str, source: str) -> None:
    """Run the UPDATE door from a sibling column."""
    spark.sql("INSERT INTO " + table + " SELECT id, NULL, 0, " + source + " FROM " + FSRC)
    spark.sql("UPDATE " + table + " SET v = c")


def run_overwrite_partitions(spark: Any, table: str, source: str) -> None:
    """Run the overwritePartitions door."""
    seed_eleven(spark, table)
    source_frame_for(spark, source).writeTo(table).overwritePartitions()


def run_insert_into_overwrite(spark: Any, table: str, source: str) -> None:
    """Run the insertInto overwrite door."""
    seed_eleven(spark, table)
    source_frame_for(spark, source).write.insertInto(table, overwrite=True)


def run_writeto_append(spark: Any, table: str, source: str) -> None:
    """Run the writeTo append door."""
    source_frame_for(spark, source).writeTo(table).append()


def run_insert_into_append(spark: Any, table: str, source: str) -> None:
    """Run the insertInto append door."""
    source_frame_for(spark, source).write.insertInto(table)


DOORS = {
    "insert": run_insert,
    "insert_overwrite": run_insert_overwrite,
    "merge_insert": run_merge_insert,
    "merge_update": run_merge_update,
    "update": run_update,
    "overwrite_partitions": run_overwrite_partitions,
    "insert_into_overwrite": run_insert_into_overwrite,
    "writeto_append": run_writeto_append,
    "insert_into_append": run_insert_into_append,
}


def seed_probe_source(spark: Any) -> None:
    """Create the probe source table holding the eleven ticks."""
    ticks = value_ticks()
    ids = list(range(1, len(ticks) + 1))
    spark.sql(
        "CREATE TABLE "
        + FSRC
        + " (id INT, ns timestamp_ns, tzns timestamptz_ns) USING iceberg "
        + "TBLPROPERTIES ('format-version' = '3')"
    )
    arrays = {
        "id": pa.array(ids, pa.int32()),
        "ns": pa.array(ticks, pa.timestamp("ns")),
        "tzns": pa.array(ticks, pa.timestamp("ns", tz="UTC")),
    }
    doors.frame_of(spark, pa.table(arrays)).writeTo(FSRC).append()


def measure_one(spark: Any, target: str, door: str, source: str, zone: str) -> dict[str, Any]:
    """Measure one door target and source, with rule references beside it."""
    ticks = value_ticks()
    table = "ice.ns.t_" + source
    extra = ""
    if door == "update":
        extra = ", c " + doors.SOURCE_SQL_TYPES[source]
    try:
        doors.create_target(spark, table, target, extra)
        DOORS[door](spark, table, source)
        back = doors.read_back(spark, table)
        values = back.get("values", [])
        floors = [floor_micros(t) for t in ticks]
        rules = [rule_expected(source, target, t, zone) for t in ticks]
        labels = []
        for index, tick in enumerate(ticks):
            observed = values[index] if index < len(values) else None
            labels.append(label_cell(source, target, tick, zone, observed))
        return {
            "type": back.get("type"),
            "ids": back.get("ids"),
            "values": values,
            "ticks": ticks,
            "labels_ticks": list(VALUE_LABELS),
            "floor": floors,
            "rule": rules,
            "labels": labels,
        }
    except Exception as error:
        cell = doors.error_cell(error)
        cell["ticks"] = ticks
        cell["labels_ticks"] = list(VALUE_LABELS)
        cell["floor"] = [floor_micros(t) for t in ticks]
        cell["rule"] = [rule_expected(source, target, t, zone) for t in ticks]
        cell["labels"] = ["error" for _ in ticks]
        return cell


def cell_key(zone: str, target: str, door: str, source: str) -> str:
    """Return the fixture key of one measured combination."""
    return zone + "|" + target + "|" + door + "|" + source


def record_zone(zone: str) -> dict[str, Any]:
    """Measure every target door and source of one zone."""
    cells: dict[str, Any] = {}
    for target in TARGETS:
        for door in DOOR_NAMES:
            with tempfile.TemporaryDirectory(prefix="tsns-floor-") as warehouse:
                spark = doors.open_session(zone, Path(warehouse))
                try:
                    seed_probe_source(spark)
                    for source in SOURCES:
                        key = cell_key(zone, target, door, source)
                        cells[key] = measure_one(spark, target, door, source, zone)
                finally:
                    spark.stop()
    return cells


def main() -> None:
    """Record one zone to stdout, or every zone into the evidence files."""
    parser = argparse.ArgumentParser(description="Record floor doors main cells")
    parser.add_argument("commit")
    parser.add_argument("--zone")
    parser.add_argument("--output", type=Path, default=None)
    arguments = parser.parse_args()
    if arguments.zone:
        json.dump(record_zone(arguments.zone), sys.stdout)
        return
    workers = [
        subprocess.Popen(
            [sys.executable, __file__, arguments.commit, "--zone", zone],
            stdout=subprocess.PIPE,
            text=True,
        )
        for zone in doors.ZONES
    ]
    cells: dict[str, Any] = {}
    for worker in workers:
        output, _ = worker.communicate()
        if worker.returncode != 0:
            raise SystemExit(worker.returncode)
        cells.update(json.loads(output))
    document = {
        "recorded_at_commit": arguments.commit,
        "value_labels": list(VALUE_LABELS),
        "cells": cells,
    }
    if arguments.output is not None:
        arguments.output.write_text(json.dumps(document, sort_keys=True) + "\n")
        print(str(len(cells)) + " cells")
    else:
        json.dump(document, sys.stdout)


if __name__ == "__main__":
    main()
