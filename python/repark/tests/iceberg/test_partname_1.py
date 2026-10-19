"""WO PARTNAME-1: partition-field names stay Spark's on each door.

Spark names a generated partition field differently per door: the CREATE door
omits the width (``bucket(16, id)`` becomes ``id_bucket``) while the
ALTER/UPDATE door keeps it (``bucket(8, id)`` becomes ``id_bucket_8``). The
U9-TYPES-1 residue R-26 compared the two doors against each other; this suite
pins each door against Spark's measured answer of 2026-09-26, read from the
table's latest metadata JSON. The redundancy divergence is a recorded fork
residue, not a pin here.

pins: partname-1/C-001, C-002, C-003
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession


@pytest.fixture()
def session(tmp_path: Path) -> Iterator[ReparkSession]:
    """Open a UTC facade session over a fresh memory catalog warehouse."""
    opened = (
        ReparkSession.builder.appName("partname-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    opened.register_memory_catalog("sc", str(tmp_path))
    opened.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    try:
        yield opened
    finally:
        opened.stop()


def latest_metadata(warehouse: Path, table: str) -> dict[str, Any]:
    """Read the newest metadata JSON of table under the warehouse."""
    name = table.split(".")[-1]
    files = [
        path
        for path in warehouse.rglob("*.metadata.json")
        if path.parent.name == "metadata" and path.parent.parent.name.split("-")[0] == name
    ]
    if not files:
        raise FileNotFoundError(f"no metadata for {table}")
    newest = max(files, key=lambda path: (path.stat().st_mtime_ns, path.name))
    return json.loads(newest.read_text(encoding="utf-8"))


def spec_fields(metadata: dict[str, Any]) -> list[tuple[str, str, int, int]]:
    """Return the default spec fields as name, transform, source and field id."""
    spec = next(
        entry
        for entry in metadata["partition-specs"]
        if entry["spec-id"] == metadata["default-spec-id"]
    )
    return [
        (field["name"], field["transform"], field["source-id"], field["field-id"])
        for field in spec["fields"]
    ]


def observed(
    session: ReparkSession, warehouse: Path, table: str
) -> list[tuple[str, str, int, int]]:
    """Return the default spec fields of table from its latest metadata JSON."""
    return spec_fields(latest_metadata(warehouse, table))


def test_create_door_bucket_and_truncate_omit_the_width(
    session: ReparkSession, tmp_path: Path
) -> None:
    """Pin the CREATE-door names of bucket(16, id) and truncate(10, s)."""
    session.sql(
        "CREATE TABLE sc.ns.t2 (id INT, s STRING) USING iceberg "
        "PARTITIONED BY (bucket(16, id), truncate(10, s))"
    ).collect()
    assert observed(session, tmp_path, "sc.ns.t2") == [
        ("id_bucket", "bucket[16]", 1, 1000),
        ("s_trunc", "truncate[10]", 2, 1001),
    ]


def test_create_door_hours_names_ts_hour(session: ReparkSession, tmp_path: Path) -> None:
    """Pin the CREATE-door name of hours(ts)."""
    session.sql(
        "CREATE TABLE sc.ns.c5 (ts TIMESTAMP) USING iceberg PARTITIONED BY (hours(ts))"
    ).collect()
    assert observed(session, tmp_path, "sc.ns.c5") == [("ts_hour", "hour", 1, 1000)]


def test_create_door_bucket_beside_identity(session: ReparkSession, tmp_path: Path) -> None:
    """Pin the CREATE-door names of bucket(4, id) beside id."""
    session.sql(
        "CREATE TABLE sc.ns.c3 (id INT) USING iceberg PARTITIONED BY (bucket(4, id), id)"
    ).collect()
    assert observed(session, tmp_path, "sc.ns.c3") == [
        ("id_bucket", "bucket[4]", 1, 1000),
        ("id", "identity", 1, 1001),
    ]


def test_update_door_bucket_truncate_days_sequence(session: ReparkSession, tmp_path: Path) -> None:
    """Pin the UPDATE-door names as bucket, truncate and days fields land."""
    session.sql("CREATE TABLE sc.ns.t4 (id INT, s STRING, ts TIMESTAMP) USING iceberg").collect()
    session.sql("ALTER TABLE sc.ns.t4 ADD PARTITION FIELD bucket(8, id)").collect()
    assert observed(session, tmp_path, "sc.ns.t4") == [("id_bucket_8", "bucket[8]", 1, 1000)]
    session.sql("ALTER TABLE sc.ns.t4 ADD PARTITION FIELD truncate(2, s)").collect()
    assert observed(session, tmp_path, "sc.ns.t4") == [
        ("id_bucket_8", "bucket[8]", 1, 1000),
        ("s_trunc_2", "truncate[2]", 2, 1001),
    ]
    session.sql("ALTER TABLE sc.ns.t4 ADD PARTITION FIELD days(ts)").collect()
    assert observed(session, tmp_path, "sc.ns.t4") == [
        ("id_bucket_8", "bucket[8]", 1, 1000),
        ("s_trunc_2", "truncate[2]", 2, 1001),
        ("ts_day", "day", 3, 1002),
    ]


def test_update_door_time_names_each_on_own_table(session: ReparkSession, tmp_path: Path) -> None:
    """Pin the UPDATE-door name of each time transform on its own table."""
    cases = [
        ("y0", "years(ts)", ("ts_year", "year", 3, 1000)),
        ("mo0", "months(ts)", ("ts_month", "month", 3, 1000)),
        ("d0", "days(ts)", ("ts_day", "day", 3, 1000)),
        ("h0", "hours(ts)", ("ts_hour", "hour", 3, 1000)),
    ]
    for table, transform, expected in cases:
        session.sql(
            f"CREATE TABLE sc.ns.{table} (id INT, s STRING, ts TIMESTAMP) USING iceberg"
        ).collect()
        session.sql(f"ALTER TABLE sc.ns.{table} ADD PARTITION FIELD {transform}").collect()
        assert observed(session, tmp_path, f"sc.ns.{table}") == [expected]


def test_update_door_bucket16_beside_bucket8(session: ReparkSession, tmp_path: Path) -> None:
    """Pin the UPDATE-door names of bucket(8, id) beside bucket(16, id)."""
    session.sql("CREATE TABLE sc.ns.bb (id INT, s STRING, ts TIMESTAMP) USING iceberg").collect()
    session.sql("ALTER TABLE sc.ns.bb ADD PARTITION FIELD bucket(8, id)").collect()
    session.sql("ALTER TABLE sc.ns.bb ADD PARTITION FIELD bucket(16, id)").collect()
    assert observed(session, tmp_path, "sc.ns.bb") == [
        ("id_bucket_8", "bucket[8]", 1, 1000),
        ("id_bucket_16", "bucket[16]", 1, 1001),
    ]


def test_update_door_trunc4_beside_trunc2(session: ReparkSession, tmp_path: Path) -> None:
    """Pin the UPDATE-door names of truncate(2, s) beside truncate(4, s)."""
    session.sql("CREATE TABLE sc.ns.tt (id INT, s STRING, ts TIMESTAMP) USING iceberg").collect()
    session.sql("ALTER TABLE sc.ns.tt ADD PARTITION FIELD truncate(2, s)").collect()
    session.sql("ALTER TABLE sc.ns.tt ADD PARTITION FIELD truncate(4, s)").collect()
    assert observed(session, tmp_path, "sc.ns.tt") == [
        ("s_trunc_2", "truncate[2]", 2, 1000),
        ("s_trunc_4", "truncate[4]", 2, 1001),
    ]


def test_update_door_named_month(session: ReparkSession, tmp_path: Path) -> None:
    """Pin the UPDATE-door name of months(ts) AS my_month."""
    session.sql("CREATE TABLE sc.ns.m0 (id INT, s STRING, ts TIMESTAMP) USING iceberg").collect()
    session.sql("ALTER TABLE sc.ns.m0 ADD PARTITION FIELD months(ts) AS my_month").collect()
    assert observed(session, tmp_path, "sc.ns.m0") == [("my_month", "month", 3, 1000)]


def test_update_door_drop_removes_and_replace_renames_with_fresh_id(
    session: ReparkSession, tmp_path: Path
) -> None:
    """Pin DROP removing id_bucket_8 and REPLACE landing s_trunc_5 on a fresh id."""
    session.sql("CREATE TABLE sc.ns.dr (id INT, s STRING, ts TIMESTAMP) USING iceberg").collect()
    session.sql("ALTER TABLE sc.ns.dr ADD PARTITION FIELD bucket(8, id)").collect()
    session.sql("ALTER TABLE sc.ns.dr ADD PARTITION FIELD truncate(2, s)").collect()
    session.sql("ALTER TABLE sc.ns.dr ADD PARTITION FIELD days(ts)").collect()
    session.sql("ALTER TABLE sc.ns.dr DROP PARTITION FIELD bucket(8, id)").collect()
    assert observed(session, tmp_path, "sc.ns.dr") == [
        ("s_trunc_2", "truncate[2]", 2, 1001),
        ("ts_day", "day", 3, 1002),
    ]
    session.sql(
        "ALTER TABLE sc.ns.dr REPLACE PARTITION FIELD truncate(2, s) WITH truncate(5, s)"
    ).collect()
    assert observed(session, tmp_path, "sc.ns.dr") == [
        ("ts_day", "day", 3, 1002),
        ("s_trunc_5", "truncate[5]", 2, 1003),
    ]


def test_update_door_uuid_bucket_carries_the_width(session: ReparkSession, tmp_path: Path) -> None:
    """Pin the UPDATE-door name of bucket(4, u) over a uuid column."""
    session.sql("CREATE TABLE sc.ns.up (id INT) USING iceberg").collect()
    session.sql("ALTER TABLE sc.ns.up ADD COLUMN u UUID").collect()
    session.sql("ALTER TABLE sc.ns.up ADD PARTITION FIELD bucket(4, u)").collect()
    assert observed(session, tmp_path, "sc.ns.up") == [("u_bucket_4", "bucket[4]", 2, 1000)]
