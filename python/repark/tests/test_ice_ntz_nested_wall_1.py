"""ICE-NTZ-NESTED-WALL-1: a nested microsecond ``TIMESTAMP_NTZ`` leaf stores one wall.

The doors wired so far are the ones ``zone_store_frame`` serves: ``INSERT … BY NAME``,
``INSERT OVERWRITE … SELECT``, ``overwritePartitions`` and ``insertInto(overwrite=True)``.
An instant stores the session wall. A wall literal stores ``1767323045123456`` in every
zone. A nested ``TIMESTAMP`` leaf stores the instant.

pins: ice-ntz-nested-wall-1/C-003
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession

ZONES = ("UTC", "America/New_York", "Asia/Kolkata")
SESSION_WALL = {
    "UTC": 1_767_323_045_123_456,
    "America/New_York": 1_767_305_045_123_456,
    "Asia/Kolkata": 1_767_342_845_123_456,
}
INSTANT = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'"
WALL = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
WALL_TICK = 1_767_323_045_123_456


def open_session(zone: str, warehouse: Path) -> Any:
    """Open one session in ``zone`` with a private memory catalog."""
    spark = (
        ReparkSession.builder.appName("ice-ntz-nested-wall-1")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    return spark


def leaf_ticks(spark: Any, table: str, leaf: str) -> list[int | None]:
    """Return the stored microseconds of ``leaf``, from ``toArrow`` cast to int64."""
    arrow = spark.sql(f"SELECT {leaf} AS v FROM {table}").toArrow()
    column = arrow.column("v").combine_chunks()
    return column.cast(pa.int64(), safe=False).to_pylist()


def create_nested(spark: Any, table: str, leaf_type: str) -> None:
    """Create a v2 table whose struct leaf is ``leaf_type``."""
    spark.sql(
        f"CREATE TABLE {table} (id INT, st STRUCT<v: {leaf_type}, n: INT>, k INT) "
        "USING iceberg TBLPROPERTIES ('format-version'='2')"
    )


def write_source(spark: Any) -> None:
    """Write the instant into ``ice.ns.src.l``."""
    spark.sql(
        "CREATE TABLE ice.ns.src (id INT, l TIMESTAMP) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO ice.ns.src VALUES (1, {INSTANT})")


def by_name_from_column(spark: Any, table: str) -> None:
    """``INSERT … BY NAME`` of the source column."""
    spark.sql(
        f"INSERT INTO {table} BY NAME SELECT 0 AS k, named_struct('v', l, 'n', 1) AS st, id "
        "FROM ice.ns.src"
    )


def by_name_literal(spark: Any, table: str, literal: str) -> None:
    """``INSERT … BY NAME`` of one literal."""
    spark.sql(
        f"INSERT INTO {table} BY NAME SELECT 0 AS k, named_struct('v', {literal}, 'n', 1) AS st, "
        "1 AS id"
    )


def seed_null(spark: Any, table: str) -> None:
    """Write one null struct so an overwrite has a row to replace."""
    spark.sql(
        f"INSERT INTO {table} VALUES (1, named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', 0), 0)"
    )


def overwrite_select(spark: Any, table: str) -> None:
    """``INSERT OVERWRITE … SELECT`` of the source column."""
    seed_null(spark, table)
    spark.sql(
        f"INSERT OVERWRITE {table} SELECT id, named_struct('v', l, 'n', 1) AS st, 0 AS k "
        "FROM ice.ns.src"
    )


def overwrite_partitions(spark: Any, table: str) -> None:
    """DataFrame ``overwritePartitions`` of the source column."""
    seed_null(spark, table)
    frame = spark.sql("SELECT id, named_struct('v', l, 'n', 1) AS st, 0 AS k FROM ice.ns.src")
    frame.writeTo(table).overwritePartitions()


def insert_into_overwrite(spark: Any, table: str) -> None:
    """DataFrame ``insertInto(overwrite=True)`` of the source column."""
    seed_null(spark, table)
    frame = spark.sql("SELECT id, named_struct('v', l, 'n', 1) AS st, 0 AS k FROM ice.ns.src")
    frame.write.insertInto(table, overwrite=True)


@pytest.mark.parametrize("zone", ZONES)
def test_zone_store_doors_store_the_session_wall(tmp_path: Path, zone: str) -> None:
    """By-name and overwrite doors store the session wall of an instant."""
    spark = open_session(zone, tmp_path)
    try:
        write_source(spark)
        expected = SESSION_WALL[zone]
        doors = (
            ("by_name", by_name_from_column),
            ("overwrite", overwrite_select),
            ("partitions", overwrite_partitions),
            ("insert_into", insert_into_overwrite),
        )
        for name, write in doors:
            table = f"ice.ns.{name}"
            create_nested(spark, table, "TIMESTAMP_NTZ")
            write(spark, table)
            assert leaf_ticks(spark, table, "st.v") == [expected], name
        wall = "ice.ns.wall"
        create_nested(spark, wall, "TIMESTAMP_NTZ")
        by_name_literal(spark, wall, WALL)
        assert leaf_ticks(spark, wall, "st.v") == [WALL_TICK]
        literal = "ice.ns.literal"
        create_nested(spark, literal, "TIMESTAMP_NTZ")
        by_name_literal(spark, literal, INSTANT)
        assert leaf_ticks(spark, literal, "st.v") == [expected]
        control = "ice.ns.ltz"
        create_nested(spark, control, "TIMESTAMP")
        by_name_from_column(spark, control)
        assert leaf_ticks(spark, control, "st.v") == [WALL_TICK]
    finally:
        spark.stop()
