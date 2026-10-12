"""Shared tables and readers for the nested NTZ wall facade."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

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
_V2 = "USING iceberg TBLPROPERTIES ('format-version'='2')"


def open_session(zone: str, warehouse: Path) -> Any:
    """Open one session in ``zone`` with a private memory catalog."""
    spark = (
        ReparkSession.builder.appName("ice-ntz-nested-wall-1")
        .config("spark.sql.session.timeZone", zone)
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    return spark


def parquet_leaf(spark: Any, table: str, column: str, field: str) -> list[int | None]:
    """Return one struct field's microseconds from the table's Parquet data files."""
    files = spark.sql(f"SELECT content, file_path FROM {table}.files").toArrow().to_pylist()
    ticks: list[int | None] = []
    for file in files:
        if file["content"] != 0:
            continue
        stored = pq.read_table(file["file_path"], columns=[column]).column(column)
        ticks.extend(stored.combine_chunks().field(field).cast(pa.int64(), safe=False).to_pylist())
    return ticks


def leaf_ticks(spark: Any, table: str, leaf: str) -> list[int | None]:
    """Return the stored microseconds of ``st.v`` or ``st.w`` from the Parquet files."""
    column_name, _, field = leaf.partition(".")
    return parquet_leaf(spark, table, column_name, field)


def stored_rows(spark: Any, table: str) -> list[tuple[int, int | None, int | None]]:
    """Return ``(id, st.v microseconds, st.n)`` from Parquet, sorted by id."""
    files = spark.sql(f"SELECT content, file_path FROM {table}.files").toArrow().to_pylist()
    rows: list[tuple[int, int | None, int | None]] = []
    for file in files:
        if file["content"] != 0:
            continue
        loaded = pq.read_table(file["file_path"], columns=["id", "st"])
        ticks = (
            loaded.column("st").combine_chunks().field("v").cast(pa.int64(), safe=False).to_pylist()
        )
        numbers = loaded.column("st").combine_chunks().field("n").to_pylist()
        rows.extend(zip(loaded.column("id").to_pylist(), ticks, numbers, strict=True))
    rows.sort()
    return rows


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
