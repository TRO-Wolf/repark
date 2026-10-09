"""ICE-TSNS-MERGE-WALL-1: the write-door matrix shared by the recorder and the pins.

Every cell writes eight moments with non-zero sub-microsecond digits through one door, from one
source type, into one target column type, in one session zone, and reads the stored int64 ticks
back. ``expected_wall`` is the rule for a ``timestamp_ns`` target; every other target is a
control whose answer is main's recorded one.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa

from repark import ReparkSession, functions

ZONES = ("UTC", "Asia/Kolkata", "America/New_York")
MOMENTS = (
    "2026-01-02 03:04:05.123456789",
    "1969-12-31 23:59:59.999999999",
    "2026-03-08 02:30:00.000000001",
    "2026-11-01 01:30:00.000000001",
    "2026-03-08 06:59:59.999999999",
    "2026-03-08 07:00:00.000000001",
    "2026-11-01 05:30:00.000000001",
    "2026-11-01 06:30:00.000000001",
)
EPOCH = datetime(1970, 1, 1)
SOURCES = ("l", "n", "ns", "tzns", "s")
SOURCE_SQL_TYPES = {
    "l": "TIMESTAMP",
    "n": "TIMESTAMP_NTZ",
    "ns": "timestamp_ns",
    "tzns": "timestamptz_ns",
    "s": "STRING",
}
SOURCE_ARROW_TYPES = {
    "l": pa.timestamp("us", tz="UTC"),
    "n": pa.timestamp("us"),
    "ns": pa.timestamp("ns"),
    "tzns": pa.timestamp("ns", tz="UTC"),
    "s": pa.string(),
}
TARGETS = {
    "ts_ns": ("timestamp_ns", 3, pa.timestamp("ns")),
    "tz_ns": ("timestamptz_ns", 3, pa.timestamp("ns", tz="UTC")),
    "ntz_v3": ("TIMESTAMP_NTZ", 3, pa.timestamp("us")),
    "ltz_v3": ("TIMESTAMP", 3, pa.timestamp("us", tz="UTC")),
    "ntz_v2": ("TIMESTAMP_NTZ", 2, pa.timestamp("us")),
    "ltz_v2": ("TIMESTAMP", 2, pa.timestamp("us", tz="UTC")),
}
IDS = tuple(range(1, len(MOMENTS) + 1))
SRC = "ice.ns.src"


def nanos_of(text: str) -> int:
    """Return the int64 nanoseconds of a nine-digit wall ``text`` read as UTC."""
    whole, fraction = text.split(".")
    seconds = (datetime.fromisoformat(whole) - EPOCH) // timedelta(seconds=1)
    return seconds * 1_000_000_000 + int(fraction)


NANOS = tuple(nanos_of(text) for text in MOMENTS)


def wall_nanos(instant: int, zone: str) -> int:
    """Return the wall of ``instant`` nanoseconds in ``zone`` as naive nanoseconds."""
    micros, rest = divmod(instant, 1_000)
    local = (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)).astimezone(
        ZoneInfo(zone)
    )
    wall = (local.replace(tzinfo=None) - EPOCH) // timedelta(microseconds=1)
    return wall * 1_000 + rest


def expected_wall(source: str, zone: str) -> list[int]:
    """Return what a ``timestamp_ns`` column stores for every moment of ``source``."""
    if source == "l":
        return [wall_nanos(value // 1_000 * 1_000, zone) for value in NANOS]
    if source == "n":
        return [value // 1_000 * 1_000 for value in NANOS]
    if source == "tzns":
        return [wall_nanos(value, zone) for value in NANOS]
    return list(NANOS)


def micro_text(text: str) -> str:
    """Return ``text`` cut to six fraction digits."""
    return text[:-3]


def literal(source: str, index: int) -> str:
    """Return the SQL literal of moment ``index`` typed as ``source``."""
    text = MOMENTS[index]
    if source == "l":
        return f"TIMESTAMP '{micro_text(text)}+00:00'"
    if source == "n" and NANOS[index] < 0:
        return f"to_timestamp_ntz('{micro_text(text)}')"
    if source == "n":
        return f"TIMESTAMP_NTZ '{micro_text(text)}'"
    if source == "ns":
        return f"CAST('{text}' AS timestamp_ns)"
    if source == "tzns":
        return f"CAST('{text}+00:00' AS timestamptz_ns)"
    return f"'{text}'"


def source_array(source: str) -> pa.Array:
    """Return the Arrow array of every moment typed as ``source``."""
    if source == "s":
        return pa.array(list(MOMENTS), pa.string())
    if source in ("l", "n"):
        return pa.array([value // 1_000 for value in NANOS], SOURCE_ARROW_TYPES[source])
    return pa.array(list(NANOS), SOURCE_ARROW_TYPES[source])


def frame_of(spark: Any, table: pa.Table) -> Any:
    """Return a DataFrame over ``table`` that keeps every Arrow type."""
    return spark.createDataFrame(table.to_pandas(types_mapper=pd.ArrowDtype))


def open_session(zone: str, warehouse: Path) -> Any:
    """Open a v3-enabled session in ``zone`` with the source table written."""
    spark = (
        ReparkSession.builder.appName("ice-tsns-merge-wall-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    columns = ", ".join(f"{name} {SOURCE_SQL_TYPES[name]}" for name in SOURCES)
    spark.sql(
        f"CREATE TABLE {SRC} (id INT, {columns}) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    )
    arrays = {"id": pa.array(list(IDS), pa.int32())}
    arrays.update({name: source_array(name) for name in SOURCES})
    frame_of(spark, pa.table(arrays)).writeTo(SRC).append()
    return spark


def create_target(spark: Any, table: str, target: str, extra: str = "") -> None:
    """Create ``table`` as ``(id INT, v <target>, k INT<extra>)``."""
    sql_type, version, _ = TARGETS[target]
    spark.sql(
        f"CREATE TABLE {table} (id INT, v {sql_type}, k INT{extra}) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '{version}')"
    )


def seed_ids(spark: Any, table: str) -> None:
    """Write one NULL-valued row per moment into ``table``."""
    rows = ", ".join(f"({index}, NULL, 0)" for index in IDS)
    spark.sql(f"INSERT INTO {table} VALUES {rows}")


def select_source(source: str) -> str:
    """Return the query that projects ``source`` as the target's three columns."""
    return f"SELECT id, {source} AS v, 0 AS k FROM {SRC}"


def door_insert_values(spark: Any, table: str, source: str) -> None:
    """INSERT … VALUES of typed literals."""
    rows = ", ".join(f"({index + 1}, {literal(source, index)}, 0)" for index in range(len(IDS)))
    spark.sql(f"INSERT INTO {table} VALUES {rows}")


def door_insert_select(spark: Any, table: str, source: str) -> None:
    """INSERT … SELECT of a source column."""
    spark.sql(f"INSERT INTO {table} {select_source(source)}")


def door_insert_overwrite(spark: Any, table: str, source: str) -> None:
    """INSERT OVERWRITE … SELECT of a source column."""
    spark.sql(f"INSERT OVERWRITE {table} {select_source(source)}")


def door_insert_replace_where(spark: Any, table: str, source: str) -> None:
    """INSERT … REPLACE WHERE of a source column."""
    seed_ids(spark, table)
    spark.sql(f"INSERT INTO {table} REPLACE WHERE id >= 0 {select_source(source)}")


def door_merge_insert(spark: Any, table: str, source: str) -> None:
    """MERGE … WHEN NOT MATCHED THEN INSERT of a source column."""
    spark.sql(
        f"MERGE INTO {table} t USING {SRC} s ON t.id = s.id "
        f"WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, s.{source}, 0)"
    )


def door_merge_insert_star(spark: Any, table: str, source: str) -> None:
    """MERGE … WHEN NOT MATCHED THEN INSERT * of a source column."""
    spark.sql(
        f"MERGE INTO {table} t USING ({select_source(source)}) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def door_merge_insert_literal(spark: Any, table: str, source: str) -> None:
    """MERGE … WHEN NOT MATCHED THEN INSERT of typed literals."""
    for index in range(len(IDS)):
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT {index + 1} AS id) s ON t.id = s.id "
            f"WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {literal(source, index)}, 0)"
        )


def door_merge_update(spark: Any, table: str, source: str) -> None:
    """MERGE … WHEN MATCHED THEN UPDATE SET of a source column."""
    seed_ids(spark, table)
    spark.sql(
        f"MERGE INTO {table} t USING {SRC} s ON t.id = s.id "
        f"WHEN MATCHED THEN UPDATE SET v = s.{source}"
    )


def door_merge_update_star(spark: Any, table: str, source: str) -> None:
    """MERGE … WHEN MATCHED THEN UPDATE SET * of a source column."""
    seed_ids(spark, table)
    spark.sql(
        f"MERGE INTO {table} t USING ({select_source(source)}) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET *"
    )


def door_merge_update_literal(spark: Any, table: str, source: str) -> None:
    """MERGE … WHEN MATCHED THEN UPDATE SET of typed literals."""
    seed_ids(spark, table)
    for index in range(len(IDS)):
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT {index + 1} AS id) s ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET v = {literal(source, index)}"
        )


def door_update_literal(spark: Any, table: str, source: str) -> None:
    """UPDATE … SET of typed literals."""
    seed_ids(spark, table)
    for index in range(len(IDS)):
        spark.sql(f"UPDATE {table} SET v = {literal(source, index)} WHERE id = {index + 1}")


def door_update_column(spark: Any, table: str, source: str) -> None:
    """UPDATE … SET of a sibling column of the source type."""
    spark.sql(f"INSERT INTO {table} SELECT id, NULL, 0, {source} FROM {SRC}")
    spark.sql(f"UPDATE {table} SET v = c")


def source_frame(spark: Any, source: str) -> Any:
    """Return the DataFrame that projects ``source`` as the target's three columns."""
    return spark.table(SRC).selectExpr("id", f"{source} AS v", "0 AS k")


def door_df_append(spark: Any, table: str, source: str) -> None:
    """DataFrameWriterV2 append."""
    source_frame(spark, source).writeTo(table).append()


def door_df_overwrite_partitions(spark: Any, table: str, source: str) -> None:
    """DataFrameWriterV2 overwritePartitions."""
    seed_ids(spark, table)
    source_frame(spark, source).writeTo(table).overwritePartitions()


def door_df_overwrite_where(spark: Any, table: str, source: str) -> None:
    """DataFrameWriterV2 overwrite with a condition, the DataFrame replaceWhere."""
    seed_ids(spark, table)
    source_frame(spark, source).writeTo(table).overwrite(functions.col("id") >= 0)


def door_df_insert_into(spark: Any, table: str, source: str) -> None:
    """DataFrameWriter insertInto."""
    source_frame(spark, source).write.insertInto(table)


def door_df_insert_into_overwrite(spark: Any, table: str, source: str) -> None:
    """DataFrameWriter insertInto with overwrite."""
    seed_ids(spark, table)
    source_frame(spark, source).write.insertInto(table, overwrite=True)


def door_df_save_append(spark: Any, table: str, source: str) -> None:
    """DataFrameWriter saveAsTable in append mode."""
    source_frame(spark, source).write.mode("append").saveAsTable(table)


def door_df_arrow(spark: Any, table: str, source: str) -> None:
    """An in-memory Arrow array appended through the DataFrame door."""
    arrays = {
        "id": pa.array(list(IDS), pa.int32()),
        "v": source_array(source),
        "k": pa.array([0] * len(IDS), pa.int32()),
    }
    frame_of(spark, pa.table(arrays)).writeTo(table).append()


DOORS: dict[str, Callable[[Any, str, str], None]] = {
    "insert_values": door_insert_values,
    "insert_select": door_insert_select,
    "insert_overwrite": door_insert_overwrite,
    "insert_replace_where": door_insert_replace_where,
    "merge_insert": door_merge_insert,
    "merge_insert_star": door_merge_insert_star,
    "merge_insert_literal": door_merge_insert_literal,
    "merge_update": door_merge_update,
    "merge_update_star": door_merge_update_star,
    "merge_update_literal": door_merge_update_literal,
    "update_literal": door_update_literal,
    "update_column": door_update_column,
    "df_append": door_df_append,
    "df_overwrite_partitions": door_df_overwrite_partitions,
    "df_overwrite_where": door_df_overwrite_where,
    "df_insert_into": door_df_insert_into,
    "df_insert_into_overwrite": door_df_insert_into_overwrite,
    "df_save_append": door_df_save_append,
    "df_arrow": door_df_arrow,
}
MODES = {
    "cow": "",
    "mor": (
        ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read', "
        "'write.merge.mode' = 'merge-on-read'"
    ),
}


def carry_delete(spark: Any, table: str) -> None:
    """DELETE one row; the others carry."""
    spark.sql(f"DELETE FROM {table} WHERE id = 1")


def carry_update(spark: Any, table: str) -> None:
    """UPDATE a sibling column of every row."""
    spark.sql(f"UPDATE {table} SET k = k + 1 WHERE id > 1")
    spark.sql(f"DELETE FROM {table} WHERE id = 1")


def carry_merge(spark: Any, table: str) -> None:
    """MERGE that deletes one row and updates a sibling column of the others."""
    spark.sql(
        f"MERGE INTO {table} t USING {SRC} s ON t.id = s.id "
        "WHEN MATCHED AND t.id = 1 THEN DELETE WHEN MATCHED THEN UPDATE SET k = 7"
    )


def carry_rewrite_data_files(spark: Any, table: str) -> None:
    """Compact the data files."""
    short = table.removeprefix("ice.")
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    spark.sql(
        f"CALL ice.system.rewrite_data_files(table => '{short}', "
        "options => map('min-input-files', '2', 'rewrite-all', 'true'))"
    )


def carry_rewrite_manifests(spark: Any, table: str) -> None:
    """Rewrite the manifests."""
    short = table.removeprefix("ice.")
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    spark.sql(f"CALL ice.system.rewrite_manifests(table => '{short}')")


def carry_rewrite_position_deletes(spark: Any, table: str) -> None:
    """Rewrite the position delete files."""
    short = table.removeprefix("ice.")
    spark.sql(f"DELETE FROM {table} WHERE id = 1")
    spark.sql(f"CALL ice.system.rewrite_position_delete_files(table => '{short}')")


CARRIES: dict[str, Callable[[Any, str], None]] = {
    "delete": carry_delete,
    "update_sibling": carry_update,
    "merge_sibling": carry_merge,
    "rewrite_data_files": carry_rewrite_data_files,
    "rewrite_manifests": carry_rewrite_manifests,
    "rewrite_position_delete_files": carry_rewrite_position_deletes,
}


def seed_exact(spark: Any, table: str, target: str) -> list[int]:
    """Append every moment in the target's own Arrow type, one file per half; return the ticks."""
    arrow_type = TARGETS[target][2]
    ticks = [value if arrow_type.unit == "ns" else value // 1_000 for value in NANOS]
    for half in (slice(0, 4), slice(4, 8)):
        arrays = {
            "id": pa.array(list(IDS)[half], pa.int32()),
            "v": pa.array(ticks[half], arrow_type),
            "k": pa.array([0] * 4, pa.int32()),
        }
        frame_of(spark, pa.table(arrays)).writeTo(table).append()
    return ticks


def read_back(spark: Any, table: str) -> dict[str, Any]:
    """Return the Arrow type of ``v`` and its int64 ticks ordered by ``id``."""
    result = spark.sql(f"SELECT id, v FROM {table} ORDER BY id").to_arrow()
    column = result.column("v")
    return {
        "type": str(column.type),
        "ids": result.column("id").to_pylist(),
        "values": column.cast(pa.int64()).to_pylist(),
    }


def error_cell(error: Exception) -> dict[str, Any]:
    """Return the recorded shape of a refused cell."""
    lines = [line for line in str(error).splitlines() if line.strip()]
    return {"error": type(error).__name__, "message": (lines[-1] if lines else "")[:260]}


def cell_key(zone: str, target: str, door: str, part: str) -> str:
    """Return the fixture key of one cell."""
    return f"{zone}|{target}|{door}|{part}"


def measure_door(spark: Any, target: str, door: str) -> dict[str, Any]:
    """Run ``door`` once per source type into fresh ``target`` tables; return part -> cell."""
    cells: dict[str, Any] = {}
    for source in SOURCES:
        table = f"ice.ns.t_{source}"
        extra = f", c {SOURCE_SQL_TYPES[source]}" if door == "update_column" else ""
        try:
            create_target(spark, table, target, extra)
            DOORS[door](spark, table, source)
            cells[source] = read_back(spark, table)
        except Exception as error:
            cells[source] = error_cell(error)
    return cells


def measure_carry(spark: Any, target: str, carry: str) -> dict[str, Any]:
    """Run ``carry`` in both row-level modes over exactly seeded tables; return part -> cell."""
    cells: dict[str, Any] = {}
    sql_type, version, _ = TARGETS[target]
    for mode, properties in MODES.items():
        table = f"ice.ns.t_{mode}"
        try:
            spark.sql(
                f"CREATE TABLE {table} (id INT, v {sql_type}, k INT) USING iceberg "
                f"TBLPROPERTIES ('format-version' = '{version}'{properties})"
            )
            ticks = seed_exact(spark, table, target)
            CARRIES[carry](spark, table)
            cell = read_back(spark, table)
            cell["seeded"] = ticks[1:]
            cells[mode] = cell
        except Exception as error:
            cells[mode] = error_cell(error)
    return cells


def measure(spark: Any, target: str, door: str) -> dict[str, Any]:
    """Measure one door or one ``carry_``-prefixed carry; return part -> cell."""
    if door.startswith("carry_"):
        return measure_carry(spark, target, door.removeprefix("carry_"))
    return measure_door(spark, target, door)


ALL_DOORS = (*DOORS, *(f"carry_{name}" for name in CARRIES))
