"""The joined head stores the wall rows of the nested-NTZ write shapes.

pins: ice-ntz-nested-wall-1/C-003
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from ice_ntz_nested_wall_1_helpers import (
    WALL_TICK,
    open_session,
    stored_rows,
)

WALL = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
WALL_SUMMER = "TIMESTAMP_NTZ '2026-07-04 07:22:33.654321'"
INSTANT_NY = "TIMESTAMP_LTZ '2026-01-02 08:04:05.123456+00:00'"
INSTANT_UTC = "TIMESTAMP_LTZ '2026-01-02 03:04:05.123456+00:00'"
INSTANT_JULY = "TIMESTAMP_LTZ '2026-07-04 11:22:33.654321+00:00'"
NTZ_STRUCT = "STRUCT<v: TIMESTAMP_NTZ, n: INT>"
LTZ_STRUCT = "STRUCT<v: TIMESTAMP_LTZ, n: INT>"
JAN_NY = 1_767_305_045_123_456
JULY_NY = 1_783_149_753_654_321
ZONE = "America/New_York"
ExpectedRow = tuple[int, int | None, int | None]


def seed_mixed(spark: Any) -> None:
    """Create the wall side and the instant side of the mixed inventory."""
    spark.sql(f"CREATE TABLE ice.ns.a (id INT, st {NTZ_STRUCT}) USING iceberg")
    spark.sql(f"INSERT INTO ice.ns.a SELECT 1, named_struct('v', {WALL}, 'n', CAST(7 AS INT))")
    spark.sql("INSERT INTO ice.ns.a SELECT 2, NULL")
    spark.sql(
        "INSERT INTO ice.ns.a SELECT 3, "
        "named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', CAST(8 AS INT))"
    )
    spark.sql(
        f"INSERT INTO ice.ns.a SELECT 4, named_struct('v', {WALL_SUMMER}, 'n', CAST(7 AS INT))"
    )
    spark.sql(f"CREATE TABLE ice.ns.b (id INT, st {LTZ_STRUCT}) USING iceberg")
    spark.sql(
        f"INSERT INTO ice.ns.b SELECT 1, named_struct('v', {INSTANT_NY}, 'n', CAST(7 AS INT))"
    )
    spark.sql(
        f"INSERT INTO ice.ns.b SELECT 2, named_struct('v', {INSTANT_UTC}, 'n', CAST(7 AS INT))"
    )
    spark.sql(
        f"INSERT INTO ice.ns.b SELECT 3, named_struct('v', {INSTANT_JULY}, 'n', CAST(7 AS INT))"
    )
    spark.sql("INSERT INTO ice.ns.b SELECT 4, NULL")


def seed_target(spark: Any) -> None:
    """Create the nested-NTZ target with its instant sibling column."""
    spark.sql(f"CREATE TABLE ice.ns.tt (id INT, st {NTZ_STRUCT}, c {LTZ_STRUCT}) USING iceberg")
    spark.sql(
        "INSERT INTO ice.ns.tt SELECT 1, "
        f"named_struct('v', {WALL}, 'n', CAST(7 AS INT)), "
        f"named_struct('v', {INSTANT_JULY}, 'n', CAST(7 AS INT))"
    )
    spark.sql(
        "INSERT INTO ice.ns.tt SELECT 2, "
        f"named_struct('v', {WALL}, 'n', CAST(7 AS INT)), "
        f"named_struct('v', {INSTANT_JULY}, 'n', CAST(7 AS INT))"
    )
    spark.sql(
        "INSERT INTO ice.ns.tt SELECT 3, NULL, "
        f"named_struct('v', {INSTANT_NY}, 'n', CAST(7 AS INT))"
    )


def write_insert_case(spark: Any) -> str:
    """Insert the mixed CASE into a nested-NTZ target."""
    spark.sql(f"CREATE TABLE ice.ns.wn2 (id INT, st {NTZ_STRUCT}) USING iceberg")
    spark.sql(
        "INSERT INTO ice.ns.wn2 SELECT a.id AS id, "
        "CASE WHEN a.id = 2 THEN b.st ELSE a.st END AS mixed "
        "FROM ice.ns.a a JOIN ice.ns.b b ON a.id = b.id"
    )
    return "ice.ns.wn2"


def write_insert_coalesce(spark: Any) -> str:
    """Insert the mixed COALESCE into a nested-NTZ target."""
    spark.sql(f"CREATE TABLE ice.ns.wn3 (id INT, st {NTZ_STRUCT}) USING iceberg")
    spark.sql(
        "INSERT INTO ice.ns.wn3 SELECT a.id AS id, COALESCE(a.st, b.st) AS mixed "
        "FROM ice.ns.a a JOIN ice.ns.b b ON a.id = b.id"
    )
    return "ice.ns.wn3"


def write_insert_overwrite_union(spark: Any) -> str:
    """Overwrite a nested-NTZ target with the mixed union."""
    spark.sql(f"CREATE TABLE ice.ns.wn5 (id INT, st {NTZ_STRUCT}) USING iceberg")
    spark.sql(
        "INSERT OVERWRITE ice.ns.wn5 SELECT id, st AS mixed FROM ice.ns.a "
        "UNION ALL SELECT id + 10 AS id, st AS mixed FROM ice.ns.b"
    )
    return "ice.ns.wn5"


def write_update_case_where(spark: Any) -> str:
    """Update a nested-NTZ target with the mixed CASE."""
    seed_target(spark)
    spark.sql("UPDATE ice.ns.tt SET st = CASE WHEN id = 1 THEN c ELSE st END WHERE id < 3")
    return "ice.ns.tt"


def write_merge_update_case(spark: Any) -> str:
    """Merge-update a nested-NTZ target with the mixed CASE."""
    seed_target(spark)
    spark.sql(
        "MERGE INTO ice.ns.tt t USING ice.ns.b s ON t.id = s.id WHEN MATCHED "
        "THEN UPDATE SET t.st = CASE WHEN s.id = 1 THEN s.st ELSE t.st END"
    )
    return "ice.ns.tt"


def write_merge_insert_coalesce(spark: Any) -> str:
    """Merge-insert the mixed COALESCE into a nested-NTZ target."""
    seed_target(spark)
    spark.sql(
        "MERGE INTO ice.ns.tt t USING (SELECT a.id + 100 AS id, "
        "COALESCE(a.st, b.st) AS st FROM ice.ns.a a "
        "JOIN ice.ns.b b ON a.id = b.id) s ON t.id = s.id WHEN NOT MATCHED "
        "THEN INSERT (id, st, c) VALUES (s.id, s.st, s.st)"
    )
    return "ice.ns.tt"


def write_merge_source_union(spark: Any) -> str:
    """Merge-insert the mixed union into a nested-NTZ target."""
    seed_target(spark)
    spark.sql(
        "MERGE INTO ice.ns.tt t USING (SELECT id + 100 AS id, mixed AS st "
        "FROM (SELECT id, st AS mixed FROM ice.ns.a UNION ALL "
        "SELECT id + 10 AS id, st AS mixed FROM ice.ns.b) u) s "
        "ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.st)"
    )
    return "ice.ns.tt"


def write_df_append(spark: Any) -> str:
    """Append the mixed union through the DataFrame writer."""
    spark.sql(f"CREATE TABLE ice.ns.wn6 (id INT, st {NTZ_STRUCT}) USING iceberg")
    frame = spark.sql(
        "SELECT id, st AS mixed FROM ice.ns.a UNION ALL "
        "SELECT id + 10 AS id, st AS mixed FROM ice.ns.b"
    )
    frame.withColumnRenamed("mixed", "st").writeTo("ice.ns.wn6").append()
    return "ice.ns.wn6"


CASE_ROWS = [(1, WALL_TICK, 7), (2, JAN_NY, 7), (3, None, 8), (4, JULY_NY, 7)]
UNION_ROWS = [
    (1, WALL_TICK, 7),
    (2, None, None),
    (3, None, 8),
    (4, JULY_NY, 7),
    (11, WALL_TICK, 7),
    (12, JAN_NY, 7),
    (13, JULY_NY, 7),
    (14, None, None),
]
SHAPES: tuple[tuple[str, Callable[[Any], str], list[tuple[int, int | None, int | None]]], ...] = (
    ("insert-case", write_insert_case, CASE_ROWS),
    ("insert-coalesce", write_insert_coalesce, CASE_ROWS),
    ("insert-overwrite-union", write_insert_overwrite_union, UNION_ROWS),
    (
        "update-case-where",
        write_update_case_where,
        [(1, JULY_NY, 7), (2, WALL_TICK, 7), (3, None, None)],
    ),
    (
        "merge-update-case",
        write_merge_update_case,
        [(1, WALL_TICK, 7), (2, WALL_TICK, 7), (3, None, None)],
    ),
    (
        "merge-insert-coalesce",
        write_merge_insert_coalesce,
        [
            (1, WALL_TICK, 7),
            (2, WALL_TICK, 7),
            (3, None, None),
            (101, WALL_TICK, 7),
            (102, JAN_NY, 7),
            (103, None, 8),
            (104, JULY_NY, 7),
        ],
    ),
    (
        "merge-source-union",
        write_merge_source_union,
        [
            (1, WALL_TICK, 7),
            (2, WALL_TICK, 7),
            (3, None, None),
            (101, WALL_TICK, 7),
            (102, None, None),
            (103, None, 8),
            (104, JULY_NY, 7),
            (111, WALL_TICK, 7),
            (112, JAN_NY, 7),
            (113, JULY_NY, 7),
            (114, None, None),
        ],
    ),
    ("df-append", write_df_append, UNION_ROWS),
)


@pytest.mark.parametrize(("shape", "write", "expected"), SHAPES)
def test_joined_write_shapes_store_the_wall_rows(
    tmp_path: Path, shape: str, write: Callable[[Any], str], expected: list[ExpectedRow]
) -> None:
    """Each joined write shape stores the wall rows Spark stores."""
    spark = open_session(ZONE, tmp_path)
    try:
        seed_mixed(spark)
        table = write(spark)
        assert stored_rows(spark, table) == expected, shape
    finally:
        spark.stop()
