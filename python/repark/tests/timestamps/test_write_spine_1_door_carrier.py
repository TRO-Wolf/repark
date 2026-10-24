"""WRITE-SPINE-1 step 3a round 2: the static door classifier and the runtime door record."""

from __future__ import annotations

import pytest

import repark
from repark import ReparkSession, _native

STRUCT_TABLE = "(id INT, st STRUCT<v: INT, w: INT>, k INT) USING iceberg"
SOURCE_ROW = "SELECT 1 AS id, named_struct('v', 7, 'w', 8) AS st, 0 AS k"
MERGE_HEAD = "MERGE INTO mem.ns.t t USING mem.ns.s s ON t.id = s.id"
OPTION = {"snapshot-property.write-spine-1": "carrier"}
STATIC_CASES: tuple[tuple[str, dict[str, str] | None, str | None], ...] = (
    (f"INSERT INTO mem.ns.t {SOURCE_ROW}", None, "D1"),
    (f"INSERT INTO mem.ns.t {SOURCE_ROW}", OPTION, "D2"),
    (f"INSERT INTO mem.ns.t BY NAME {SOURCE_ROW}", None, "D3"),
    (f"INSERT OVERWRITE mem.ns.t {SOURCE_ROW}", None, "D4"),
    (f"INSERT INTO mem.ns.t REPLACE WHERE k = 0 {SOURCE_ROW}", None, "D5"),
    (f"CREATE TABLE mem.ns.c USING iceberg AS {SOURCE_ROW}", None, "D6"),
    (f"{MERGE_HEAD} WHEN NOT MATCHED THEN INSERT *", None, "D7"),
    (f"{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.st = s.st", None, "D8"),
    (f"{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.st.v = s.k", None, "D9"),
    ("UPDATE mem.ns.t SET k = 2 WHERE id = 1", None, "D10"),
    ("DELETE FROM mem.ns.t WHERE id = 1", None, None),
    ("SELECT 1", None, None),
)
RUNTIME_CASES: tuple[tuple[str, list[str]], ...] = (
    (f"INSERT INTO mem.ns.t {SOURCE_ROW}", ["D1"]),
    (f"INSERT INTO mem.ns.t BY NAME {SOURCE_ROW}", ["D3"]),
    (f"INSERT OVERWRITE mem.ns.t {SOURCE_ROW}", ["D4"]),
    (
        f"{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.st = s.st WHEN NOT MATCHED THEN INSERT *",
        ["D8", "D7"],
    ),
    (f"{MERGE_HEAD} WHEN MATCHED THEN UPDATE SET t.st.v = s.k", ["D9"]),
    ("UPDATE mem.ns.t SET k = 2 WHERE id = 1", ["D10"]),
    ("SELECT * FROM mem.ns.t", []),
)


@pytest.mark.parametrize(("sql", "options", "door"), STATIC_CASES)
def test_write_door_of_names_the_door_of_each_spark_spelling_without_running_it(
    sql: str, options: dict[str, str] | None, door: str | None
) -> None:
    """The classifier reads the router's intercepts and the parsed statement, nothing else."""
    assert _native.write_door_of(sql, options) == door


def test_last_write_doors_records_the_doors_each_statement_took(
    spark_with_warehouse: ReparkSession,
) -> None:
    """The Spark session records the doors of its last statement and clears them per statement."""
    for setup in (
        f"CREATE TABLE mem.ns.t {STRUCT_TABLE}",
        f"CREATE TABLE mem.ns.s {STRUCT_TABLE}",
        f"INSERT INTO mem.ns.s {SOURCE_ROW}",
    ):
        spark_with_warehouse.sql(setup).collect()
    native = spark_with_warehouse._ensure_alive()
    for sql, expected in RUNTIME_CASES:
        spark_with_warehouse.sql(sql).collect()
        assert _native.last_write_doors(native) == expected, sql


def test_the_bare_sql_session_records_no_door() -> None:
    """``repark.sql()`` runs outside the Spark router, so its session records nothing."""
    repark.sql("SELECT 1").collect()
    assert _native.last_write_doors(repark._ANSI_NATIVE) == []
