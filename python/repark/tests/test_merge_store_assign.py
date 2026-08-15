"""MERGE INSERT ANSI store-assignment gate (audit M9, repros r13b/r13c).

Oracle: Spark's DML store-assignment policy (``Cast.canANSIStoreAssign``, default
``spark.sql.storeAssignmentPolicy=ANSI``) rejects boolean→int, timestamp→bigint and
string→numeric at ANALYSIS time (``INCOMPATIBLE_DATA_FOR_TABLE``) — far narrower than a CAST.
Before the fix repark's insert path ran the full arrow cast kernel, silently committing
``flag = 1`` for a boolean and the raw epoch micros for a timestamp. These pins are red
without ``validate_insert_store_assignment`` (merge/insert.rs). The UPDATE path is incidentally
guarded by DataFusion CASE coercion (divergent error shape — audit residual, not pinned here).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException

FQ = "mem.ns.assign_t"
SRC = "mem.ns.assign_s"
COW_PROPS = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""
NEEDLE = r"not ANSI-store-assignable"


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-merge-store-assign").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _tables(spark: ReparkSession, target_cols: str, source_cols: str) -> None:
    spark.sql(f"CREATE TABLE {FQ} ({target_cols}) USING iceberg TBLPROPERTIES ({COW_PROPS})")
    spark.sql(f"CREATE TABLE {SRC} ({source_cols}) USING iceberg TBLPROPERTIES ({COW_PROPS})")


def _merge_insert(spark: ReparkSession, columns: str, values: str) -> None:
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id "
        f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"
    )


def test_boolean_to_int_insert_refuses(spark: ReparkSession) -> None:
    """r13b: boolean source into INT target — Spark analysis error, was a silent 1."""
    _tables(spark, "id BIGINT, flag INT", "id BIGINT, b BOOLEAN")
    spark.sql(f"INSERT INTO {SRC} VALUES (2, true)")
    with pytest.raises(AnalysisException, match=NEEDLE):
        _merge_insert(spark, "id, flag", "s.id, s.b")
    assert spark.sql(f"SELECT * FROM {FQ}").to_arrow().num_rows == 0


def test_timestamp_to_bigint_insert_refuses(spark: ReparkSession) -> None:
    """r13c: timestamp source into BIGINT target — was a silent raw-epoch write."""
    _tables(spark, "id BIGINT, ts_us BIGINT", "id BIGINT, ev TIMESTAMP")
    spark.sql(f"INSERT INTO {SRC} VALUES (3, TIMESTAMP '2026-01-01 00:00:00')")
    with pytest.raises(AnalysisException, match=NEEDLE):
        _merge_insert(spark, "id, ts_us", "s.id, s.ev")
    assert spark.sql(f"SELECT * FROM {FQ}").to_arrow().num_rows == 0


def test_string_to_bigint_insert_refuses(spark: ReparkSession) -> None:
    """string→numeric is not ANSI store-assignable (Spark refuses even for numeric strings)."""
    _tables(spark, "id BIGINT, n BIGINT", "id BIGINT, txt STRING")
    spark.sql(f"INSERT INTO {SRC} VALUES (4, '42')")
    with pytest.raises(AnalysisException, match=NEEDLE):
        _merge_insert(spark, "id, n", "s.id, s.txt")


def test_numeric_widening_and_null_fill_still_insert(spark: ReparkSession) -> None:
    """Positive controls: INT→BIGINT widening passes the gate; omitted nullable NULL-fills."""
    _tables(spark, "id BIGINT, big BIGINT, extra STRING", "id BIGINT, small INT")
    spark.sql(f"INSERT INTO {SRC} VALUES (5, 9)")
    _merge_insert(spark, "id, big", "s.id, s.small")
    out = spark.sql(f"SELECT id, big, extra FROM {FQ}").to_arrow()
    assert out.to_pylist() == [{"id": 5, "big": 9, "extra": None}]


def test_atomic_to_string_still_inserts(spark: ReparkSession) -> None:
    """AtomicType→StringType is store-assignable in Spark (boolean → 'true')."""
    _tables(spark, "id BIGINT, s STRING", "id BIGINT, b BOOLEAN")
    spark.sql(f"INSERT INTO {SRC} VALUES (6, true)")
    _merge_insert(spark, "id, s", "s.id, s.b")
    assert spark.sql(f"SELECT s FROM {FQ}").to_arrow().to_pylist() == [{"s": "true"}]
