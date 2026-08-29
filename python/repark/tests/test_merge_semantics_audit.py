"""MERGE-audit corpus — green pins for gap-map holes verified in the MERGE audit.

Oracle surface: Spark 4 MERGE semantics (each docstring names the semantic; recipes verified
against the engine during the audit; see planning/hardening/MERGE-AUDIT-FINDINGS.md, gap-map
rows c/d/n/o/g and finding M21). End-to-end pins run against the in-memory Iceberg catalog
(local only — no AWS). All assertions ride the Arrow path (``to_arrow``), value AND type.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark.functions import col

FQ = "mem.ns.audit"
SRC = "mem.ns.audit_src"
COW_PROPS = """
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
"""


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-merge-audit").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _create(spark: ReparkSession, table: str, columns: str) -> None:
    spark.sql(f"CREATE TABLE {table} ({columns}) USING iceberg TBLPROPERTIES ({COW_PROPS})")


def _rows(spark: ReparkSession, sql: str) -> list[dict[str, object]]:
    return spark.sql(sql).to_arrow().to_pylist()


def _arrow(spark: ReparkSession, sql: str) -> pa.Table:
    return spark.sql(sql).to_arrow()


def test_merge_null_safe_on_matches_null_keys_sql_door(spark: ReparkSession) -> None:
    """``ON t.id <=> s.id``: NULL matches NULL (Spark null-safe equality) — gap-map (d), M21.

    Guards the scan-pruning skip for null-safe conditions (scan_prune.rs `<=>` guard): a
    pruning regression would lose the NULL-keyed match (update lost) instead of failing loud.
    """
    _create(spark, FQ, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {FQ} VALUES (NULL,'x'),(5,'y')")
    _create(spark, SRC, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {SRC} VALUES (NULL,'A'),(5,'B')")
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id <=> s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v"
    )
    out = _arrow(spark, f"SELECT id, v FROM {FQ} ORDER BY id NULLS LAST")
    assert out.schema.field("id").type == pa.int64()
    assert out.schema.field("v").type == pa.large_string() or pa.types.is_string(
        out.schema.field("v").type
    )
    assert out.to_pylist() == [
        {"id": 5, "v": "B"},
        {"id": None, "v": "A"},
    ]


def test_merge_null_safe_on_matches_null_keys_builder_door(spark: ReparkSession) -> None:
    """Builder ``eqNullSafe`` condition (lowers to IS NOT DISTINCT FROM) — gap-map (d), M21.

    The Python door never emits ``<=>`` (Column.eqNullSafe renders IS NOT DISTINCT FROM), so
    this pins the second spelling of the same null-safe join through MergeIntoWriter.
    """
    _create(spark, FQ, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {FQ} VALUES (NULL,'x'),(5,'y')")
    source = spark.sql(
        "SELECT CAST(NULL AS BIGINT) AS id, 'A' AS v UNION ALL SELECT CAST(5 AS BIGINT), 'B'"
    )
    (
        source.mergeInto(FQ, col("target.id").eqNullSafe(col("source.id")))
        .whenMatched()
        .update({"v": col("source.v")})
        .merge()
    )
    out = _rows(spark, f"SELECT id, v FROM {FQ} ORDER BY id NULLS LAST")
    assert out == [{"id": 5, "v": "B"}, {"id": None, "v": "A"}]


def test_merge_null_keys_do_not_match_builder_door(spark: ReparkSession) -> None:
    """Builder-door ``=`` join: NULL never matches NULL (3VL) — gap-map (c) builder hole.

    The SQL-door twin is the differential-parity row ``null_merge_keys_do_not_match``; this
    pins the same semantics through MergeIntoWriter: the NULL-keyed target row survives
    unchanged and the NULL-keyed source row is NOT MATCHED (inserted).
    """
    _create(spark, FQ, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {FQ} VALUES (NULL,'keep'),(5,'y')")
    source = spark.sql(
        "SELECT CAST(NULL AS BIGINT) AS id, 'new' AS v UNION ALL SELECT CAST(5 AS BIGINT), 'B'"
    )
    (
        source.mergeInto(FQ, col("target.id") == col("source.id"))
        .whenMatched()
        .update({"v": col("source.v")})
        .whenNotMatched()
        .insert({"id": col("source.id"), "v": col("source.v")})
        .merge()
    )
    out = _rows(spark, f"SELECT id, v FROM {FQ} ORDER BY id NULLS LAST, v")
    assert out == [
        {"id": 5, "v": "B"},
        {"id": None, "v": "keep"},
        {"id": None, "v": "new"},
    ]


def test_merge_self_merge_target_as_source(spark: ReparkSession) -> None:
    """Self-merge (``USING <target>``): every row matches itself and updates — gap-map (o).

    Spark permits a MERGE whose source reads the target; the scan must see the pre-merge
    snapshot (no Halloween problem: each row updates exactly once).
    """
    _create(spark, FQ, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {FQ} VALUES (1,'a'),(2,'b')")
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {FQ} AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = 'z'"
    )
    out = _rows(spark, f"SELECT id, v FROM {FQ} ORDER BY id")
    assert out == [{"id": 1, "v": "z"}, {"id": 2, "v": "z"}]


def test_merge_join_key_update_on_unpartitioned_target(spark: ReparkSession) -> None:
    """UPDATE may assign the join-key column itself — gap-map (n), decoupled from partitioning.

    The only prior pin (PIN R2) conflates key-update with bucket-partition rerouting; this
    pins the plain unpartitioned path.
    """
    _create(spark, FQ, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {FQ} VALUES (1,'a'),(2,'b')")
    _create(spark, SRC, "id BIGINT, newid BIGINT")
    spark.sql(f"INSERT INTO {SRC} VALUES (1, 100)")
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET id = s.newid"
    )
    out = _arrow(spark, f"SELECT id, v FROM {FQ} ORDER BY id")
    assert out.schema.field("id").type == pa.int64()
    assert out.to_pylist() == [{"id": 2, "v": "b"}, {"id": 100, "v": "a"}]


def test_merge_partial_insert_null_fills_omitted_nullable_column(spark: ReparkSession) -> None:
    """Explicit ``INSERT (subset)`` NULL-fills omitted nullable columns end-to-end — gap-map (g).

    The validation side (required column refuse, unknown/duplicate column) is pinned in the
    engine battery; this pins the committed NULL-fill through the SQL door.
    """
    _create(spark, FQ, "id BIGINT, v STRING, extra STRING")
    _create(spark, SRC, "id BIGINT, v STRING")
    spark.sql(f"INSERT INTO {SRC} VALUES (1,'a')")
    spark.sql(
        f"MERGE INTO {FQ} AS t USING {SRC} AS s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    )
    out = _arrow(spark, f"SELECT id, v, extra FROM {FQ}")
    assert (
        pa.types.is_string(out.schema.field("extra").type)
        or out.schema.field("extra").type == pa.large_string()
    )
    assert out.to_pylist() == [{"id": 1, "v": "a", "extra": None}]
