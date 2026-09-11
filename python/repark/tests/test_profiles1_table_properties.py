"""`write.*` table-property pins — REVIEW-FIX-8 (RF-4, Q-40).

The session accepts and stores any `write.*` value; the engine applies the
table's own property at write time (or refuses loud there). Both legs are
measured here against the in-memory Iceberg catalog (local only — no AWS).

pins: review-fix-8/C-004
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession


@pytest.fixture
def spark(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ReparkSession:
    monkeypatch.setenv("REPARK_CONFIG", "")
    session = ReparkSession.builder.appName("pytest-write-table-props").getOrCreate()
    session.register_memory_catalog("mem", tmp_path)
    session.sql("CREATE NAMESPACE mem.ns")
    return session


def _data_files(warehouse: Path, table: str) -> int:
    roots = list((warehouse / "repark_ctas" / "mem" / "ns").rglob(table))
    assert len(roots) == 1
    return len(list((roots[0] / "data").rglob("*.parquet")))


def _merge_rewrite(spark: ReparkSession, warehouse: Path, table: str, props: str) -> int:
    spark.sql(f"CREATE TABLE mem.ns.{table} (id BIGINT, v STRING) USING iceberg {props}")
    spark.sql(
        f"INSERT INTO mem.ns.{table} SELECT value AS id, CAST(value AS STRING) AS v "
        "FROM range(20000)"
    )
    spark.sql("CREATE TABLE mem.ns.src (id BIGINT, v STRING) USING iceberg")
    spark.sql("INSERT INTO mem.ns.src SELECT value AS id, 'w' AS v FROM range(20000)")
    spark.sql(
        f"MERGE INTO mem.ns.{table} AS x USING mem.ns.src AS y ON x.id = y.id "
        "WHEN MATCHED THEN UPDATE SET v = y.v"
    )
    spark.sql("DROP TABLE mem.ns.src")
    rows = spark.sql(f"SELECT count(*) AS n FROM mem.ns.{table}").to_arrow().to_pylist()
    assert rows == [{"n": 20000}]
    return _data_files(warehouse, table)


def test_bogus_distribution_mode_refuses_at_write(spark: ReparkSession) -> None:
    """An invalid `write.distribution-mode` table property refuses loud at CTAS."""
    with pytest.raises(Exception, match="not supported"):
        spark.sql(
            "CREATE TABLE mem.ns.c_bogus USING iceberg PARTITIONED BY (part) "
            "TBLPROPERTIES ('write.distribution-mode' = 'bogus') AS SELECT value AS id, "
            "CAST(value % 8 AS INT) AS part FROM range(200000)"
        )


def test_target_file_size_applies_at_table(spark: ReparkSession, tmp_path: Path) -> None:
    """A tiny `write.target-file-size-bytes` property splits the MERGE rewrite."""
    tiny = _merge_rewrite(
        spark, tmp_path, "t_tiny", "TBLPROPERTIES ('write.target-file-size-bytes' = '1')"
    )
    plain = _merge_rewrite(spark, tmp_path, "t_plain", "")
    assert tiny > plain
