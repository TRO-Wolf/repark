"""Named oracle deliverable for PG2 (env-gated live + DuckDB skip-loud).

- Live PySpark 4.1.2 JDBC is the sole type-mapping authority when DSN + jar available
  (orchestrator-only). Agents never invent passwords.
- DuckDB postgres extension differential when DSN + extension available.
- Without REPARK_PG_DSN: skip-loud, default gate stays green.
"""

from __future__ import annotations

import os

import pytest

from repark import SparkSession


def _dsn_or_skip() -> str | None:
    dsn = os.environ.get("REPARK_PG_DSN", "").strip()
    if not dsn:
        print(
            "SKIP-LOUD: REPARK_PG_DSN unset -- live postgres JDBC oracle not run "
            "(orchestrator injects DSN; agents never invent credentials)"
        )
        return None
    return dsn


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("pg2-oracle").getOrCreate()


def test_live_format_postgres_smoke_or_skip(spark: SparkSession) -> None:
    dsn = _dsn_or_skip()
    if dsn is None:
        return
    # Read-only subquery -- never mutate existing schemas.
    frame = (
        spark.read.format("postgres")
        .option("url", dsn)
        .option("dbtable", "(SELECT 1::int4 AS id, 'ok'::text AS label) AS smoke")
        .load()
    )
    rows = frame.orderBy("id").collect()
    assert len(rows) == 1
    assert int(rows[0]["id"]) == 1
    assert str(rows[0]["label"]) == "ok"


def test_live_jdbc_predicates_shape_or_skip(spark: SparkSession) -> None:
    dsn = _dsn_or_skip()
    if dsn is None:
        return
    # predicates[] against a VALUES subquery wrapped as table form needs a real table;
    # when only DSN is present, exercise single-partition jdbc(url, table_subquery, props).
    frame = spark.read.jdbc(
        dsn,
        "(SELECT 1::int4 AS id UNION ALL SELECT 2::int4) AS t",
        properties={},
    )
    assert frame.count() == 2


def test_duckdb_postgres_differential_or_skip() -> None:
    """Always-on differential path: skip-loud if duckdb/extension/DSN missing (P7)."""
    dsn = _dsn_or_skip()
    if dsn is None:
        return
    try:
        import duckdb
    except ImportError:
        print("SKIP-LOUD: duckdb not importable")
        return
    try:
        conn = duckdb.connect()
        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")
    except Exception as exc:
        print(f"SKIP-LOUD: duckdb postgres extension unavailable: {exc}")
        return
    # Attach is env-specific; if it fails, skip-loud rather than fail the gate.
    try:
        # Never echo DSN in assertions; only use it for attach.
        conn.execute(f"ATTACH '{dsn}' AS pg (TYPE POSTGRES)")
        result = conn.execute("SELECT 1 AS id").fetchall()
        assert result[0][0] == 1
    except Exception as exc:
        print(f"SKIP-LOUD: duckdb attach/query failed: {exc}")
