"""PG4 acceptance battery -- env-gated; skip-loud without REPARK_PG_DSN (P7/P9).

Local memory Iceberg only; agents never open AWS. Scale default 100_000 via
REPARK_PG_SCALE integer; timings recorded, never gated. Registered-catalog SQL
and MERGE INTO ice USING pg.* are the dogfood path (criteria 2-3).
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from repark import SparkSession


def _dsn_or_skip() -> str | None:
    dsn = os.environ.get("REPARK_PG_DSN", "").strip()
    if not dsn:
        print(
            "SKIP-LOUD: REPARK_PG_DSN unset -- PG4 acceptance battery not run "
            "(orchestrator injects DSN; agents never invent credentials)"
        )
        return None
    return dsn


def _scale() -> int:
    """Default 100_000; integer opt-in (e.g. 1_000_000). No silent down-cap."""
    raw = os.environ.get("REPARK_PG_SCALE", "").strip()
    if not raw:
        return 100_000
    value = int(raw)
    if value <= 0:
        raise ValueError("REPARK_PG_SCALE must be a positive integer")
    return value


def test_pg_scale_default_is_100k() -> None:
    old = os.environ.pop("REPARK_PG_SCALE", None)
    try:
        assert _scale() == 100_000
    finally:
        if old is not None:
            os.environ["REPARK_PG_SCALE"] = old


def test_pg_scale_honors_env_without_silent_cap() -> None:
    """Criterion 3: default/env scale is the run scale -- no silent 1000-row clamp."""
    old = os.environ.get("REPARK_PG_SCALE")
    try:
        os.environ["REPARK_PG_SCALE"] = "100000"
        assert _scale() == 100_000
        os.environ["REPARK_PG_SCALE"] = "1000000"
        assert _scale() == 1_000_000
    finally:
        if old is None:
            os.environ.pop("REPARK_PG_SCALE", None)
        else:
            os.environ["REPARK_PG_SCALE"] = old


def test_acceptance_battery_or_skip(tmp_path: Path) -> None:
    dsn = _dsn_or_skip()
    report = Path("task/pg-integration-report.md")
    if dsn is None:
        report.write_text(
            "# PG integration report\n\n"
            "**Status:** SKIP-LOUD -- `REPARK_PG_DSN` unset at agent run.\n"
            "Default scale: 100_000 (`REPARK_PG_SCALE` integer opt-in for 1_000_000).\n"
            "Battery paths (when live): registered-catalog `SELECT pg.schema.table`, "
            "`MERGE INTO ice… USING pg.…`, jdbc types-zoo, scale timing.\n"
            "Units touch local memory-catalog Iceberg only (P9).\n",
            encoding="utf-8",
        )
        return

    warehouse = tmp_path / "iceberg_wh"
    warehouse.mkdir()
    scale = _scale()  # no silent down-cap (criterion 3)

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.catalog.ice", "memory")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .config("spark.sql.catalog.pg", "jdbc")
        .config("spark.sql.catalog.pg.url", dsn)
        .getOrCreate()
    )

    # (0) Registered-catalog SQL -- catalog.schema.table name resolution (not only jdbc).
    t0 = time.perf_counter()
    catalog_rows = spark.sql(
        "SELECT table_schema, table_name FROM pg.information_schema.tables "
        "WHERE table_schema = 'information_schema' LIMIT 5"
    ).collect()
    t1 = time.perf_counter()
    assert len(catalog_rows) >= 1, "registered catalog SELECT pg.information_schema.tables"

    # (1) Types-zoo via format postgres (subquery; no DDL into foreign schemas).
    zoo = (
        spark.read.format("postgres")
        .option("url", dsn)
        .option(
            "dbtable",
            "(SELECT 1::int4 AS i4, 2::int8 AS i8, true::bool AS b, "
            "'x'::text AS t, 1.5::float8 AS f) AS zoo",
        )
        .load()
    )
    assert len(zoo.collect()) == 1
    t2 = time.perf_counter()

    # (2) Scale timing -- full REPARK_PG_SCALE (default 100k); record, never gate.
    single = spark.read.jdbc(
        dsn,
        f"(SELECT generate_series(1, {scale}) AS id) AS g",
        properties={},
    )
    counted = single.count()
    t3 = time.perf_counter()
    assert counted == scale

    # (3) Heterogeneous: MERGE INTO local Iceberg USING registered pg catalog table
    # (three-part pg.schema.table -- criterion 2-3; not a jdbc temp-view workaround).
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    spark.sql("CREATE TABLE ice.silver.tgt AS SELECT CAST('' AS VARCHAR) AS table_name")
    spark.sql(
        "MERGE INTO ice.silver.tgt AS t "
        "USING pg.information_schema.tables AS s "
        "ON t.table_name = s.table_name "
        "WHEN NOT MATCHED AND s.table_schema = 'information_schema' "
        "AND s.table_name = 'tables' "
        "THEN INSERT (table_name) VALUES (s.table_name)"
    )
    joined = spark.sql(
        "SELECT COUNT(*) AS n FROM ice.silver.tgt t "
        "JOIN pg.information_schema.tables p "
        "ON t.table_name = p.table_name "
        "WHERE p.table_schema = 'information_schema'"
    ).collect()
    t4 = time.perf_counter()
    merged = spark.sql("SELECT table_name FROM ice.silver.tgt").collect()
    assert len(merged) >= 1
    assert int(joined[0]["n"]) >= 1

    report.write_text(
        "# PG integration report\n\n"
        f"**Status:** OK (live DSN present)\n\n"
        f"- Scale: {scale} (default 100_000; no silent cap)\n"
        f"- Registered-catalog SELECT wall_s: {t1 - t0:.4f} rows={len(catalog_rows)}\n"
        f"- Types-zoo wall_s: {t2 - t1:.4f}\n"
        f"- Scale count wall_s: {t3 - t2:.4f} count={counted}\n"
        f"- MERGE INTO ice USING pg (catalog path) wall_s: {t4 - t3:.4f}\n"
        f"- MERGE result rows: {len(merged)}\n"
        "- Local memory Iceberg only (P9); no AWS.\n",
        encoding="utf-8",
    )
