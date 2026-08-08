"""T6 / CQ-008 / BUG-007 — catalog listing list-on-access (out-of-band create/drop).

Pins: after a Catalog-API create that does **not** re-register the DataFusion provider,
``spark.catalog.listTables`` still sees the table; after an out-of-band drop it is absent
(not a phantom). Includes OOB drop of a **DF-known** name (product CREATE then Catalog-API
drop without reregister) — must not hard-fail via information_schema (F-T6-PHANTOM-A /
F-T6-PIN-DROP-A). Temps still list when Iceberg side is cleaned (F-T6-TEMP-A). Free SQL
residual (provider snapshot) is covered in the Rust residual pin and ADR-0004 — not claimed
fixed here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession


@pytest.fixture
def spark_wh(tmp_path: Path) -> tuple[ReparkSession, str]:
    """Session with memory catalog + namespace; warehouse path for OOB table locations."""
    warehouse = str(tmp_path / "wh")
    Path(warehouse).mkdir(parents=True, exist_ok=True)
    session = ReparkSession.builder.appName("pytest-catalog-staleness").getOrCreate()
    session.register_memory_catalog("glue_catalog", warehouse)
    session.sql("CREATE NAMESPACE glue_catalog.ns1")
    session.sql("CREATE TABLE glue_catalog.ns1.entity AS SELECT 1 AS id")
    return session, warehouse


def test_list_tables_sees_oob_create(spark_wh: tuple[ReparkSession, str]) -> None:
    """Out-of-band create (no DF reregister) → listTables includes the new name."""
    spark, warehouse = spark_wh
    spark.catalog.setCurrentCatalog("glue_catalog")
    before = {table.name for table in spark.catalog.listTables("ns1")}
    assert "entity" in before
    assert "oob_t" not in before

    spark._testing_oob_create_table("glue_catalog", "ns1", "oob_t", warehouse)

    after = {table.name for table in spark.catalog.listTables("ns1")}
    assert "oob_t" in after
    assert "entity" in after


def test_list_tables_drop_oob_absent_not_phantom(
    spark_wh: tuple[ReparkSession, str],
) -> None:
    """Out-of-band drop of a never-in-DF name → listTables must not phantom the name."""
    spark, warehouse = spark_wh
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark._testing_oob_create_table("glue_catalog", "ns1", "to_drop", warehouse)
    assert "to_drop" in {table.name for table in spark.catalog.listTables("ns1")}

    spark._testing_oob_drop_table("glue_catalog", "ns1", "to_drop")

    names = {table.name for table in spark.catalog.listTables("ns1")}
    assert "to_drop" not in names
    assert "entity" in names


def test_list_tables_oob_drop_df_known_absent_not_crash(
    spark_wh: tuple[ReparkSession, str],
) -> None:
    """F-T6-PIN-DROP-A / F-T6-PHANTOM-A: OOB drop of a **DF-known** Iceberg table.

    Product CREATE registers the name in the IcebergCatalogProvider snapshot; Catalog-API
    drop without reregister leaves a phantom DF name. listTables must succeed, omit the
    victim, keep sibling tables, and not raise TableNotFound via information_schema.
    """
    spark, _warehouse = spark_wh
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark.sql("CREATE TABLE glue_catalog.ns1.keep AS SELECT 1 AS id")
    spark.sql("CREATE TABLE glue_catalog.ns1.victim AS SELECT 2 AS id")
    before = {table.name for table in spark.catalog.listTables("ns1")}
    assert {"entity", "keep", "victim"}.issubset(before)

    spark._testing_oob_drop_table("glue_catalog", "ns1", "victim")

    # Live list is authoritative and must not crash.
    live = set(spark.list_iceberg_table_names("glue_catalog", "ns1"))
    assert "victim" not in live
    assert "keep" in live
    assert "entity" in live

    tables = spark.catalog.listTables("ns1")
    names = {table.name for table in tables}
    assert "victim" not in names
    assert "keep" in names
    assert "entity" in names
    # No-arg listTables must also survive the same phantom (global walk residual was the bug).
    no_arg = {table.name for table in spark.catalog.listTables()}
    assert "victim" not in no_arg


def test_list_tables_temps_still_appended_with_live_iceberg(
    spark_wh: tuple[ReparkSession, str],
) -> None:
    """Live Iceberg list must still append session temp views (Spark parity)."""
    spark, _warehouse = spark_wh
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark.sql("SELECT 1 AS n").createOrReplaceTempView("tv_staleness")
    tables = spark.catalog.listTables("ns1")
    by_name = {table.name: table for table in tables}
    assert "entity" in by_name
    assert by_name["entity"].isTemporary is False
    assert "tv_staleness" in by_name
    assert by_name["tv_staleness"].isTemporary is True


def test_list_tables_temps_survive_oob_drop_df_known(
    spark_wh: tuple[ReparkSession, str],
) -> None:
    """F-T6-TEMP-A: temps still list when Iceberg side is cleaned via OOB drop of DF-known."""
    spark, _warehouse = spark_wh
    spark.catalog.setCurrentCatalog("glue_catalog")
    spark.sql("CREATE TABLE glue_catalog.ns1.victim AS SELECT 1 AS id")
    spark.sql("SELECT 1 AS n").createOrReplaceTempView("tv_after_oob")
    spark._testing_oob_drop_table("glue_catalog", "ns1", "victim")

    tables = spark.catalog.listTables("ns1")
    by_name = {table.name: table for table in tables}
    assert "victim" not in by_name
    assert "entity" in by_name
    assert by_name["entity"].isTemporary is False
    assert "tv_after_oob" in by_name
    assert by_name["tv_after_oob"].isTemporary is True


def test_refresh_catalog_provider_round_trip(spark_wh: tuple[ReparkSession, str]) -> None:
    """Explicit refresh rebuilds the DF provider (SQL residual escape hatch)."""
    spark, warehouse = spark_wh
    spark._testing_oob_create_table("glue_catalog", "ns1", "for_refresh", warehouse)
    # listTables already live; refresh is for free SQL / information_schema consumers.
    spark.refresh_catalog_provider("glue_catalog")
    names = spark.list_iceberg_table_names("glue_catalog", "ns1")
    assert "for_refresh" in names
