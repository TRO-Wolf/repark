"""A13: location-less CTAS under ``register_memory_catalog`` uses the warehouse.

Two independent sessions that pass different warehouses must not share a directory
keyed by catalog/namespace/table name.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from repark import ReparkSession


def _fallback_table_dir(root: Path, catalog: str, namespace: str, table: str) -> Path:
    return root / "repark_ctas" / catalog / namespace / table


def test_location_less_ctas_writes_under_the_warehouse(tmp_path: Path) -> None:
    """A namespace with no LOCATION writes Iceberg files under the catalog warehouse."""
    catalog = "a13wh"
    namespace = "ns"
    table = "events"
    spark = ReparkSession.builder.appName("a13-warehouse").getOrCreate()
    try:
        spark.register_memory_catalog(catalog, tmp_path)
        spark.sql(f"CREATE NAMESPACE {catalog}.{namespace}")
        spark.sql(f"CREATE TABLE {catalog}.{namespace}.{table} USING iceberg AS SELECT 1 AS id")
        rows = spark.sql(f"SELECT id FROM {catalog}.{namespace}.{table}").to_arrow()
        assert rows.column("id").to_pylist() == [1]
        assert _fallback_table_dir(tmp_path, catalog, namespace, table).exists()
        shared = _fallback_table_dir(Path(tempfile.gettempdir()), catalog, namespace, table)
        assert not shared.exists(), f"process-temp shared root must stay unused: {shared}"
    finally:
        spark.stop()


def test_two_warehouses_do_not_share_a_location_less_table(tmp_path: Path) -> None:
    """Same names, different warehouses: each session owns its own directory."""
    warehouse_a = tmp_path / "a"
    warehouse_b = tmp_path / "b"
    warehouse_a.mkdir()
    warehouse_b.mkdir()
    spark_a = ReparkSession.builder.appName("a13-iso-a").getOrCreate()
    spark_b = spark_a.newSession()
    try:
        spark_a.register_memory_catalog("mem", warehouse_a)
        spark_b.register_memory_catalog("mem", warehouse_b)
        spark_a.sql("CREATE NAMESPACE mem.ns")
        spark_b.sql("CREATE NAMESPACE mem.ns")
        spark_a.sql("CREATE TABLE mem.ns.events USING iceberg AS SELECT 1 AS id")
        spark_b.sql("CREATE TABLE mem.ns.events USING iceberg AS SELECT 2 AS id")
        ids_a = spark_a.sql("SELECT id FROM mem.ns.events").to_arrow().column("id").to_pylist()
        ids_b = spark_b.sql("SELECT id FROM mem.ns.events").to_arrow().column("id").to_pylist()
        assert ids_a == [1]
        assert ids_b == [2]
        assert _fallback_table_dir(warehouse_a, "mem", "ns", "events").exists()
        assert _fallback_table_dir(warehouse_b, "mem", "ns", "events").exists()
        assert not _fallback_table_dir(warehouse_a, "mem", "ns", "events").samefile(
            _fallback_table_dir(warehouse_b, "mem", "ns", "events")
        )
    finally:
        spark_a.stop()
        spark_b.stop()
