"""Register the local memory Iceberg catalog and create a namespace in it.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.register_memory_catalog",
    "SparkSession.create_namespace",
]


def main() -> None:
    """Run the measured registration answers: the catalog lists, becomes current, hosts a namespace."""
    warehouse = Path.cwd() / "ex21_wh_reg"
    warehouse.mkdir(parents=True, exist_ok=True)
    repark = ReparkSession.builder.appName("ex21-ses-register").master("local[1]").getOrCreate()
    try:
        repark.register_memory_catalog("ex21_cat", str(warehouse))
        catalog = repark.catalog

        catalogs = [tuple(row) for row in catalog.listCatalogs()]
        catalogs_expected = [("ex21_cat", None), ("spark_catalog", None)]
        if catalogs != catalogs_expected:
            raise SystemExit(f"Catalog.listCatalogs rows {catalogs!r} != {catalogs_expected!r}")

        current = catalog.currentCatalog()
        current_expected = "ex21_cat"
        if current != current_expected:
            raise SystemExit(f"currentCatalog after register {current!r} != {current_expected!r}")

        repark.create_namespace("ex21_cat", "ex21_db")
        exists = catalog.databaseExists("ex21_db")
        exists_expected = True
        if exists != exists_expected:
            raise SystemExit(f"databaseExists after create_namespace {exists!r} != {exists_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
