"""Set the current catalog and database on the session and read both back.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.setCurrentCatalog",
    "Catalog.set_current_catalog",
    "Catalog.setCurrentDatabase",
    "Catalog.set_current_database",
]


def main() -> None:
    """Run the measured set-and-readback answers for the current catalog and database."""
    warehouse = Path.cwd() / "ex21_wh_set"
    warehouse.mkdir(parents=True, exist_ok=True)
    repark = ReparkSession.builder.appName("ex21-cat-set").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.register_memory_catalog("ex21_cat", str(warehouse))
        flipped = catalog.currentCatalog()
        flipped_expected = "ex21_cat"
        if flipped != flipped_expected:
            raise SystemExit(
                f"Catalog.currentCatalog after register {flipped!r} != {flipped_expected!r}"
            )

        repark.create_namespace("ex21_cat", "ex21_db")

        cleared = catalog.setCurrentCatalog("spark_catalog")
        cleared_expected = None
        if cleared != cleared_expected:
            raise SystemExit(
                f"Catalog.setCurrentCatalog return {cleared!r} != {cleared_expected!r}"
            )
        current = catalog.currentCatalog()
        current_expected = "spark_catalog"
        if current != current_expected:
            raise SystemExit(f"Catalog.currentCatalog {current!r} != {current_expected!r}")

        catalog.set_current_catalog("ex21_cat")
        snake_current = catalog.currentCatalog()
        if snake_current != flipped_expected:
            raise SystemExit(
                f"Catalog.currentCatalog after set_current_catalog {snake_current!r}"
                f" != {flipped_expected!r}"
            )

        catalog.setCurrentDatabase("ex21_db")
        database = catalog.currentDatabase()
        database_expected = "ex21_db"
        if database != database_expected:
            raise SystemExit(f"Catalog.currentDatabase {database!r} != {database_expected!r}")

        catalog.setCurrentCatalog("spark_catalog")
        catalog.set_current_database("default")
        snake_database = catalog.currentDatabase()
        snake_database_expected = "default"
        if snake_database != snake_database_expected:
            raise SystemExit(
                f"Catalog.currentDatabase after set_current_database {snake_database!r}"
                f" != {snake_database_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
