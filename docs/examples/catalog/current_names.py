"""Current catalog and database names on the default session.

pins: ex-20-window-catalog/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.currentCatalog",
    "Catalog.current_catalog",
    "Catalog.currentDatabase",
    "Catalog.current_database",
]


def main() -> None:
    """Run the measured current-catalog answers: the default catalog and database names."""
    repark = ReparkSession.builder.appName("ex-cat-current").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        current = catalog.currentCatalog()
        current_expected = "spark_catalog"
        if current != current_expected:
            raise SystemExit(f"Catalog.currentCatalog {current!r} != {current_expected!r}")

        snake_current = catalog.current_catalog()
        if snake_current != current_expected:
            raise SystemExit(f"Catalog.current_catalog {snake_current!r} != {current_expected!r}")

        database = catalog.currentDatabase()
        database_expected = "default"
        if database != database_expected:
            raise SystemExit(f"Catalog.currentDatabase {database!r} != {database_expected!r}")

        snake_database = catalog.current_database()
        if snake_database != database_expected:
            raise SystemExit(
                f"Catalog.current_database {snake_database!r} != {database_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
