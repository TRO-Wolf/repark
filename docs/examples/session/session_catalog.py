"""The catalog and conf property surfaces on a fresh default session.

pins: ex-21-catalog-session/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "SparkSession.catalog",
]


def main() -> None:
    """Run the measured catalog-property answers: the Catalog type and the default names."""
    repark = ReparkSession.builder.appName("ex21-ses-catalog").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        kind = type(catalog).__name__
        kind_expected = "Catalog"
        if kind != kind_expected:
            raise SystemExit(f"spark.catalog type {kind!r} != {kind_expected!r}")

        current = catalog.currentCatalog()
        current_expected = "spark_catalog"
        if current != current_expected:
            raise SystemExit(f"currentCatalog {current!r} != {current_expected!r}")

        database = catalog.currentDatabase()
        database_expected = "default"
        if database != database_expected:
            raise SystemExit(f"currentDatabase {database!r} != {database_expected!r}")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
