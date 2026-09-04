"""Catalog listings: the temp-view ``listTables`` row and the ``listCatalogs`` row.

pins: ex-20-window-catalog/C-001
"""

from __future__ import annotations

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.listTables",
    "Catalog.listCatalogs",
    "Catalog.list_catalogs",
]


def main() -> None:
    """Run the measured listing answers: one temp-view row, one pattern row, one catalog row."""
    repark = ReparkSession.builder.appName("ex-cat-list").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.createDataFrame([(1, "x")], ["k", "s"]).createTempView("ex20_tv")
        tables = [tuple(row) for row in catalog.listTables()]
        tables_expected = [("ex20_tv", None, [], None, "TEMPORARY", True)]
        if tables != tables_expected:
            raise SystemExit(f"Catalog.listTables rows {tables!r} != {tables_expected!r}")

        pattern_tables = [tuple(row) for row in catalog.listTables(pattern="ex20*")]
        if pattern_tables != tables_expected:
            raise SystemExit(
                f"Catalog.listTables pattern {pattern_tables!r} != {tables_expected!r}"
            )

        catalogs = [tuple(row) for row in catalog.listCatalogs()]
        catalogs_expected = [("spark_catalog", None)]
        if catalogs != catalogs_expected:
            raise SystemExit(f"Catalog.listCatalogs rows {catalogs!r} != {catalogs_expected!r}")

        snake_catalogs = [tuple(row) for row in catalog.list_catalogs()]
        if snake_catalogs != catalogs_expected:
            raise SystemExit(
                f"Catalog.list_catalogs rows {snake_catalogs!r} != {catalogs_expected!r}"
            )
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
