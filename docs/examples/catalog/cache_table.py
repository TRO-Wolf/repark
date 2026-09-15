"""cacheTable / isCached / uncacheTable on a temp view and an Iceberg table.

pins: catalog-surface-1/C-004
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.cacheTable",
    "Catalog.cache_table",
    "Catalog.isCached",
    "Catalog.is_cached",
    "Catalog.uncacheTable",
    "Catalog.uncache_table",
]


def main() -> None:
    """Cache a table, observe it through both spellings, then release it."""
    warehouse = Path.cwd() / "ex_cat_cache_wh"
    warehouse.mkdir(parents=True, exist_ok=True)
    repark = ReparkSession.builder.appName("ex-cat-cache").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.register_memory_catalog("ex_cat", str(warehouse))
        repark.create_namespace("ex_cat", "ex_db")
        repark.sql("CREATE TABLE ex_cat.ex_db.t1 AS SELECT 1 AS a, 'x' AS p")
        catalog.setCurrentCatalog("ex_cat")
        catalog.setCurrentDatabase("ex_db")
        repark.createDataFrame([(1,)], ["v"]).createOrReplaceTempView("tv")

        if catalog.isCached("tv") is not False:
            raise SystemExit("Catalog.isCached before cacheTable is not False")
        if catalog.cacheTable("tv") is not None:
            raise SystemExit("Catalog.cacheTable did not return None")
        if catalog.isCached("tv") is not True:
            raise SystemExit("Catalog.isCached after cacheTable is not True")
        if catalog.uncacheTable("tv") is not None:
            raise SystemExit("Catalog.uncacheTable did not return None")
        if catalog.isCached("tv") is not False:
            raise SystemExit("Catalog.isCached after uncacheTable is not False")

        catalog.cache_table("t1")
        if catalog.is_cached("t1") is not True:
            raise SystemExit("Catalog.is_cached after cache_table is not True")
        catalog.uncache_table("t1")
        if catalog.is_cached("t1") is not False:
            raise SystemExit("Catalog.is_cached after uncache_table is not False")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
