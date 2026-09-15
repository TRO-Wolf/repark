"""dropGlobalTempView / recoverPartitions / refreshTable / refreshByPath answers.

pins: catalog-surface-1/C-006
"""

from __future__ import annotations

from pathlib import Path

from repark.spark import ReparkSession

COVERS: list[str] = [
    "Catalog.dropGlobalTempView",
    "Catalog.drop_global_temp_view",
    "Catalog.recoverPartitions",
    "Catalog.recover_partitions",
    "Catalog.refreshTable",
    "Catalog.refresh_table",
    "Catalog.refreshByPath",
    "Catalog.refresh_by_path",
]


def main() -> None:
    """Run the maintenance no-ops and the missing-view answer through both spellings."""
    warehouse = Path.cwd() / "ex_cat_maintain_wh"
    warehouse.mkdir(parents=True, exist_ok=True)
    repark = ReparkSession.builder.appName("ex-cat-maintain").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.register_memory_catalog("ex_cat", str(warehouse))
        repark.create_namespace("ex_cat", "ex_db")
        repark.sql("CREATE TABLE ex_cat.ex_db.t1 AS SELECT 1 AS a")
        catalog.setCurrentCatalog("ex_cat")
        catalog.setCurrentDatabase("ex_db")
        repark.createDataFrame([(1,)], ["v"]).createOrReplaceTempView("tv")

        if catalog.dropGlobalTempView("nope_gv") is not False:
            raise SystemExit("Catalog.dropGlobalTempView did not answer False")
        if catalog.drop_global_temp_view("nope_gv") is not False:
            raise SystemExit("Catalog.drop_global_temp_view did not answer False")

        if catalog.recoverPartitions("t1") is not None:
            raise SystemExit("Catalog.recoverPartitions did not answer None")
        if catalog.recover_partitions("t1") is not None:
            raise SystemExit("Catalog.recover_partitions did not answer None")

        if catalog.refreshTable("t1") is not None:
            raise SystemExit("Catalog.refreshTable did not answer None")
        if catalog.refresh_table("tv") is not None:
            raise SystemExit("Catalog.refresh_table on a view did not answer None")

        if catalog.refreshByPath("/nonexistent/zz") is not None:
            raise SystemExit("Catalog.refreshByPath did not answer None")
        if catalog.refresh_by_path("/nonexistent/zz") is not None:
            raise SystemExit("Catalog.refresh_by_path did not answer None")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
