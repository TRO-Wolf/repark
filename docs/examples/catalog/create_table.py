"""createTable / createExternalTable build empty Iceberg tables and answer DataFrames.

pins: catalog-surface-1/C-005
"""

from __future__ import annotations

import warnings
from pathlib import Path

from repark.spark import ReparkSession
from repark.spark.types import IntegerType, StringType, StructField, StructType

COVERS: list[str] = [
    "Catalog.createTable",
    "Catalog.create_table",
    "Catalog.createExternalTable",
    "Catalog.create_external_table",
]


def main() -> None:
    """Create empty schema'd Iceberg tables through both spellings and the deprecated alias."""
    warehouse = Path.cwd() / "ex_cat_create_wh"
    warehouse.mkdir(parents=True, exist_ok=True)
    repark = ReparkSession.builder.appName("ex-cat-create").master("local[1]").getOrCreate()
    try:
        catalog = repark.catalog
        repark.register_memory_catalog("ex_cat", str(warehouse))
        repark.create_namespace("ex_cat", "ex_db")
        catalog.setCurrentCatalog("ex_cat")
        catalog.setCurrentDatabase("ex_db")

        schema = StructType([StructField("a", IntegerType()), StructField("p", StringType())])
        frame = catalog.createTable("ct1", schema=schema, source="iceberg", description="d")
        if frame.columns != ["a", "p"] or frame.collect() != []:
            raise SystemExit(f"Catalog.createTable frame {frame.schema.simpleString()!r}")
        if repark.table("ct1").collect() != []:
            raise SystemExit("Catalog.createTable did not leave an empty readable table")

        snake_frame = catalog.create_table(
            "ct2", schema=StructType([StructField("a", IntegerType())])
        )
        if snake_frame.columns != ["a"]:
            raise SystemExit(f"Catalog.create_table frame {snake_frame.columns!r}")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ext_frame = catalog.createExternalTable(
                "cet1", schema=StructType([StructField("a", IntegerType())])
            )
        if ext_frame.columns != ["a"]:
            raise SystemExit(f"Catalog.createExternalTable frame {ext_frame.columns!r}")
        if not any(item.category is FutureWarning for item in caught):
            raise SystemExit("Catalog.createExternalTable did not warn FutureWarning")

        with warnings.catch_warnings(record=True) as snake_caught:
            warnings.simplefilter("always")
            catalog.create_external_table(
                "cet2", schema=StructType([StructField("a", IntegerType())])
            )
        if not any(item.category is FutureWarning for item in snake_caught):
            raise SystemExit("Catalog.create_external_table did not warn FutureWarning")
    finally:
        repark.stop()


if __name__ == "__main__":
    main()
