import importlib
import json
import sys
import tempfile
from pathlib import Path

engine, ansi, cells_dir, wh_parent, jar, out = sys.argv[1:7]
sys.path.insert(0, cells_dir)
cells = importlib.import_module("_decimal_common_type_1_cells")

wh = tempfile.mkdtemp(prefix=f"orc-{engine}-{ansi}-", dir=wh_parent)
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

    builder = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.shuffle.partitions", "1")
    )
    version = pyspark.__version__
else:
    import repark._native as native
    from repark import ReparkSession as Builder
    from repark import functions as sf

    print("native", native.__file__, flush=True)
    builder = Builder.builder.appName("dct-oracle")
    version = "repark:" + native.__file__.split("/scratchpad/", 1)[-1]
s = (
    builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
cells.make_tables(s, wh)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
found = cells.measure_all(s, sf, "lc")
Path(out).write_text(
    json.dumps({"engine": engine, "version": version, "ansi": ansi, "cells": found}, sort_keys=True)
)
print(engine, ansi, len(found))
