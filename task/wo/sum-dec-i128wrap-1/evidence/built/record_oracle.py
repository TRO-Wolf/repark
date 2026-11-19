import importlib
import json
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sf

tests, ansi, root, jar, out = sys.argv[1:6]
sys.path.insert(0, tests)
cells = importlib.import_module("_sum_dec_i128wrap_1_cells")

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.jars", jar)
    .config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.files.openCostInBytes", "134217728")
    .config("spark.sql.warehouse.dir", f"{root}/spark-warehouse")
    .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", f"{root}/wh")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
cells.make_tables(spark, root)
found = cells.measure_all(spark, sf, Window, "lc", "spark")
found["conf/ansi"] = spark.conf.get("spark.sql.ansi.enabled")
repeats = cells.measure_repeats(spark, cells.REPEATS)
splits = spark.table("t4").rdd.getNumPartitions()
spark.stop()
record = {
    "ansi": ansi,
    "cells": found,
    "repeats": repeats,
    "t4_partitions": splits,
    "provenance": {"engine": "spark", "version": pyspark.__version__, "jar": Path(jar).name},
}
Path(out).write_text(json.dumps(record, sort_keys=True, indent=1) + "\n")
