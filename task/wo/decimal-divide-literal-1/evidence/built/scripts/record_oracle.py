import importlib
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

cells_dir, jar, root, out = sys.argv[1:5]
sys.path.insert(0, cells_dir)
cells = importlib.import_module("_decimal_divide_literal_1_cells")

EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def session(mode: str, wh: Path, extra: dict) -> SparkSession:
    b = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config("spark.sql.extensions", EXT)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", str(wh / "spark-warehouse"))
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(wh / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", mode)
    )
    for k, v in extra.items():
        b = b.config(k, v)
    return b.getOrCreate()


result = {"cells": {}, "pick_false": {}, "pmod": {}, "runtime_set": {"default": {}, "false": {}}}
for mode in ("false", "true"):
    wh = Path(root) / f"pmod-{mode}"
    s = session(mode, wh, {})
    cells.make_pmod_table(s, str(wh))
    result["pmod"][mode] = cells.measure_pmod(s, sf)
    s.stop()
    for built, extra in (("default", {}), ("false", {cells.PICK_SET_KEY: "false"})):
        wh = Path(root) / f"set-{built}-{mode}"
        s = session(mode, wh, extra)
        cells.make_tables(s, str(wh))
        result["runtime_set"][built][mode] = cells.measure_runtime_set(s, built == "false")
        s.stop()
    wh = Path(root) / f"main-{mode}"
    s = session(mode, wh, {})
    cells.make_tables(s, str(wh))
    result["cells"][mode] = cells.measure_all(s, sf, "lc")
    s.stop()
    wh = Path(root) / f"pmp-{mode}"
    s = session(mode, wh, {"spark.sql.legacy.literal.pickMinimumPrecision": "false"})
    cells.make_tables(s, str(wh))
    result["pick_false"][mode] = cells.measure_pick_false(s, sf)
    s.stop()
Path(out).write_text(json.dumps(result, sort_keys=True))
