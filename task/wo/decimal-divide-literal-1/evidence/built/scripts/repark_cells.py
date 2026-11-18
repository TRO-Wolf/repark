import importlib
import json
import sys
from pathlib import Path

import repark._native as native
from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

cells_dir, root, out = sys.argv[1:4]
sys.path.insert(0, cells_dir)
cells = importlib.import_module("_decimal_divide_literal_1_cells")
print("PKG", native.__file__)


def session(mode: str, wh: Path, extra: dict) -> ReparkSession:
    _reset_active_session_for_tests()
    b = (
        ReparkSession.builder.appName("ddl-delta2-cells")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(wh / "wh"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", mode)
    )
    for k, v in extra.items():
        b = b.config(k, v)
    return b.getOrCreate()


result = {"pkg": native.__file__, "pmod": {}, "runtime_set": {"default": {}, "false": {}}}
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
Path(out).write_text(json.dumps(result, sort_keys=True))
