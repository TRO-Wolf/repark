from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import _temp_view_session_settings_1_cells as cells

EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def session(engine: str, root: str, mode: str, jar: str) -> object:
    if engine == "spark":
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
            .config("spark.sql.extensions", EXT)
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", root + "/spark-warehouse")
        )
    else:
        import repark._native as native
        from repark import ReparkSession
        from repark.spark.session import _reset_active_session_for_tests

        print("NATIVE " + native.__file__, flush=True)
        _reset_active_session_for_tests()
        builder = ReparkSession.builder
    builder = (
        builder.appName("record-temp-view-session-settings-1")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", root + "/lc")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if mode != "unset":
        builder = builder.config("spark.sql.ansi.enabled", mode)
    return builder.getOrCreate()


def main() -> None:
    engine, parent, jar, out, what = sys.argv[1:6]
    found: dict[str, dict] = {}
    for mode in ("true", "false", "unset") if what == "cells" else ("true",):
        root = tempfile.mkdtemp(prefix="tvs-" + mode + "-", dir=parent)
        s = session(engine, root, mode, jar)
        table = cells.Cells(s, root, mode)
        found[mode] = table.run() if what == "cells" else table.explain()
        s.stop()
    Path(out).write_text(json.dumps(found, sort_keys=True, separators=(",", ":")) + "\n")
    print("WROTE " + out, flush=True)


if __name__ == "__main__":
    main()
