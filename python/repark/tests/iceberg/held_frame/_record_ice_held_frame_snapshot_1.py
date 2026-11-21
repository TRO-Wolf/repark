from __future__ import annotations

import json
import sys
from pathlib import Path

from _ice_held_frame_snapshot_1_cells import Cells
from pyspark.sql import SparkSession

EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
FIXTURE = Path(__file__).with_name("ice_held_frame_snapshot_1_spark_oracle.json")


def main(jar: str, warehouse: str) -> None:
    found = {}
    for ansi in ("true", "false"):
        session = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
            .config("spark.sql.extensions", EXTENSIONS)
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lc.type", "hadoop")
            .config("spark.sql.catalog.lc.warehouse", warehouse + "/" + ansi)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", ansi)
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        session.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
        found[ansi] = Cells(session).run()
        session.stop()
    FIXTURE.write_text(json.dumps(found, indent=1, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
