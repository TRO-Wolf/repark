"""LOCAL-DROP-RECREATE-STALE-WRITE-1 Spark leg: Spark 4.1.2 reads RePark's tables from disk.

Reads the ``cells_repark.py`` JSON lines given by ``--cells`` and, for every wrong-write row,
loads the table directory through Spark's Iceberg path reader. Writes one JSON line per row
with Spark's rows beside RePark's session rows and the truth. Takes the Iceberg runtime jar as
``--jar`` so no machine path is committed. Run with the oracle interpreter under the
single-JVM lock.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def main() -> int:
    """Read every wrong-write row by path and print the comparison lines."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cells", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("local-drop-recreate-spark-read")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", str(args.jar))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        for line in args.cells.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            if record["kind"] != "wrong-write":
                continue
            frame = spark.read.format("iceberg").load(record["table_dir"])
            rows = sorted([int(r["id"]), int(r["n"])] for r in frame.select("id", "n").collect())
            result = {
                "sequence": record["sequence"],
                "cache": record["cache"],
                "spark_reads_disk": rows,
                "repark_session": record["session_rows"],
                "truth": record["truth"],
                "spark_equals_truth": rows == record["truth"],
            }
            sys.stdout.write(json.dumps(result) + "\n")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
