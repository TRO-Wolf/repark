import json
import sys
from collections import Counter
from pathlib import Path

import cells
from pyspark.sql import SparkSession


def main() -> None:
    root, jar, out_path, wh = sys.argv[1], sys.argv[2], Path(sys.argv[3]), sys.argv[4]
    master, runs = sys.argv[5], int(sys.argv[6])
    only = sys.argv[7] if len(sys.argv) > 7 else "all"
    cores = master[6:-1]
    with out_path.open("a") as out:
        for ansi in ("false", "true"):
            spark = (
                SparkSession.builder.master(master)
                .config("spark.ui.enabled", "false")
                .config("spark.jars", jar)
                .config("spark.sql.shuffle.partitions", cores)
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.ansi.enabled", ansi)
                .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.lc.type", "hadoop")
                .config("spark.sql.catalog.lc.warehouse", f"{wh}/{master[6:-1]}-{ansi}")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")
            cells.register(spark, root)
            for name in ("t", "big", "t4"):
                parts = spark.table(name).rdd.getNumPartitions()
                print("splits", master, ansi, name, parts, flush=True)

            def record(
                key: str, sql: str, seen: Counter[str], ansi: str = ansi, ms: int = 0
            ) -> None:
                row = {"engine": "spark", "master": master, "ansi": ansi, "query": key}
                row |= {"distinct": len(seen), "answers": dict(seen), "ms": ms}
                out.write(json.dumps(row) + "\n")
                out.flush()

            cells.measure(spark, "lc", runs, True, record, only)
            spark.stop()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
