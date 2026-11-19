import json
import sys
from pathlib import Path

import r1_cells
from pyspark.sql import SparkSession


def main() -> None:
    out_path, master, ansi, codegen = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
    runs, step0, mine, jar = int(sys.argv[5]), Path(sys.argv[6]), Path(sys.argv[7]), sys.argv[8]
    set_name = sys.argv[9] if len(sys.argv) > 9 else "spark"
    cores = master.split("[")[1].rstrip("]")
    spark = (
        SparkSession.builder.master(master)
        .appName("avg-r1-spark")
        .config("spark.jars", jar)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.shuffle.partitions", cores)
        .config("spark.sql.codegen.wholeStage", codegen)
        .getOrCreate()
    )
    refused = r1_cells.register(spark, step0, mine)
    base = {"engine": "spark", "master": master, "ansi": ansi, "codegen": codegen}
    with out_path.open("a") as out:
        for name, why in refused.items():
            row = base | {"query": f"view/{name}", "answers": {json.dumps(why): 1}}
            out.write(json.dumps(row) + "\n")
        for key, sql in r1_cells.SETS[set_name]().items():
            answers = r1_cells.count(lambda sql=sql: spark.sql(sql).collect(), runs)
            out.write(json.dumps(base | {"query": key, "answers": answers}) + "\n")
            out.flush()
    spark.stop()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
