import os
import sys
import time

import cells
from common import error_record, run_frame, write_jsonl

engine, ansi, out = sys.argv[1], sys.argv[2], sys.argv[3]
records = []
if engine == "spark":
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    import pyspark

    records.append(
        {"key": "provenance", "engine": "spark", "version": pyspark.__version__, "ansi": ansi}
    )
    run_sql = s.sql
elif engine == "repark":
    import repark
    import repark._native as native
    from repark import ReparkSession

    s = (
        ReparkSession.builder.appName("dd")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    records.append(
        {
            "key": "provenance",
            "engine": "repark",
            "native": native.__file__,
            "ansi": ansi,
            "conf_ansi": s.conf.get("spark.sql.ansi.enabled"),
        }
    )
    run_sql = s.sql
else:
    import repark
    import repark._native as native

    records.append(
        {"key": "provenance", "engine": "native", "native": native.__file__, "ansi": "n/a"}
    )
    run_sql = repark.sql
    s = None
if s is not None:
    s.createDataFrame(cells.src_rows(), cells.SRC_SCHEMA).createOrReplaceTempView("src")
    for stmt in cells.SETUP:
        s.sql(stmt).collect()
else:
    run_sql(
        "CREATE VIEW src AS SELECT 1 AS id, CAST(1.25 AS DECIMAL(10,2)) AS dc, "
        "CAST(1e-20 AS DOUBLE) AS dd, CAST(1e30 AS DOUBLE) AS dbig, CAST(1.5 AS FLOAT) AS ff, "
        "7 AS ii, CAST(8 AS BIGINT) AS bb, '3' AS ss"
    ).collect()
    run_sql("CREATE VIEW vu AS SELECT 0.5 AS c UNION ALL SELECT CAST(1e-20 AS DOUBLE)").collect()
only = os.environ.get("CELL_PREFIX")
start = time.time()
for key, sql in cells.select_cells():
    if only and not key.startswith(only):
        continue
    rec = {"key": key, "sql": sql}
    try:
        rec.update(run_frame(run_sql(sql)))
    except Exception as e:
        rec.update(error_record(e))
    records.append(rec)
records.append({"key": "elapsed", "seconds": round(time.time() - start, 1)})
write_jsonl(out, records)
print(engine, ansi, len(records), round(time.time() - start, 1))
