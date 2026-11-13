import json
import sys
from pathlib import Path

from common import error_record, run_frame, write_jsonl
from pyspark.sql import SparkSession

wh, src, out, jar = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

s = (
    SparkSession.builder.master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.jars", jar)
    .config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.rd", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rd.type", "hadoop")
    .config("spark.sql.catalog.rd.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
records = [
    {
        "key": "provenance",
        "engine": "spark-reader",
        "warehouse": Path(wh).name,
        "written_by": Path(src).name,
    }
]
for line in Path(src).read_text().splitlines():
    rec = json.loads(line)
    if rec["key"] == "provenance":
        continue
    t = rec["table"].replace("lc.", "rd.", 1)
    got = {"key": rec["key"], "table": rec["table"]}
    try:
        got["spark_read"] = run_frame(s.sql(f"SELECT id, v FROM {t} ORDER BY id"))
        got["rows"] = [
            [r[0], str(r[1])] for r in s.sql(f"SELECT id, v FROM {t} ORDER BY id").collect()
        ]
    except Exception as e:
        got["spark_read"] = error_record(e)
    records.append(got)
write_jsonl(out, records)
print("read", len(records))
