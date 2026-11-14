import importlib.util
import json
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as pyspark_functions

test_file, ansi, jar, out, wh_parent = sys.argv[1:6]
spec = importlib.util.spec_from_file_location("pins", test_file)
pins = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pins)
pins.sf = pyspark_functions
wh = tempfile.mkdtemp(prefix=f"pins-spark-{ansi}-", dir=wh_parent)
s = (
    SparkSession.builder.master("local[1]")
    .config("spark.ui.enabled", "false")
    .config("spark.jars", jar)
    .config(
        "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ice.type", "hadoop")
    .config("spark.sql.catalog.ice.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
s.sql("CREATE NAMESPACE ice.ns")
records = []
for door, run in sorted(pins.DOORS.items()):
    for (source, precision, scale), values in pins.GROUPS.items():
        cell = pins.Cell(s, source, precision, scale)
        try:
            got = run(cell)
            error = None
        except Exception as e:
            got, error = {}, str(e).strip().splitlines()[0][:200]
        for i, (text, want) in enumerate(values):
            answer = got.get(i)
            equal = answer is not None and Decimal(answer) == Decimal(want)
            records.append(
                {
                    "ansi": ansi,
                    "door": door,
                    "source": source,
                    "target": f"({precision},{scale})",
                    "value": text,
                    "spark": answer,
                    "pinned": want,
                    "equal": equal,
                    "error": error,
                }
            )
with Path(out).open("w") as fh:
    for record in records:
        fh.write(json.dumps(record, sort_keys=True) + "\n")
print(ansi, len(records), sum(not r["equal"] for r in records))
