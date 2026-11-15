import json
import sys
import tempfile
from pathlib import Path

from common import error_record, show, write_jsonl

import repark._native as native_module
from repark import ReparkSession

wh_parent, values_path, out = sys.argv[1:4]
V = json.loads(Path(values_path).read_text())
wh = tempfile.mkdtemp(prefix="rxk-", dir=wh_parent)
s = (
    ReparkSession.builder.appName("rxk")
    .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "true")
    .getOrCreate()
)
records = [
    {
        "key": "provenance",
        "native": "<scratchpad>/" + native_module.__file__.split("/scratchpad/", 1)[-1],
    }
]
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns")
for src, key in (("DOUBLE", "double"), ("FLOAT", "float")):
    t = f"lc.ns.src_{src[0].lower()}"
    s.sql(f"CREATE TABLE {t} (id INT, v {src}) USING iceberg").collect()
    vals = V[key]
    for i in range(0, len(vals), 100):
        conv = (
            (lambda x: f"CAST(CAST('{x}' AS DOUBLE) AS FLOAT)")
            if src == "FLOAT"
            else (lambda x: f"CAST('{x}' AS DOUBLE)")
        )
        rows = ", ".join(f"({r['id']}, {conv(r['text'])})" for r in vals[i : i + 100])
        s.sql(f"INSERT INTO {t} VALUES {rows}").collect()
    for sc in range(0, 19):
        k = f"lc.ns.k_{src[0].lower()}_{sc}"
        rec = {
            "key": f"kcheck/{src}/{sc}",
            "src": src,
            "scale": sc,
            "filter": f"abs(v) < 1e{37 - sc}",
        }
        try:
            s.sql(f"CREATE TABLE {k} (id INT, d DECIMAL(38,{sc})) USING iceberg").collect()
            s.sql(
                f"INSERT INTO {k} SELECT id, v FROM {t} WHERE NOT isnan(v) AND abs(v) < 1e{37 - sc}"
            ).collect()
            rec["rows"] = {str(r[0]): show(r[1]) for r in s.sql(f"SELECT id, d FROM {k}").collect()}
        except Exception as e:
            rec.update(error_record(e))
        records.append(rec)
write_jsonl(out, records)
print(len(records))
