import sys
import tempfile
from pathlib import Path

from common import error_record, run_frame, write_jsonl
from write_cells import WRITES

engine, ansi, wh_parent, out, jar = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
wh = tempfile.mkdtemp(prefix=f"wh-{engine}-{ansi}-", dir=wh_parent)
if engine == "repark":
    import repark._native as native
    from repark import ReparkSession as Builder

    b = Builder.builder.appName("ddw")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
else:
    import pyspark
    from pyspark.sql import SparkSession as Builder

    b = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.shuffle.partitions", "1")
    )
    prov = {"engine": "spark", "version": pyspark.__version__, "jar": Path(jar).name}
s = (
    b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
records = [dict(key="provenance", ansi=ansi, warehouse=Path(wh).name, **prov)]
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns")
for i, (name, vtype, seed, sql) in enumerate(WRITES):
    t = f"lc.ns.w{i:02d}"
    rec = {"key": name, "table": t, "target": vtype, "sql": sql.format(t=t)}
    try:
        if vtype is not None:
            s.sql(f"CREATE TABLE {t} (id INT, v {vtype}) USING iceberg").collect()
        if seed:
            s.sql(f"INSERT INTO {t} VALUES (1, 9), (2, 9)").collect()
        if sql == "DF":
            left = s.sql("SELECT 1 AS id, 0.1 AS v")
            right = s.sql("SELECT 2 AS id, CAST(1e-20 AS DOUBLE) AS v")
            left.union(right).writeTo(t).append()
        else:
            s.sql(sql.format(t=t)).collect()
        rec["write"] = "ok"
    except Exception as e:
        rec["write"] = error_record(e)
    try:
        rec["readback"] = run_frame(s.sql(f"SELECT id, v FROM {t} ORDER BY id"))
        rec["rows"] = [
            [r[0], str(r[1])] for r in s.sql(f"SELECT id, v FROM {t} ORDER BY id").collect()
        ]
    except Exception as e:
        rec["readback"] = error_record(e)
    records.append(rec)
write_jsonl(out, records)
print(engine, ansi, wh)
