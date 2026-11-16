import json
import sys
import tempfile
from pathlib import Path

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix="critb-", dir=wh_parent)
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder

    b = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config("spark.sql.extensions", EXT)
        .config("spark.sql.shuffle.partitions", "1")
    )
    prov = {"engine": "spark", "version": pyspark.__version__}
else:
    import repark._native as native
    from repark import ReparkSession as Builder

    b = Builder.builder.appName("ddu-critic-b")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
s = (
    b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
prov["ansi"] = ansi
records = [{"key": "provenance", **prov}]
VALS = "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))"
for z in [
    "CAST(0.0 AS DOUBLE)",
    "CAST('1e-16' AS DOUBLE)",
    "CAST('2e-16' AS DOUBLE)",
    "CAST('1e-15' AS DOUBLE)",
    "CAST('1e-14' AS DOUBLE)",
    "CAST('5e-16' AS DOUBLE)",
]:
    key = "b/" + z.replace("CAST(", "").replace(" AS DOUBLE)", "").replace("'", "")
    try:
        r = s.sql(f"SELECT 1 AS id WHERE {z} IN (SELECT c FROM VALUES {VALS} AS t(c))").collect()
        records.append({"key": key, "rows": [[str(v) for v in row] for row in r]})
    except Exception as e:
        records.append({"key": key, "error": f"{type(e).__name__}: {str(e)[:120]}"})
with Path(out).open("w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")
print("wrote", out, len(records))
