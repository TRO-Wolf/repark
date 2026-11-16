import json
import sys
import tempfile
from pathlib import Path

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix="critid-", dir=wh_parent)
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

    b = Builder.builder.appName("ddu-critic-id")
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
prov["warehouse"] = Path(wh).name
prov["ansi"] = ansi
records = [{"key": "provenance", **prov}]


def shown(value: object) -> str:
    return "NULL" if value is None else str(value)


def run_dml(key: str, ddl: str, fill: str, stmt: str) -> None:
    t = f"lc.ns.{key.replace('/', '_')}"
    try:
        s.sql(ddl.format(t=t)).collect()
        s.sql(fill.format(t=t)).collect()
        s.sql(stmt.format(t=t)).collect()
        rows = s.sql(f"SELECT * FROM {t} ORDER BY id").collect()
        records.append({"key": key, "rows": [[shown(v) for v in r] for r in rows]})
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:160]}"})


DDL = "CREATE TABLE {t} (id BIGINT, v DOUBLE) USING iceberg"
FILL = "INSERT INTO {t} VALUES (1, CAST(0.1 AS DOUBLE)), (2, CAST('1e-20' AS DOUBLE))"
run_dml("del/id_in_select", DDL, FILL, "DELETE FROM {t} WHERE id IN (SELECT 1)")
run_dml("del/id_eq_scalar", DDL, FILL, "DELETE FROM {t} WHERE id = (SELECT 1)")
run_dml("upd/id_in_select", DDL, FILL, "UPDATE {t} SET v = 9.0 WHERE id IN (SELECT 2)")

with Path(out).open("w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")
print("wrote", out, len(records))
