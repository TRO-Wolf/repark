import json
import sys
import tempfile
from pathlib import Path
from typing import Any

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix="critfam-", dir=wh_parent)
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

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
    from repark import functions as sf

    b = Builder.builder.appName("ddu-critic-fam")
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


def frame_record(key: str, frame: Any) -> dict:
    rows = frame.collect()
    types = [f.dataType.simpleString() for f in frame.schema.fields]
    return {"key": key, "types": types, "rows": [[shown(v) for v in r] for r in rows]}


def run(key: str, build: Any) -> None:
    try:
        records.append(frame_record(key, build()))
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:200]}"})


ZROWS = {
    "tiny": "CAST('1e-20' AS DOUBLE)",
    "negtiny": "CAST('-1e-20' AS DOUBLE)",
    "exact": "CAST(0.1 AS DOUBLE)",
    "round16": "CAST('0.10000000000000002' AS DOUBLE)",
    "negzero": "CAST('-0.0' AS DOUBLE)",
    "half": "CAST(0.5 AS DOUBLE)",
    "big": "CAST('1e30' AS DOUBLE)",
}

VALS = "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))"

SQL_SHAPES = {}
for zname, z in ZROWS.items():
    SQL_SHAPES[f"sql/in/{zname}"] = (
        f"SELECT 1 AS id WHERE {z} IN (SELECT c FROM VALUES {VALS} AS t(c))"
    )
    SQL_SHAPES[f"sql/in_cte/{zname}"] = (
        f"WITH v AS (SELECT c FROM VALUES {VALS} AS t(c)) SELECT 1 AS id WHERE {z} IN (SELECT c "
        "FROM v)"
    )
    SQL_SHAPES[f"sql/exists/{zname}"] = (
        f"SELECT 1 AS id WHERE EXISTS (SELECT 1 FROM VALUES {VALS} AS t(c) WHERE t.c = {z})"
    )
    SQL_SHAPES[f"sql/eq_scalar/{zname}"] = (
        f"SELECT 1 AS id WHERE {z} = (SELECT max(c) FROM VALUES {VALS} AS t(c))"
    )
    SQL_SHAPES[f"sql/join/{zname}"] = (
        f"SELECT a.id FROM (SELECT 1 AS id, {z} AS z) a JOIN "
        f"(SELECT c FROM VALUES {VALS} AS t(c)) b ON a.z = b.c"
    )
    SQL_SHAPES[f"sql/intersect/{zname}"] = (
        f"SELECT {z} AS c INTERSECT SELECT c FROM VALUES {VALS} AS t(c)"
    )
    SQL_SHAPES[f"sql/union_branch/{zname}"] = (
        f"SELECT c FROM (SELECT CAST(0.1 AS DECIMAL(10,2)) AS c UNION ALL "
        f"SELECT CAST('1e-20' AS DOUBLE)) WHERE c = {z}"
    )

for key, sql in SQL_SHAPES.items():
    run(key, lambda q=sql: s.sql(q))

s.sql(f"SELECT c FROM VALUES {VALS} AS t(c)").createOrReplaceTempView("vv")
s.sql(
    "SELECT 1 AS id, CAST('1e-20' AS DOUBLE) AS z, CAST(0.1 AS DOUBLE) AS q, "
    "CAST('0.10000000000000002' AS DOUBLE) AS r, CAST('-0.0' AS DOUBLE) AS nz"
).createOrReplaceTempView("zz")
for zname, col in (("tiny", "z"), ("exact", "q"), ("round16", "r"), ("negzero", "nz")):
    run(
        f"sql/view_join/{zname}",
        lambda c=col: s.sql(f"SELECT a.id FROM zz a JOIN vv b ON a.{c} = b.c"),
    )
    run(
        f"sql/view_in/{zname}",
        lambda c=col: s.sql(f"SELECT id FROM zz WHERE {c} IN (SELECT c FROM vv)"),
    )

base = s.sql("SELECT 1 AS id, CAST('1e-20' AS DOUBLE) AS z")
M1_Z = f"z IN (SELECT c FROM VALUES {VALS} AS t(c))"
M1_EX = f"EXISTS (SELECT 1 FROM VALUES {VALS} AS t(c) WHERE t.c = z)"
run("frag/fexpr_filter_z_in_values", lambda: base.filter(sf.expr(M1_Z)).select("id"))
run("frag/fexpr_filter_exists_values", lambda: base.filter(sf.expr(M1_EX)).select("id"))
run("frag/filter_str_z_in_values", lambda: base.filter(M1_Z).select("id"))
run("frag/selectexpr_z_in_values", lambda: base.selectExpr(M1_Z + " AS p"))

dfv = s.sql(f"SELECT c FROM VALUES {VALS} AS t(c)")
run("df/from_values_join", lambda: base.join(dfv, base.z == dfv.c).select("id"))

with Path(out).open("w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")
print("wrote", out, len(records))
