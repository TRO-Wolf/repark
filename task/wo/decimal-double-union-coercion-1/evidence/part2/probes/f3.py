import json
import sys
import tempfile
from pathlib import Path
from typing import Any

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix="critf3-", dir=wh_parent)
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

    b = Builder.builder.appName("ddu-critic-f3")
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


def run(key: str, build: Any) -> None:
    try:
        rows = build().collect()
        records.append({"key": key, "rows": [[shown(v) for v in r] for r in rows]})
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:160]}"})


def make_table(name: str) -> None:
    s.sql(f"CREATE TABLE {name} (id BIGINT, d DECIMAL(10,2), v DOUBLE) USING iceberg").collect()
    s.sql(
        f"INSERT INTO {name} VALUES (1, 0.10, CAST(0.1 AS DOUBLE)), "
        f"(2, 0.00, CAST('1e-20' AS DOUBLE))"
    ).collect()


VALS = "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))"
DML = {
    "del_in_sub": "DELETE FROM {t} WHERE v IN (SELECT c FROM VALUES " + VALS + " AS x(c))",
    "del_exists": "DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM VALUES "
    + VALS
    + " AS x(c) WHERE x.c = {t}.v)",
    "del_scalar": "DELETE FROM {t} WHERE v = (SELECT max(c) FROM VALUES " + VALS + " AS x(c))",
    "upd_in_sub": "UPDATE {t} SET d = 9.99 WHERE v IN (SELECT c FROM VALUES " + VALS + " AS x(c))",
    "upd_exists": "UPDATE {t} SET d = 9.99 WHERE EXISTS (SELECT 1 FROM VALUES "
    + VALS
    + " AS x(c) WHERE x.c = v)",
}
for seq, (key, template) in enumerate(DML.items(), start=1):
    t = f"lc.ns.f3_{seq}"
    make_table(t)
    try:
        s.sql(template.format(t=t)).collect()
        records.append(
            {
                "key": key,
                "rows": [
                    [shown(v) for v in r]
                    for r in s.sql(f"SELECT id, d, v FROM {t} ORDER BY id").collect()
                ],
            }
        )
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:160]}"})

s.sql(
    "SELECT * FROM VALUES "
    "(1, CAST(0.10 AS DECIMAL(10,2))), (2, CAST(0.20 AS DECIMAL(10,2))), "
    "(3, CAST(0.30 AS DECIMAL(10,2))) AS x(id, d10)"
).createOrReplaceTempView("ti10")
run(
    "plan/in3",
    lambda: s.sql(
        "EXPLAIN SELECT id FROM ti10 WHERE d10 IN (CAST(0.5 AS DOUBLE), CAST(1.25 AS DOUBLE), "
        "CAST(4.5 AS DOUBLE))"
    ),
)
run(
    "plan/in4",
    lambda: s.sql(
        "EXPLAIN SELECT id FROM ti10 WHERE d10 IN (CAST(0.5 AS DOUBLE), CAST(1.25 AS DOUBLE), "
        "CAST(4.5 AS DOUBLE), CAST(2.75 AS DOUBLE))"
    ),
)
run(
    "in/spot_null4",
    lambda: s.sql(
        "SELECT id, d10 IN (CAST(0.5 AS DOUBLE), CAST(NULL AS DOUBLE), CAST(1.25 AS DOUBLE), "
        "CAST(4.5 AS DOUBLE)) AS p FROM ti10 ORDER BY id"
    ),
)
run(
    "in/spot_nan4",
    lambda: s.sql(
        "SELECT id, d10 IN (CAST(0.5 AS DOUBLE), CAST('NaN' AS DOUBLE), CAST(1.25 AS DOUBLE), "
        "CAST(4.5 AS DOUBLE)) AS p FROM ti10 ORDER BY id"
    ),
)
run(
    "in/spot_notin4",
    lambda: s.sql(
        "SELECT id, d10 NOT IN (CAST(0.5 AS DOUBLE), CAST(1.25 AS DOUBLE), CAST(4.5 AS DOUBLE), "
        "CAST(2.75 AS DOUBLE)) AS p FROM ti10 ORDER BY id"
    ),
)
run(
    "in/spot_mixed4",
    lambda: s.sql(
        "SELECT id, d10 IN (CAST(0.5 AS DOUBLE), 1.25, 2, CAST(4.5 AS DOUBLE)) AS p FROM ti10 "
        "ORDER BY id"
    ),
)

with Path(out).open("w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")
print("wrote", out, len(records))
