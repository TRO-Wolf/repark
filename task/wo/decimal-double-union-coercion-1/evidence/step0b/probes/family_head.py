import json
import math
import sys

engine, ansi, out = sys.argv[1:4]
if engine == "spark":
    from pyspark.sql import SparkSession as Builder

    b = Builder.builder.master("local[2]").config("spark.ui.enabled", "false")
else:
    from repark import ReparkSession as Builder

    b = Builder.builder.appName("ddu-family")
s = b.config("spark.sql.ansi.enabled", ansi).getOrCreate()
rows = []
for m in range(1, 10001):
    x = m / 100
    for k in (1, 2, 3):
        y, z = x, x
        for _ in range(k):
            y = math.nextafter(y, math.inf)
            z = math.nextafter(z, -math.inf)
        rows.append((len(rows), f"{m / 100:.2f}", repr(y)))
        rows.append((len(rows), f"{m / 100:.2f}", repr(z)))
s.createDataFrame(rows, "id INT, ds STRING, vs STRING").createOrReplaceTempView("raw")
s.sql(
    "SELECT id, CAST(ds AS DECIMAL(10,2)) AS d, CAST(vs AS DOUBLE) AS v FROM raw"
).createOrReplaceTempView("t")
result = {"engine": engine, "ansi": ansi, "rows": s.sql("SELECT count(*) FROM t").collect()[0][0]}
PREDS = (("eq", "d = v"), ("lt", "d < v"), ("ns", "d <=> v"), ("in", "v IN (d)"))
PREDS = tuple((n, p.replace("d", "CAST(CAST(d AS STRING) AS DOUBLE)")) for n, p in PREDS)
for name, pred in PREDS:
    result[name] = sorted(r[0] for r in s.sql(f"SELECT id FROM t WHERE {pred}").collect())
with open(out, "w") as fh:
    json.dump(result, fh)
print(engine, ansi, {k: len(v) for k, v in result.items() if isinstance(v, list)})
