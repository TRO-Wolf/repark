import json
import random
import sys
from pathlib import Path

engine, ansi, out = sys.argv[1], sys.argv[2], sys.argv[3]
if engine == "repark":
    import repark._native as native
    from repark import ReparkSession as Builder

    print(native.__file__, flush=True)
    b = Builder.builder.appName("ddr-fuzz")
else:
    from pyspark.sql import SparkSession as Builder

    b = Builder.builder.master("local[1]").config("spark.ui.enabled", "false")
s = b.config("spark.sql.ansi.enabled", ansi).getOrCreate()
rng = random.Random(20261113)
values = []
for _ in range(400):
    base = rng.choice([1.255, 2.675, 0.125, 9.995, 1.005, 0.3, 123.455, 0.045])
    values.append(base + rng.choice([-1, 1]) * rng.randint(1, 8) * 2.0**-52 * max(1.0, abs(base)))
for _ in range(400):
    values.append(round(rng.uniform(-1000, 1000), rng.randint(1, 4)))
for _ in range(200):
    values.append(rng.uniform(-10, 10))
texts = [repr(v) for v in values]
rows = ", ".join(f"({i}, CAST('{t}' AS DOUBLE))" for i, t in enumerate(texts))
s.sql(f"SELECT * FROM VALUES {rows} AS x(id, v)").createOrReplaceTempView("f")
SHAPES = {
    "eq_10_2": "CAST(CAST(v AS DECIMAL(10,2)) = v AS STRING)",
    "eq_10_3": "CAST(CAST(ROUND(v, 3) AS DECIMAL(10,3)) = v AS STRING)",
    "eq_lit": "CAST(v = CAST(1.255 AS DECIMAL(10,3)) AS STRING)",
    "coalesce": "CAST(coalesce(CAST(NULL AS DECIMAL(10,2)), v) AS STRING)",
    "case": "CAST(CASE WHEN id < 0 THEN CAST(0 AS DECIMAL(10,2)) ELSE v END AS STRING)",
    "in_list": "CAST(v IN (CAST(1.26 AS DECIMAL(10,2)), CAST(2.68 AS DECIMAL(10,2))) AS STRING)",
    "between": (
        "CAST(v BETWEEN CAST(1.25 AS DECIMAL(10,2)) AND CAST(1.26 AS DECIMAL(10,2)) AS STRING)"
    ),
}
records = []
for name, expr in SHAPES.items():
    try:
        got = {r[0]: r[1] for r in s.sql(f"SELECT id, {expr} FROM f").collect()}
        for i, t in enumerate(texts):
            records.append({"shape": name, "value": t, "answer": got.get(i)})
    except Exception as e:
        records.append({"shape": name, "error": str(e).strip().splitlines()[0][:200]})
union = s.sql(
    "SELECT id, v FROM (SELECT id, v FROM f UNION ALL SELECT -1, CAST(0 AS DECIMAL(10,2)))"
)
got = {r[0]: str(r[1]) for r in union.collect()}
for i, t in enumerate(texts):
    records.append({"shape": "union", "value": t, "answer": got.get(i)})
with Path(out).open("w") as fh:
    for record in records:
        fh.write(json.dumps(record, sort_keys=True) + "\n")
print(engine, ansi, len(records))
