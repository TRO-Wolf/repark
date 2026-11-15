import json
import math
import random
import sys
from pathlib import Path

engine, ansi, out = sys.argv[1], sys.argv[2], sys.argv[3]
if engine == "spark":
    from pyspark.sql import SparkSession as Builder

    b = Builder.builder.master("local[2]").config("spark.ui.enabled", "false")
    print("spark", flush=True)
else:
    import repark._native as native
    from repark import ReparkSession as Builder

    print(native.__file__, flush=True)
    b = Builder.builder.appName("vcoerce")
s = b.config("spark.sql.ansi.enabled", ansi).getOrCreate()
rng = random.Random(777)
vals = []
for _ in range(500):
    q = rng.randint(1, 6)
    m = rng.randint(1, 10 ** rng.randint(1, 8))
    x = float(f"{m}e-{q}")
    vals.append(x)
    for k in (1, 2, 3):
        y = x
        for _ in range(k):
            y = math.nextafter(y, math.inf)
        vals.append(y)
        y = x
        for _ in range(k):
            y = math.nextafter(y, -math.inf)
        vals.append(y)
for _ in range(200):
    vals.append(rng.uniform(-100, 100))
texts = sorted({repr(v) for v in vals})
rows = [(i, t) for i, t in enumerate(texts)]
s.createDataFrame(rows, "id INT, t STRING").createOrReplaceTempView("raw")
s.sql(
    "SELECT id, CAST(t AS DOUBLE) AS v, CAST(CAST(t AS DOUBLE) AS FLOAT) AS f FROM raw"
).createOrReplaceTempView("x")
DECS = [
    "DECIMAL(10,2)",
    "DECIMAL(10,3)",
    "DECIMAL(16,4)",
    "DECIMAL(20,6)",
    "DECIMAL(38,18)",
    "DECIMAL(5,0)",
]
SHAPES = {}
for d in DECS:
    k = d.replace("DECIMAL", "d").replace(",", "_").replace("(", "").replace(")", "")
    r = int(d.split(",")[1].rstrip(")"))
    SHAPES[f"eq_round_{k}"] = f"CAST(CAST(ROUND(v, {r}) AS {d}) = v AS STRING)"
    SHAPES[f"lt_round_{k}"] = f"CAST(CAST(ROUND(v, {r}) AS {d}) < v AS STRING)"
    SHAPES[f"le_round_{k}"] = f"CAST(v <= CAST(ROUND(v, {r}) AS {d}) AS STRING)"
    SHAPES[f"ns_round_{k}"] = f"CAST(CAST(ROUND(v, {r}) AS {d}) <=> v AS STRING)"
    SHAPES[f"eqf_round_{k}"] = f"CAST(CAST(ROUND(f, {r}) AS {d}) = f AS STRING)"
    SHAPES[f"coalesce_{k}"] = f"CAST(coalesce(CAST(NULL AS {d}), v) AS STRING)"
    SHAPES[f"if_{k}"] = f"CAST(IF(id < 0, CAST(0 AS {d}), v) AS STRING)"
    SHAPES[f"greatest_{k}"] = f"CAST(greatest(CAST(-1000 AS {d}), v) AS STRING)"
    SHAPES[f"in_{k}"] = f"CAST(v IN (CAST(ROUND(v, {r}) AS {d}), CAST(0 AS {d})) AS STRING)"
records = []
for name, expr in SHAPES.items():
    try:
        got = {r[0]: r[1] for r in s.sql(f"SELECT id, {expr} FROM x").collect()}
        for i, t in enumerate(texts):
            records.append({"shape": name, "value": t, "answer": got.get(i)})
    except Exception as e:
        records.append(
            {"shape": name, "value": None, "answer": "ERR " + str(e).strip().splitlines()[0][:200]}
        )
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r) + "\n")
print(engine, ansi, len(records), len(texts))
