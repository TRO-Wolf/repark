import json
import math
import random
import sys
from decimal import Decimal

engine, ansi, out = sys.argv[1:4]
if engine == "spark":
    from pyspark.sql import SparkSession as Builder

    b = Builder.builder.master("local[2]").config("spark.ui.enabled", "false")
else:
    from repark import ReparkSession as Builder

    b = Builder.builder.appName("ddu-d2d")
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
for _ in range(2000):
    vals.append(rng.uniform(-1e6, 1e6))
texts = sorted(set(format(Decimal(repr(v)), "f") for v in vals))
s.createDataFrame(list(enumerate(texts)), "id INT, t STRING").createOrReplaceTempView("raw")
SHAPES = {
    "d38_18": "CAST(TRY_CAST(t AS DECIMAL(38,18)) AS DOUBLE)",
    "d38_10": "CAST(TRY_CAST(t AS DECIMAL(38,10)) AS DOUBLE)",
    "d30_15": "CAST(TRY_CAST(t AS DECIMAL(30,15)) AS DOUBLE)",
    "d20_6": "CAST(TRY_CAST(t AS DECIMAL(20,6)) AS DOUBLE)",
    "d10_2": "CAST(TRY_CAST(t AS DECIMAL(10,2)) AS DOUBLE)",
}
records = []
for name, expr in SHAPES.items():
    try:
        got = {r[0]: r[1] for r in s.sql(f"SELECT id, CAST({expr} AS STRING) FROM raw").collect()}
        for i, t in enumerate(texts):
            records.append({"shape": name, "value": t, "answer": got.get(i)})
    except Exception as e:
        records.append({"shape": name, "value": None, "answer": "ERR " + str(e).strip().splitlines()[0][:200]})
with open(out, "w") as fh:
    for r in records:
        fh.write(json.dumps(r) + "\n")
print(engine, ansi, len(records), len(texts))
