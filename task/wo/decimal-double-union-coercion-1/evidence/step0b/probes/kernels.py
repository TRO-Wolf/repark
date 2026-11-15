import json
import math
import random
import sys
from decimal import Decimal
from pathlib import Path

engine, ansi, out = sys.argv[1:4]
if engine == "spark":
    from pyspark.sql import SparkSession as Builder

    b = Builder.builder.master("local[2]").config("spark.ui.enabled", "false")
    print("spark", flush=True)
else:
    import repark._native as native
    from repark import ReparkSession as Builder

    print(native.__file__, flush=True)
    b = Builder.builder.appName("ddu-kernels")
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
texts = sorted({repr(v) for v in vals})
plain = sorted({format(Decimal(t), "f") for t in texts})
s.createDataFrame(list(enumerate(texts)), "id INT, t STRING").createOrReplaceTempView("rawd")
s.sql(
    "SELECT id, CAST(t AS DOUBLE) AS v, CAST(CAST(t AS DOUBLE) AS FLOAT) AS f FROM rawd"
).createOrReplaceTempView("x")
s.createDataFrame(list(enumerate(plain)), "id INT, t STRING").createOrReplaceTempView("rawp")
DECS = [
    "DECIMAL(10,2)",
    "DECIMAL(16,4)",
    "DECIMAL(20,6)",
    "DECIMAL(30,15)",
    "DECIMAL(38,10)",
    "DECIMAL(38,18)",
]
SHAPES = {}
for d in DECS:
    k = d.replace("DECIMAL", "d").replace(",", "_").replace("(", "").replace(")", "")
    SHAPES[f"k1_direct_{k}"] = ("x", texts, f"CAST(TRY_CAST(v AS {d}) AS STRING)")
    SHAPES[f"k1_text_{k}"] = ("x", texts, f"CAST(TRY_CAST(CAST(v AS STRING) AS {d}) AS STRING)")
    SHAPES[f"k1f_direct_{k}"] = ("x", texts, f"CAST(TRY_CAST(f AS {d}) AS STRING)")
    SHAPES[f"k1f_text_{k}"] = (
        "x",
        texts,
        f"CAST(TRY_CAST(CAST(CAST(f AS DOUBLE) AS STRING) AS {d}) AS STRING)",
    )
    SHAPES[f"k2_direct_{k}"] = (
        "rawp",
        plain,
        f"CAST(CAST(TRY_CAST(t AS {d}) AS DOUBLE) AS STRING)",
    )
    SHAPES[f"k2_text_{k}"] = (
        "rawp",
        plain,
        f"CAST(CAST(CAST(TRY_CAST(t AS {d}) AS STRING) AS DOUBLE) AS STRING)",
    )
records = []
for name, (view, keys, expr) in SHAPES.items():
    try:
        got = {r[0]: r[1] for r in s.sql(f"SELECT id, {expr} FROM {view}").collect()}
        for i, t in enumerate(keys):
            records.append({"shape": name, "value": t, "answer": got.get(i)})
    except Exception as e:
        records.append(
            {"shape": name, "value": None, "answer": "ERR " + str(e).strip().splitlines()[0][:200]}
        )
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r) + "\n")
print(engine, ansi, len(records), len(texts), len(plain))
