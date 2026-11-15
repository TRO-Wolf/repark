import json
import math
import random
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import repark._native as native
from repark import ReparkSession

ansi, out, variant, kout = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
print(native.__file__, flush=True)
s = ReparkSession.builder.appName("vcoerce").config("spark.sql.ansi.enabled", ansi).getOrCreate()


def values(extra: bool) -> list:
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
    if extra:
        for _ in range(2000):
            vals.append(rng.uniform(-1e6, 1e6))
    return sorted({repr(v) for v in vals})


def k1_text(java_text: str | None, scale: int) -> str | None:
    if java_text is None:
        return None
    q = Decimal(java_text).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)
    return format(q, "f")


def scale_of(d: str) -> int:
    return int(d.split(",")[1].rstrip(")"))


def key_of(d: str) -> str:
    return d.replace("DECIMAL", "d").replace(",", "_").replace("(", "").replace(")", "")


ktexts = values(True)
s.createDataFrame(list(enumerate(ktexts)), "id INT, t STRING").createOrReplaceTempView("rawk")
s.sql(
    "SELECT id, CAST(t AS DOUBLE) AS v, CAST(CAST(t AS DOUBLE) AS FLOAT) AS f FROM rawk"
).createOrReplaceTempView("xk")
KDECS = [
    "DECIMAL(10,2)",
    "DECIMAL(16,4)",
    "DECIMAL(20,6)",
    "DECIMAL(30,15)",
    "DECIMAL(38,10)",
    "DECIMAL(38,18)",
]
jt = {
    r[0]: (r[1], r[2])
    for r in s.sql(
        "SELECT id, CAST(v AS STRING), CAST(CAST(f AS DOUBLE) AS STRING) FROM xk"
    ).collect()
}
krec = []
for d in KDECS:
    k, sc = key_of(d), scale_of(d)
    rows = [(i, k1_text(jt[i][0], sc), k1_text(jt[i][1], sc)) for i in range(len(ktexts))]
    s.createDataFrame(rows, "id INT, a STRING, b STRING").createOrReplaceTempView("kk")
    for kern, col in (("k1", "a"), ("k1f", "b")):
        got = {
            r[0]: r[1]
            for r in s.sql(f"SELECT id, CAST(TRY_CAST({col} AS {d}) AS STRING) FROM kk").collect()
        }
        for i, t in enumerate(ktexts):
            krec.append({"shape": f"{kern}_route_{k}", "value": t, "answer": got.get(i)})
with Path(kout).open("w") as fh:
    for r in krec:
        fh.write(json.dumps(r) + "\n")
texts = values(False)
s.createDataFrame(list(enumerate(texts)), "id INT, t STRING").createOrReplaceTempView("raw")
s.sql(
    "SELECT id, CAST(t AS DOUBLE) AS v, CAST(CAST(t AS DOUBLE) AS FLOAT) AS f FROM raw"
).createOrReplaceTempView("x0")
DECS = [
    "DECIMAL(10,2)",
    "DECIMAL(10,3)",
    "DECIMAL(16,4)",
    "DECIMAL(20,6)",
    "DECIMAL(38,18)",
    "DECIMAL(5,0)",
]
cols = []
pre = {}
for d in DECS:
    k, r = key_of(d), scale_of(d)
    for i, a, b in s.sql(
        f"SELECT id, CAST(ROUND(v, {r}) AS STRING), CAST(CAST(ROUND(f, {r}) AS DOUBLE) AS "
        "STRING) FROM x0"
    ).collect():
        pre.setdefault(i, {})[f"kv_{k}"] = k1_text(a, r)
        pre[i][f"kf_{k}"] = k1_text(b, r)
    cols += [f"kv_{k}", f"kf_{k}"]
rows = [tuple([i] + [pre[i][c] for c in cols]) for i in range(len(texts))]
s.createDataFrame(
    rows, "id INT, " + ", ".join(f"{c} STRING" for c in cols)
).createOrReplaceTempView("pre")
s.sql(
    "SELECT x0.id, x0.v, x0.f, "
    + ", ".join(f"pre.{c}" for c in cols)
    + " FROM x0 JOIN pre ON x0.id = pre.id"
).createOrReplaceTempView("x")
K2 = variant == "k1k2"


def to_dbl(y: str) -> str:
    return f"CAST(CAST({y} AS STRING) AS DOUBLE)" if K2 else f"CAST({y} AS DOUBLE)"


SHAPES = {}
for d in DECS:
    k = key_of(d)
    w = to_dbl(f"CAST(kv_{k} AS {d})")
    SHAPES[f"eq_round_{k}"] = f"CAST({w} = v AS STRING)"
    SHAPES[f"lt_round_{k}"] = f"CAST({w} < v AS STRING)"
    SHAPES[f"le_round_{k}"] = f"CAST(v <= {w} AS STRING)"
    SHAPES[f"ns_round_{k}"] = f"CAST({w} <=> v AS STRING)"
    SHAPES[f"eqf_round_{k}"] = (
        f"CAST({to_dbl(f'CAST(kf_{k} AS {d})')} = CAST(f AS DOUBLE) AS STRING)"
    )
    SHAPES[f"coalesce_{k}"] = f"CAST(coalesce({to_dbl(f'CAST(NULL AS {d})')}, v) AS STRING)"
    SHAPES[f"if_{k}"] = f"CAST(IF(id < 0, {to_dbl(f'CAST(0 AS {d})')}, v) AS STRING)"
    SHAPES[f"greatest_{k}"] = f"CAST(greatest({to_dbl(f'CAST(-1000 AS {d})')}, v) AS STRING)"
    SHAPES[f"in_{k}"] = f"CAST(v IN ({w}, {to_dbl(f'CAST(0 AS {d})')}) AS STRING)"
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
print("repark", ansi, variant, len(records), len(texts), len(krec))
