import json
import random
import struct
import sys
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

out = sys.argv[1]
rng = random.Random(20261114)
cands = set()
for _ in range(150000):
    e = rng.randint(1023 - 70, 1023 + 85)
    m = rng.getrandbits(52)
    cands.add(struct.unpack(">d", struct.pack(">Q", (e << 52) | m))[0])
for _ in range(150000):
    j = rng.randint(0, 18)
    k = rng.randint(1, 10 ** rng.randint(1, 17))
    cands.add(k / 10**j)
for n in range(-40, 41):
    cands.add(float(f"1e{n}"))
    cands.add(float(f"2e{n}"))
    cands.add(float(f"5e{n}"))
for n in range(-60, 80):
    cands.add(2.0**n)
vals = sorted(cands)
s = (
    SparkSession.builder.master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
df = s.createDataFrame([(i, v) for i, v in enumerate(vals)], "id INT, v DOUBLE")
rows = df.selectExpr("id", "v", "CAST(v AS STRING) AS t").collect()
hits = []
for r in rows:
    py = repr(r.v)
    if Decimal(r.t) != Decimal(py) or len(
        r.t.replace("-", "").split("E")[0].replace(".", "").strip("0")
    ) > len(py.replace("-", "").split("e")[0].replace(".", "").strip("0")):
        hits.append({"repr": py, "jdk": r.t, "bits": struct.pack(">d", r.v).hex()})
with Path(out).open("w") as fh:
    json.dump({"scanned": len(vals), "hits": hits}, fh)
print(len(vals), len(hits))
