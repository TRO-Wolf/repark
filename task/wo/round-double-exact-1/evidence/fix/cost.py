import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

label, out, wh_parent, run = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
ROWS = 10_000_000
STORE_ROWS = 2_000_000
REPEATS = 3
print(native.__file__, flush=True)
wh = tempfile.mkdtemp(prefix=f"rxcost-{label}-", dir=wh_parent)
s = ReparkSession.builder.appName("round-exact-cost").getOrCreate()
s.register_memory_catalog("ice", wh)
s.sql("CREATE NAMESPACE ice.ns").collect()
s.sql(
    f"SELECT id, CAST(id AS DOUBLE) * 1.2345678901 / 1000 AS v, "
    f"CAST(id % 100000 AS DOUBLE) / 1000 AS p, "
    f"CAST(CAST(id AS DOUBLE) * 1.2345678901 / 1000 AS FLOAT) AS f FROM range({ROWS})"
).createOrReplaceTempView("gen")
s.sql("CREATE TABLE ice.ns.base (id BIGINT, v DOUBLE, p DOUBLE, f FLOAT) USING iceberg").collect()
s.sql("INSERT INTO ice.ns.base SELECT id, v, p, f FROM gen").collect()
STORE = f"INSERT INTO ice.ns.{{t}} SELECT id, round(v, 2) FROM ice.ns.base WHERE id < {STORE_ROWS}"
QUERIES = {
    "round_2": "SELECT sum(round(v, 2)) FROM ice.ns.base",
    "round_18": "SELECT sum(round(v, 18)) FROM ice.ns.base",
    "bround_2": "SELECT sum(bround(v, 2)) FROM ice.ns.base",
    "round_float_2": "SELECT sum(round(f, 2)) FROM ice.ns.base",
    "round_prices_2": "SELECT sum(round(p, 2)) FROM ice.ns.base",
    "store_double": STORE,
    "store_10_2": STORE,
}
TARGETS = {"store_double": "DOUBLE", "store_10_2": "DECIMAL(10,2)"}
counter = 0
result = {
    "label": label,
    "run": int(run),
    "native": native.__file__.split("/scratchpad/", 1)[-1],
    "times": {},
}
for name, sql in QUERIES.items():
    times = []
    for repeat in range(REPEATS + 1):
        statement = sql
        if name in TARGETS:
            counter += 1
            table = f"t{counter}"
            s.sql(
                f"CREATE TABLE ice.ns.{table} (id BIGINT, r {TARGETS[name]}) USING iceberg"
            ).collect()
            statement = sql.format(t=table)
        start = time.perf_counter()
        s.sql(statement).collect()
        elapsed = time.perf_counter() - start
        if repeat > 0:
            times.append(elapsed)
    result["times"][name] = statistics.median(times)
    print(name, round(result["times"][name], 4), flush=True)
with Path(out).open("a") as fh:
    fh.write(json.dumps(result) + "\n")
