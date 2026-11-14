import json
import statistics
import sys
import tempfile
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

label, out, wh_parent = sys.argv[1], sys.argv[2], sys.argv[3]
ROWS = 10_000_000
REPEATS = 3
print(native.__file__, flush=True)
wh = tempfile.mkdtemp(prefix=f"cost-{label}-", dir=wh_parent)
s = ReparkSession.builder.appName("ddr-cost").getOrCreate()
s.register_memory_catalog("ice", wh)
s.sql("CREATE NAMESPACE ice.ns").collect()
s.sql(
    f"SELECT id, CAST(id AS DOUBLE) / 1000 + 0.0005 AS v, "
    f"CAST(CAST(id AS DOUBLE) / 1000 + 0.0005 AS DECIMAL(16,4)) AS d FROM range({ROWS})"
).createOrReplaceTempView("src")
s.sql("CREATE TABLE ice.ns.base (id BIGINT, v DOUBLE, d DECIMAL(16,4)) USING iceberg").collect()
s.sql("INSERT INTO ice.ns.base SELECT id, v, d FROM src").collect()
QUERIES = {
    "select_cast_10_2": "SELECT max(CAST(v AS DECIMAL(10,2))) FROM ice.ns.base",
    "select_cast_38_18": "SELECT max(CAST(v AS DECIMAL(38,18))) FROM ice.ns.base",
    "filter_decimal_eq_double": "SELECT count(*) FROM ice.ns.base WHERE d = v",
    "store_10_2": "INSERT INTO ice.ns.{t} SELECT id, v FROM ice.ns.base",
    "store_38_18": "INSERT INTO ice.ns.{t} SELECT id, v FROM ice.ns.base",
}
TARGETS = {"store_10_2": "DECIMAL(10,2)", "store_38_18": "DECIMAL(38,18)"}
counter = 0
result = {"label": label, "native": native.__file__.split("/scratchpad/", 1)[-1], "times": {}}
for name, sql in QUERIES.items():
    times = []
    for run in range(REPEATS + 1):
        statement = sql
        if name in TARGETS:
            counter += 1
            table = f"t{counter}"
            s.sql(
                f"CREATE TABLE ice.ns.{table} (id BIGINT, v {TARGETS[name]}) USING iceberg"
            ).collect()
            statement = sql.format(t=table)
        start = time.perf_counter()
        s.sql(statement).collect()
        elapsed = time.perf_counter() - start
        if run > 0:
            times.append(elapsed)
    result["times"][name] = statistics.median(times)
    print(name, round(result["times"][name], 4), flush=True)
with Path(out).open("a") as fh:
    fh.write(json.dumps(result) + "\n")
