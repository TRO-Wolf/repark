import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

label, case, out, wh_parent = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
ROWS = 10_000_000
REPEATS = int(os.environ.get("REPARK_COST_REPEATS", "5"))
EXPLAIN = os.environ.get("REPARK_COST_EXPLAIN") == "1"
print(native.__file__, flush=True)
wh = tempfile.mkdtemp(prefix=f"nocast-{label}-{case}-", dir=wh_parent)
s = ReparkSession.builder.appName("ddr-nocast-cost").getOrCreate()
s.register_memory_catalog("ice", wh)
s.sql("CREATE NAMESPACE ice.ns").collect()

if case == "mixed":
    s.sql(
        f"SELECT id, CAST(id AS DOUBLE) / 1000 + 0.0005 AS v, "
        f"CAST(id AS DECIMAL(16,4)) AS d FROM range({ROWS})"
    ).createOrReplaceTempView("src")
    s.sql("CREATE TABLE ice.ns.base (id BIGINT, v DOUBLE, d DECIMAL(16,4)) USING iceberg").collect()
    s.sql("INSERT INTO ice.ns.base SELECT id, v, d FROM src").collect()
    QUERIES = {
        "project_decimal": "SELECT max(d * 2 + d) FROM ice.ns.base",
        "project_double": "SELECT max(v * 2 + v) FROM ice.ns.base",
        "filter_decimal": "SELECT count(*) FROM ice.ns.base WHERE d > 5000000",
        "filter_double": "SELECT count(*) FROM ice.ns.base WHERE v > 5000",
        "aggregate_both": (
            "SELECT id % 100 AS k, sum(d) AS sd, avg(v) AS av, max(d) AS md, min(v) AS nv "
            "FROM ice.ns.base GROUP BY id % 100"
        ),
        "store_decimal_20_6": "INSERT INTO ice.ns.{t} SELECT id, d FROM ice.ns.base",
    }
    TARGETS = {"store_decimal_20_6": "DECIMAL(20,6)"}
elif case == "double":
    s.sql(
        f"SELECT id, CAST(id AS DOUBLE) / 1000 + 0.0005 AS v FROM range({ROWS})"
    ).createOrReplaceTempView("src")
    s.sql("CREATE TABLE ice.ns.base (id BIGINT, v DOUBLE) USING iceberg").collect()
    s.sql("INSERT INTO ice.ns.base SELECT id, v FROM src").collect()
    QUERIES = {
        "project_double": "SELECT max(v * 2 + v), min(v / 3) FROM ice.ns.base",
        "store_double": "INSERT INTO ice.ns.{t} SELECT id, v * 2 + v FROM ice.ns.base",
    }
    TARGETS = {"store_double": "DOUBLE"}
else:
    raise SystemExit(f"unknown case {case}")

counter = 0
result = {
    "label": label,
    "case": case,
    "native": native.__file__.split("/scratchpad/", 1)[-1],
    "times": {},
    "answers": {},
    "repeats": REPEATS,
    "plans": {},
}
for name, sql in QUERIES.items():
    if EXPLAIN:
        statement = sql
        if name in TARGETS:
            s.sql(
                f"CREATE TABLE ice.ns.x_{name} (id BIGINT, v {TARGETS[name]}) USING iceberg"
            ).collect()
            statement = sql.format(t=f"x_{name}")
        try:
            rows = s.sql(f"EXPLAIN {statement}").collect()
            result["plans"][name] = [str(tuple(row)) for row in rows]
        except Exception as error:
            result["plans"][name] = [type(error).__name__, str(error).splitlines()[0][:300]]
        continue
    times = []
    answer = None
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
        rows = s.sql(statement).collect()
        elapsed = time.perf_counter() - start
        if run > 0:
            times.append(elapsed)
        if name in TARGETS:
            rows = s.sql(f"SELECT count(*), max(v) FROM ice.ns.{table}").collect()
        answer = sorted(
            str(tuple(round(x, 6) if isinstance(x, float) else x for x in row)) for row in rows
        )[:3]
    result["times"][name] = statistics.median(times)
    result["answers"][name] = answer
    print(name, round(result["times"][name], 4), flush=True)
with Path(out).open("a") as fh:
    fh.write(json.dumps(result) + "\n")
