import json
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession
from repark.spark import functions as fn

case, label, data, out = sys.argv[1:5]
scratch = tempfile.mkdtemp(prefix="cv2-", dir=data + "/..")
s = (
    ReparkSession.builder.appName("view-cost-2")
    .config("spark.sql.ansi.enabled", "true")
    .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", scratch + "/lc")
    .getOrCreate()
)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
s.read.parquet(f"{data}/k").createOrReplaceTempView("k")
s.read.parquet(f"{data}/k2").createOrReplaceTempView("k2")
s.sql("SELECT a, b, a % 7 AS m FROM k").createOrReplaceTempView("v")
s.sql("SELECT a, m FROM v").createOrReplaceTempView("vv")
small = s.sql("SELECT a, b, a % 7 AS m FROM k WHERE a < 200")
QUERIES = {
    "read": ("SELECT count(*), sum(m) FROM v", 20),
    "filter": ("SELECT count(*), sum(m) FROM v WHERE a > 1000 AND b < 1500000", 8),
    "join": ("SELECT count(*), sum(v.m) FROM v JOIN k2 ON v.a = k2.a", 18),
    "nested": ("SELECT count(*), sum(m) FROM vv", 20),
    "noview": ("SELECT count(*), sum(a % 7) FROM k", 20),
    "limit": ("SELECT m FROM v WHERE a > 5 LIMIT 3", 20),
}
serial = iter(range(10_000))


def facade(op: str) -> list:
    if op == "selectexpr":
        return small.selectExpr("m + 1 AS n").collect()
    if op == "condjoin":
        other = s.sql("SELECT CAST(1 AS INT) AS one").alias("r")
        return small.alias("l").join(other, fn.col("l.m") > fn.col("r.one")).select("l.m").collect()
    if op == "unpivot":
        return small.unpivot(["a"], ["m"], "k", "val").select("val").collect()
    if op == "setop":
        return small.intersect(small).collect()
    if op == "explode":
        return small.select(fn.explode(fn.array(fn.col("m"))).alias("m")).collect()
    if op == "gagg_lit":
        return small.select(fn.max("m").alias("m"), fn.lit(1).alias("one")).collect()
    table = "lc.ns.t" + str(next(serial))
    if op == "saveastable":
        small.write.format("iceberg").saveAsTable(table)
    elif op == "insertinto":
        s.sql("CREATE TABLE " + table + " (a INT, b BIGINT, m INT) USING iceberg").collect()
        small.write.insertInto(table)
    elif op == "writeto":
        small.writeTo(table).using("iceberg").create()
    elif op == "parquet":
        small.write.parquet(scratch + "/" + table)
    return []


FACADE = {
    "selectexpr": 20,
    "condjoin": 10,
    "unpivot": 20,
    "setop": 10,
    "explode": 20,
    "gagg_lit": 20,
    "saveastable": 4,
    "insertinto": 3,
    "writeto": 4,
    "parquet": 8,
}
NOISE = [
    (re.compile(r"__repark_[a-z_]*?[0-9a-f]{32}"), "<scratch>"),
    (re.compile(r"file_groups=\{[^}]*\}"), "file_groups=<files>"),
    (re.compile(r"RoundRobinBatch\(\d+\)"), "RoundRobinBatch(<n>)"),
]
if case == "explain":
    plans = {}
    for name, (sql, _) in QUERIES.items():
        text = "\n".join(str(c) for row in s.sql("EXPLAIN " + sql).collect() for c in row)
        for pattern, token in NOISE:
            text = pattern.sub(token, text)
        plans[name] = text
    Path(out).write_text(json.dumps(plans, indent=1, sort_keys=True) + "\n")
    shutil.rmtree(scratch, ignore_errors=True)
    sys.exit(0)
kind, name = case.split("_", 1)
held = s.sql("SELECT a, b, a % 7 AS m FROM k")
if kind == "differ":
    s.sql("SET spark.sql.ansi.enabled=false").collect()


def once() -> object:
    if name == "cache":
        cached = held.cache()
        got = cached.count()
        cached.unpersist()
        return got
    if name in FACADE:
        return facade(name)
    return s.sql(QUERIES[name][0]).collect()


reps = 6 if name == "cache" else FACADE.get(name) or QUERIES[name][1]
got = once()
start = time.perf_counter()
for _ in range(reps):
    got = once()
seconds = time.perf_counter() - start
record = {
    "case": case,
    "label": label,
    "seconds": seconds,
    "reps": reps,
    "answer": str(got if not isinstance(got, list) else [list(r) for r in got])[:300],
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as fh:
    fh.write(json.dumps(record) + "\n")
shutil.rmtree(scratch, ignore_errors=True)
