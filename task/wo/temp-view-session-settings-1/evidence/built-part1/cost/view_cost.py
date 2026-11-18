import json
import re
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

case, label, data, out = sys.argv[1:5]
s = (
    ReparkSession.builder.appName("view-cost")
    .config("spark.sql.ansi.enabled", "true")
    .getOrCreate()
)
s.read.parquet(f"{data}/k").createOrReplaceTempView("k")
s.read.parquet(f"{data}/k2").createOrReplaceTempView("k2")
s.sql("CREATE OR REPLACE TEMP VIEW v AS SELECT a, b, a % 7 AS m FROM k").collect()
s.sql("CREATE OR REPLACE TEMP VIEW vv AS SELECT a, m FROM v").collect()
QUERIES = {
    "read": ("SELECT count(*), sum(m) FROM v", 20),
    "filter": ("SELECT count(*), sum(m) FROM v WHERE a > 1000 AND b < 1500000", 8),
    "join": ("SELECT count(*), sum(v.m) FROM v JOIN k2 ON v.a = k2.a", 18),
    "nested": ("SELECT count(*), sum(m) FROM vv", 20),
    "noview": ("SELECT count(*), sum(a % 7) FROM k", 20),
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
    sys.exit(0)
kind, name = case.split("_", 1)
if kind == "differ":
    s.sql("SET spark.sql.ansi.enabled=false").collect()
sql, reps = QUERIES[name]
got = s.sql(sql).collect()
start = time.perf_counter()
for _ in range(reps):
    got = s.sql(sql).collect()
seconds = time.perf_counter() - start
record = {
    "case": case,
    "label": label,
    "seconds": seconds,
    "reps": reps,
    "answer": str([list(r) for r in got])[:300],
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as fh:
    fh.write(json.dumps(record) + "\n")
