import json
import os
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

label, case, round_no, data, ctl_path, ack_path, out = sys.argv[1:8]
s = (
    ReparkSession.builder.appName("csv-wiring-cost")
    .config("spark.sql.session.timeZone", "America/Los_Angeles")
    .getOrCreate()
)
K = str(Path(data, "k.csv"))
IS = str(Path(data, "is.csv"))
CASES = {
    "k0_count": lambda: s.read.schema("n INT, ts TIMESTAMP").csv(K).count(),
    "k0_hour": lambda: (
        s.read.schema("n INT, ts TIMESTAMP")
        .csv(K)
        .filter("n > 1000000")
        .selectExpr("max(hour(ts))")
        .collect()
    ),
    "is_count": lambda: s.read.schema("n INT, s STRING").csv(IS).count(),
    "is_sum": lambda: (
        s.read.schema("n INT, s STRING").csv(IS).selectExpr("sum(n)", "max(s)").collect()
    ),
    "is_none": lambda: s.read.csv(IS).selectExpr("count(_c0)", "max(_c1)").collect(),
    "is_infer": lambda: (
        s.read.option("inferSchema", "true").csv(IS).selectExpr("sum(_c0)", "max(_c1)").collect()
    ),
}
action = CASES[case]
reps = 10
got = action()
action()
ctl = os.open(ctl_path, os.O_WRONLY)
ack = os.open(ack_path, os.O_RDONLY)
os.write(ctl, b"enable\n")
os.read(ack, 5)
times = []
for _ in range(reps):
    start = time.perf_counter()
    got = action()
    times.append(time.perf_counter() - start)
os.write(ctl, b"disable\n")
os.read(ack, 5)
times.sort()
record = {
    "label": label,
    "case": case,
    "round": int(round_no),
    "median": (times[4] + times[5]) / 2,
    "answer": str(got)[:160],
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as handle:
    handle.write(json.dumps(record) + "\n")
s.stop()
