import json
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

case, label, data, out = sys.argv[1:5]
s = (
    ReparkSession.builder.appName("csv-cost")
    .config("spark.sql.session.timeZone", "America/Los_Angeles")
    .getOrCreate()
)
path = f"{data}/k.csv"
SCHEMA = "n INT, ts TIMESTAMP"


def k0_count() -> object:
    return s.read.schema(SCHEMA).csv(path).count()


def k0_hour() -> object:
    frame = s.read.schema(SCHEMA).csv(path).filter("n > 1000000")
    return frame.selectExpr("max(hour(ts))").collect()


def k2() -> object:
    return s.sql("SELECT count(*), max(hour(ts)) FROM k2v").collect()


s.read.schema(SCHEMA).csv(path).createOrReplaceTempView("k2v")
CASES = {"k0_count": (k0_count, 5), "k0_hour": (k0_hour, 5), "k2": (k2, 5)}
action, reps = CASES[case]
got = action()
start = time.perf_counter()
for _ in range(reps):
    got = action()
seconds = time.perf_counter() - start
record = {
    "case": case,
    "label": label,
    "seconds": seconds,
    "reps": reps,
    "answer": str(got)[:200],
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as handle:
    handle.write(json.dumps(record) + "\n")
