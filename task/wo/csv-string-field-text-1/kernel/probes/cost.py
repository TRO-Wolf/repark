import json
import os
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession
from repark.spark import functions
from repark.spark.column import Column

variant, round_no, data, ctl_path, ack_path, out = sys.argv[1:7]
s = (
    ReparkSession.builder.appName("csv-field-kernel-cost")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
path = str(Path(data, "is.csv"))


def frame() -> object:
    texts = s.read_csv(path, {"header": "false", "utf8_columns": "column_1"})
    if variant == "cast":
        n = functions.col("column_1").cast("int")
    else:
        n = Column(functions.col("column_1")._inner.csv_field("int"))
    return texts.select(n.alias("n"), functions.col("column_2").alias("s"))


def action() -> object:
    return frame().selectExpr("sum(n)", "max(s)", "count(n)").collect()


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
    "variant": variant,
    "round": int(round_no),
    "reps": reps,
    "median": (times[4] + times[5]) / 2,
    "total": sum(times),
    "answer": str(got)[:120],
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as handle:
    handle.write(json.dumps(record) + "\n")
s.stop()
