import json
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path

import repark._native as native
from repark import ReparkSession

label, case, round_no, data, ctl_path, ack_path, out = sys.argv[1:8]
SHAPE = ["n", "CAST(ts AS STRING) AS t", "hour(ts) AS h", "unix_timestamp(ts) AS u"]
json_dir = Path(data) / "json2m"
s = (
    ReparkSession.builder.appName("cache-a1-perf")
    .config("spark.sql.session.timeZone", "America/Los_Angeles")
    .getOrCreate()
)
s.read.schema("n INT, ts TIMESTAMP").json(str(json_dir)).limit(1).collect()


def frame() -> object:
    return s.read.schema("n INT, ts TIMESTAMP").json(str(json_dir)).selectExpr(*SHAPE)


ctl = os.open(ctl_path, os.O_WRONLY)
ack = os.open(ack_path, os.O_RDONLY)


def counted(action: Callable[[], object]) -> tuple[object, float]:
    os.write(ctl, b"enable\n")
    os.read(ack, 5)
    start = time.perf_counter()
    got = action()
    seconds = time.perf_counter() - start
    os.write(ctl, b"disable\n")
    os.read(ack, 5)
    return got, seconds


def view_total() -> int:
    f = frame().cache()
    f.createOrReplaceTempView("cv")
    return s.table("cv").count()


if case == "fill_count":
    f = frame().cache()
    got, seconds = counted(f.count)
elif case == "view_total":
    got, seconds = counted(view_total)
elif case == "plain_count":
    got, seconds = counted(lambda: frame().count())
else:
    raise SystemExit("unknown case " + case)
record = {
    "label": label,
    "case": case,
    "round": int(round_no),
    "seconds": seconds,
    "answer": got,
    "native": native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as handle:
    handle.write(json.dumps(record) + "\n")
s.stop()
