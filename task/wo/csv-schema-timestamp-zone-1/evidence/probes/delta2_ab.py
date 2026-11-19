import json
import statistics
import sys
import time
from collections.abc import Callable
from pathlib import Path

import repark._native as native
from repark import ReparkSession
from repark.spark.functions import col

label, data, out, mode = sys.argv[1:5]
s = (
    ReparkSession.builder.appName("delta2-ab")
    .config("spark.sql.session.timeZone", "America/Los_Angeles")
    .getOrCreate()
)
base = type(s).read_csv


def all_text_read_csv(self: object, path: str, options: dict[str, str] | None = None) -> object:
    if options and "utf8_columns" in options:
        options = dict(options, inferschema="true")
    return base(self, path, options)


if mode == "alltext":
    type(s).read_csv = all_text_read_csv
path = f"{data}/k.csv"
SCHEMA = "n INT, ts TIMESTAMP"
QUERY = "SELECT count(*), max(hour(ts)) FROM k2v"
if mode == "twin":
    text = s.read_csv(path, {"header": "false", "utf8_columns": "column_2", "inferschema": "true"})
    text.select(col("column_2").cast("timestamp").alias("ts")).createOrReplaceTempView("k2v")
else:
    s.read.schema(SCHEMA).csv(path).createOrReplaceTempView("k2v")
planned = s.sql(QUERY)
hour = s.read.schema(SCHEMA).csv(path).filter("n > 1000000").selectExpr("max(hour(ts))")


def k2() -> object:
    return s.sql(QUERY).collect()


def k0_hour() -> object:
    frame = s.read.schema(SCHEMA).csv(path).filter("n > 1000000")
    return frame.selectExpr("max(hour(ts))").collect()


def k0_count() -> object:
    return s.read.schema(SCHEMA).csv(path).count()


def read() -> object:
    return s.read.schema(SCHEMA).csv(path)


cases: dict[str, Callable[[], object]] = {
    "k2": k2,
    "k2_planned": planned.collect,
    "k0_hour": k0_hour,
    "k0_hour_planned": hour.collect,
    "k0_count": k0_count,
    "read": read,
}
answers = {key: str(action())[:80] for key, action in cases.items() if key != "read"}
samples: dict[str, list[float]] = {key: [] for key in cases}
for _ in range(9):
    for key, action in cases.items():
        start = time.perf_counter()
        action()
        samples[key].append(time.perf_counter() - start)
record = {
    "label": f"{label}:{mode}",
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    "answers": answers,
    "median_ms": {key: round(statistics.median(vals) * 1000, 2) for key, vals in samples.items()},
    "min_ms": {key: round(min(vals) * 1000, 2) for key, vals in samples.items()},
    "res": samples,
}
with Path(out).open("a") as handle:
    handle.write(json.dumps(record) + "\n")
