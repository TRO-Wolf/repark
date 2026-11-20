import json
import sys
import time
from pathlib import Path
from typing import Any

import repark._native as native
from repark import ReparkSession
from repark.spark import functions as fn

data, out_path, case = sys.argv[1:4]
ROWS = 2_000_000
SHAPE = ["n", "CAST(ts AS STRING) AS t", "hour(ts) AS h", "unix_timestamp(ts) AS u"]
print("NATIVE", native.__file__, flush=True)
root = Path(data)
json_dir, csv_dir = root / "json2m", root / "csv2m"
if not json_dir.exists():
    json_dir.mkdir(parents=True)
    with (json_dir / "a.json").open("w") as handle:
        for i in range(ROWS):
            handle.write(f'{{"n": {i}, "ts": "2024-01-01 {i % 24:02d}:{i % 60:02d}:00"}}\n')
if not csv_dir.exists():
    csv_dir.mkdir(parents=True)
    with (csv_dir / "a.csv").open("w") as handle:
        handle.write("n,ts\n")
        for i in range(ROWS):
            handle.write(f"{i},2024-01-01 {i % 24:02d}:{i % 60:02d}:00\n")
s = (
    ReparkSession.builder.appName("cache-lazy-cost")
    .config("spark.sql.session.timeZone", "America/Los_Angeles")
    .getOrCreate()
)
s.read.schema("n INT, ts TIMESTAMP").json(str(json_dir)).limit(1).collect()
marks: dict[str, float] = {}
start = time.perf_counter()


def mark(name: str) -> None:
    marks[name] = round((time.perf_counter() - start) * 1000, 1)


def reader(kind: str) -> Any:
    if kind == "json":
        return s.read.schema("n INT, ts TIMESTAMP").json(str(json_dir))
    return s.read.option("header", "true").schema("n INT, ts TIMESTAMP").csv(str(csv_dir))


if case == "json_cache_count_collect":
    f = reader("json").selectExpr(*SHAPE).cache()
    mark("build")
    f.count()
    mark("count")
    f.collect()
    mark("collect")
elif case == "json_cache_collect_first":
    f = reader("json").selectExpr(*SHAPE).cache()
    mark("build")
    f.collect()
    mark("collect")
    f.count()
    mark("count")
elif case == "json_cache_view":
    f = reader("json").selectExpr(*SHAPE).cache()
    f.createOrReplaceTempView("cv")
    mark("register")
    s.table("cv").count()
    mark("count")
elif case == "csv_udf_derived":
    made = fn.udf(lambda x: x + 1, "int")
    g = reader("csv").select("n", "ts", made("n").alias("m")).selectExpr(*SHAPE, "m")
    mark("build")
    g.count()
    mark("count")
    g.count()
    mark("count2")
else:
    made = fn.udf(lambda x: x + 1, "int")
    g = reader("csv").selectExpr(*SHAPE).select("n", "t", "h", "u", made("n").alias("m"))
    mark("build")
    g.count()
    mark("count")
    g.count()
    mark("count2")
with Path(out_path).open("a") as handle:
    handle.write(json.dumps({"case": case, "ms": marks}) + "\n")
