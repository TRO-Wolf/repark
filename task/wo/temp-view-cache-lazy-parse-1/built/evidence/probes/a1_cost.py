import json
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

case, build, data, out_path = sys.argv[1:5]
ROWS = 2_000_000
SHAPE = ["n", "CAST(ts AS STRING) AS t", "hour(ts) AS h", "unix_timestamp(ts) AS u"]
print("NATIVE", native.__file__, flush=True)
json_dir = Path(data) / "json2m"
if not json_dir.exists():
    json_dir.mkdir(parents=True)
    with (json_dir / "a.json").open("w") as handle:
        for i in range(ROWS):
            handle.write(f'{{"n": {i}, "ts": "2024-01-01 {i % 24:02d}:{i % 60:02d}:00"}}\n')
s = (
    ReparkSession.builder.appName("cache-a1-cost")
    .config("spark.sql.session.timeZone", "America/Los_Angeles")
    .getOrCreate()
)
s.read.schema("n INT, ts TIMESTAMP").json(str(json_dir)).limit(1).collect()
marks: dict[str, float] = {}
start = time.perf_counter()


def mark(name: str) -> None:
    marks[name] = round((time.perf_counter() - start) * 1000, 2)


def frame():
    return s.read.schema("n INT, ts TIMESTAMP").json(str(json_dir)).selectExpr(*SHAPE)


if case == "count_collect":
    f = frame().cache()
    mark("build")
    f.count()
    mark("count")
    f.collect()
    mark("collect")
elif case == "collect_first":
    f = frame().cache()
    mark("build")
    f.collect()
    mark("collect")
    f.count()
    mark("count")
elif case == "view_count":
    f = frame().cache()
    f.createOrReplaceTempView("cv")
    mark("register")
    s.table("cv").count()
    mark("count")
elif case == "selective_child":
    f = frame().cache()
    mark("build")
    f.filter("n = 7").count()
    mark("child")
elif case == "second_child":
    f = frame().cache()
    f.filter("n = 7").count()
    mark("first")
    f.filter("n = 9").count()
    mark("second")
else:
    raise SystemExit("unknown case " + case)
with Path(out_path).open("a") as handle:
    handle.write(json.dumps({"case": case, "build": build, "ms": marks}) + "\n")
s.stop()
