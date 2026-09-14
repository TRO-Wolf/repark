import json
import os
import subprocess
import sys

_WORKER = """
import json
import os
import resource
import sys
import time

depth = int(sys.argv[1])
mode = sys.argv[2]

import numpy
import pyarrow
from repark import ReparkSession
from repark.spark import functions as F

spark = ReparkSession.builder.appName("array-null-1-route").getOrCreate()
frame = spark.createDataFrame([([1, 2],)], "a array<int>")
frame.select(F.col("a").alias("warm")).collect()


def vm_size_bytes():
    for line in open("/proc/self/status"):
        if line.startswith("VmSize:"):
            return int(line.split()[1]) * 1024
    return 0


def peak_rss_bytes():
    for line in open("/proc/self/status"):
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    return 0


resource.setrlimit(resource.RLIMIT_AS, (vm_size_bytes() + 3 * 8 * 1024**3,) * 2)

before = peak_rss_bytes()
builder = F.array_append if mode == "append" else F.array_prepend
build_start = time.monotonic()
expression = F.col("a")
for level in range(1, depth + 1):
    expression = builder(expression, F.lit(level))
build_wall = time.monotonic() - build_start
build_delta = peak_rss_bytes() - before
collect_start = time.monotonic()
frame.select(expression.alias("c")).collect()
collect_wall = time.monotonic() - collect_start
print("JSON" + json.dumps({
    "build_wall_s": round(build_wall, 4),
    "collect_wall_s": round(collect_wall, 4),
    "build_delta": build_delta,
    "delta": peak_rss_bytes() - before,
}), flush=True)
"""

route = sys.argv[1]
native = sys.argv[2]
env = dict(os.environ, REPARK_ARRAY_NULL_1_ROUTE=route)

for mode in ("append", "prepend"):
    for depth in (12, 40):
        try:
            completed = subprocess.run(
                [sys.executable, "-c", _WORKER, str(depth), mode],
                capture_output=True,
                text=True,
                timeout=900,
                check=False,
                env=env,
            )
        except subprocess.TimeoutExpired:
            print(
                json.dumps(
                    {
                        "route": route,
                        "native": native,
                        "mode": mode,
                        "depth": depth,
                        "timeout_s": 900,
                    }
                ),
                flush=True,
            )
            continue
        tail = completed.stdout.strip().splitlines()
        if not tail or not tail[-1].startswith("JSON"):
            print(
                json.dumps(
                    {
                        "route": route,
                        "native": native,
                        "mode": mode,
                        "depth": depth,
                        "died": completed.returncode,
                        "stderr": completed.stderr[-400:],
                    }
                ),
                flush=True,
            )
            continue
        result = json.loads(tail[-1][len("JSON") :])
        row = {"route": route, "native": native, "mode": mode, "depth": depth, **result}
        print(json.dumps(row), flush=True)
