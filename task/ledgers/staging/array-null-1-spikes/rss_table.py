import json
import subprocess
import sys

_WORKER = """
import json
import resource
import sys

depth = int(sys.argv[1])
mode = sys.argv[2]
do_collect = int(sys.argv[3])

import numpy
import pyarrow
from repark import ReparkSession
from repark.spark import functions as F

spark = ReparkSession.builder.appName("array-null-1-rss").getOrCreate()
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
if mode == "flat":
    frame.select(
        *[F.array_append(F.col("a"), F.lit(i)).alias(f"c{i}") for i in range(depth)]
    ).collect()
    print("JSON" + json.dumps({"delta": peak_rss_bytes() - before}), flush=True)
    sys.exit(0)
builder = F.array_append if mode == "append" else F.array_prepend
expression = F.col("a")
for level in range(1, depth + 1):
    expression = builder(expression, F.lit(level))
build_delta = peak_rss_bytes() - before
if do_collect:
    frame.select(expression.alias("c")).collect()
print("JSON" + json.dumps({
    "build_delta": build_delta,
    "delta": peak_rss_bytes() - before,
}), flush=True)
"""

for mode in ("append", "prepend", "flat"):
    for depth in (4, 8, 12, 14, 16, 40):
        if mode == "flat" and depth != 40:
            continue
        if mode != "flat" and depth == 40:
            continue
        do_collect = "0" if depth >= 16 and mode != "flat" else "1"
        completed = subprocess.run(
            [sys.executable, "-c", _WORKER, str(depth), mode, do_collect],
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
        tail = completed.stdout.strip().splitlines()
        if not tail or not tail[-1].startswith("JSON"):
            print(
                json.dumps(
                    {
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
        print(json.dumps({"mode": mode, "depth": depth, **result}), flush=True)
