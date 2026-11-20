import json
import resource
import sys
import time
from pathlib import Path
from typing import Any

import asof
from common import modules

KEYS = 200_000


def session(engine: str) -> Any:
    if engine == "spark":
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[16]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "16g")
            .config("spark.sql.shuffle.partitions", "16")
        )
    else:
        import repark._native as native
        from repark import ReparkSession

        print("NATIVE", native.__file__, flush=True)
        builder = ReparkSession.builder.appName("asof-cost")
    s = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    if engine == "spark":
        s.sparkContext.setLogLevel("FATAL")
    return s


def frames(s: Any, f: Any, n: int, keyed: bool) -> tuple[Any, Any]:
    key = (f.col("id") % KEYS) if keyed else f.lit(0)
    left = s.range(n).select(
        f.col("id").cast("int").alias("lid"),
        key.cast("string").alias("k1"),
        (f.col("id") * 3).alias("t"),
        f.col("id").alias("lv"),
    )
    right = s.range(n).select(
        f.col("id").cast("int").alias("rid"),
        key.cast("string").alias("k1"),
        (f.col("id") * 3 + 1).alias("t"),
        f.col("id").alias("rv"),
    )
    return left, right


def build(engine: str, surf: str, s: Any, n: int, keyed: bool) -> Any:
    f, win = modules(engine)
    left, right = frames(s, f, n, keyed)
    keys = ["k1"] if keyed else []
    tol = None if keyed else f.lit(2)
    if surf == "D":
        out = asof.direct(left, right, "k1" if keyed else None, "left", tol, True, "backward")
    elif surf == "W1":
        out = asof.w1(f, win, left, right, keys, "left", tol, True, "backward")
    else:
        out = asof.w2(f, win, left, right, keys, "left", tol, True, "backward")
    return out.agg(f.count(f.lit(1)).alias("c"), f.sum("rid").alias("s"))


def peak(engine: str, s: Any) -> int:
    if engine != "spark":
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    pid = s.sparkContext._jvm.java.lang.ProcessHandle.current().pid()
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM"):
            return int(line.split()[1])
    return -1


def main() -> None:
    engine, surf, n, keyed, reps, out = sys.argv[1:7]
    s = session(engine)
    record: dict[str, Any] = {
        "engine": engine,
        "surf": surf,
        "n": int(n),
        "keyed": keyed == "keys",
        "times": [],
    }
    try:
        for _ in range(int(reps)):
            frame = build(engine, surf, s, int(n), keyed == "keys")
            start = time.perf_counter()
            rows = frame.collect()
            record["times"].append(round(time.perf_counter() - start, 3))
            record["result"] = [str(v) for v in rows[0]]
    except Exception as error:
        record["error"] = type(error).__name__ + " " + " ".join(str(error).split())[:300]
    record["peak_rss_kb"] = peak(engine, s)
    with Path(out).open("a") as handle:
        handle.write(json.dumps(record) + "\n")
    print(json.dumps(record), flush=True)


if __name__ == "__main__":
    main()
