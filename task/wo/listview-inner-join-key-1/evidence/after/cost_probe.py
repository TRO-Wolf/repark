from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any

ROWS = 200_000
DISTINCT = 50_000
EXECUTIONS = 5
BUILDS = 2_000
WARMUP = 2
KEYS = {
    "list": "array(id % {d}, id % 3)",
    "listview": "arrow_cast(array(id % {d}, id % 3), 'ListView(Int64)')",
}


def timed(thunk: Any) -> float:
    start = time.perf_counter()
    thunk()
    return time.perf_counter() - start


def frames(spark: Any, kind: str) -> tuple[Any, Any]:
    key = KEYS[kind].format(d=DISTINCT)
    spark.range(ROWS).selectExpr("id AS lid", f"{key} AS k").createOrReplaceTempView("cost_l")
    right = spark.range(ROWS).selectExpr("id + 1000000 AS rid", f"{key} AS k")
    right.createOrReplaceTempView("cost_r")
    return spark.table("cost_l"), spark.table("cost_r")


def aggregate_join(left: Any, right: Any) -> list[list[Any]]:
    from repark.spark import functions

    joined = left.join(right, "k", "inner")
    counted = joined.agg(functions.count(functions.lit(1)), functions.sum("lid"))
    return [list(row) for row in counted.collect()]


def build_joins(left: Any, right: Any) -> None:
    for _ in range(BUILDS):
        _ = left.join(right, "k", "inner").columns


def execute(spark: Any, kind: str) -> dict[str, Any]:
    left, right = frames(spark, kind)
    rows: list[list[Any]] = []
    for _ in range(WARMUP):
        rows = aggregate_join(left, right)
    seconds = sum(timed(lambda: aggregate_join(left, right)) for _ in range(EXECUTIONS))
    digest = hashlib.sha256(json.dumps(rows).encode()).hexdigest()[:16]
    return {"executions": EXECUTIONS, "seconds": seconds, "rows": rows, "sha": digest}


def build(spark: Any, kind: str) -> dict[str, Any]:
    left, right = frames(spark, kind)
    for _ in range(WARMUP):
        build_joins(left, right)
    return {"builds": BUILDS, "seconds": timed(lambda: build_joins(left, right))}


def measure(args: argparse.Namespace) -> None:
    from repark import ReparkSession

    spark = ReparkSession.builder.appName("listview-cost").getOrCreate()
    case = execute if args.case == "execute" else build
    record = {"case": args.case, "kind": args.kind, "leg": args.leg, "run": args.run}
    record.update(case(spark, args.kind))
    spark.stop()
    sys.stdout.write(json.dumps(record) + "\n")


def report(path: Path) -> None:
    groups: dict[tuple[str, str, str], list[float]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        key = (record["case"], record["kind"], record["leg"])
        groups.setdefault(key, []).append(record["seconds"])
    for key, values in sorted(groups.items()):
        sys.stdout.write(f"{key} median={statistics.median(values):.4f} runs={values}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=["execute", "build"])
    parser.add_argument("--kind", choices=sorted(KEYS))
    parser.add_argument("--leg", default="")
    parser.add_argument("--run", type=int, default=0)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.report is not None:
        report(args.report)
        return
    measure(args)


if __name__ == "__main__":
    main()
