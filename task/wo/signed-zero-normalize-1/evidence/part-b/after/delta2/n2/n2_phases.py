from __future__ import annotations

import argparse
import functools
import importlib.util
import json
import statistics
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
COST = HERE.parents[2] / "cost"
SPEC = importlib.util.spec_from_file_location("cost_probe", COST / "cost_probe.py")
assert SPEC is not None and SPEC.loader is not None
COST_PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COST_PROBE)


def package_of(module: Any) -> str:
    return Path(module._native.__file__).resolve().parents[1].name


def timed(work: Any) -> tuple[float, bool]:
    start = time.perf_counter()
    try:
        work()
    except Exception:
        return time.perf_counter() - start, False
    return time.perf_counter() - start, True


def passes(spark: Any, text: str) -> int | None:
    try:
        table = spark.sql("EXPLAIN VERBOSE " + text).toArrow()
    except Exception:
        return None
    labels = [str(value) for column in table.columns for value in column.to_pylist()]
    return sum(1 for label in labels if label.endswith("after simplify_expressions"))


def collect_after(frames: list[Any]) -> None:
    frames[0].collect()


def plan_into(spark: Any, text: str, frames: list[Any]) -> None:
    frames.append(spark.sql(text))


def measure(spark: Any, text: str, repeats: int) -> dict[str, Any]:
    timed(lambda: spark.sql(text).collect())
    plan, explain, total = [], [], []
    answered = True
    for _ in range(repeats):
        frames: list[Any] = []
        seconds, ok = timed(functools.partial(plan_into, spark, text, frames))
        plan.append(seconds)
        if ok:
            seconds, ok = timed(functools.partial(collect_after, frames))
            total.append(plan[-1] + seconds)
        answered = answered and ok
        explain.append(timed(lambda: spark.sql("EXPLAIN " + text).collect())[0])
    return {
        "sql": statistics.median(plan),
        "explain": statistics.median(explain),
        "total": statistics.median(total) if total else None,
        "answered": answered,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--repeats", type=int, default=5)
    arguments = parser.parse_args()
    import repark

    spark = COST_PROBE.session(arguments.warehouse)
    records = [
        json.loads(line)
        for line in COST_PROBE.READS.read_text(encoding="utf-8").splitlines()
        if line
    ]
    for record in records:
        if record.get("record") == "setup" and record["door"] == "spark-sql":
            spark.sql(record["text"]).collect()
    cells = [r for r in records if r.get("record") == "cell" and r["route"] == "spark-sql"]
    with Path(arguments.out).open("w", encoding="utf-8") as handle:
        handle.write(json.dumps({"label": arguments.label, "package": package_of(repark)}) + "\n")
        for cell in cells:
            row = {"cid": cell["cid"], **measure(spark, cell["text"], arguments.repeats)}
            row["passes"] = passes(spark, cell["text"])
            handle.write(json.dumps(row) + "\n")


if __name__ == "__main__":
    main()
