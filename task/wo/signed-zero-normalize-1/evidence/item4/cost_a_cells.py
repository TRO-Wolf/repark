import argparse
import importlib.util
import json
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("cost_probe", HERE / "cost_probe.py")
if SPEC is None or SPEC.loader is None:
    raise SystemExit("cost_probe.py not found beside this script")
cost_probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cost_probe)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()
    spark = cost_probe.session(arguments.warehouse)
    text = cost_probe.READS.read_text(encoding="utf-8")
    records = [json.loads(line) for line in text.splitlines() if line]
    for record in records:
        if record.get("record") == "setup" and record["door"] == "spark-sql":
            spark.sql(record["text"]).collect()
    cells = [r for r in records if r.get("record") == "cell" and r["route"] == "spark-sql"]
    times = []
    for index, cell in enumerate(cells):
        start = time.perf_counter()
        answered = True
        try:
            spark.sql(cell["text"]).collect()
        except Exception:
            answered = False
        times.append((index, answered, time.perf_counter() - start))
    with Path(arguments.out).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"label": arguments.label, "cells": times}) + "\n")


if __name__ == "__main__":
    main()
