from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
STEP0 = HERE / "step0"
WIDTH = 300
COMPARISONS = 50
EXPLAINS = 200
WARMUP = 10


def sql_cells() -> list[dict[str, Any]]:
    cells = [
        json.loads(line)
        for line in (STEP0 / "cells.jsonl").read_text(encoding="utf-8").splitlines()
        if line
    ]
    return [cell for cell in cells if cell["door"] == "sql"]


def statements(runner: Any, cells: list[dict[str, Any]]) -> dict[str, Any]:
    spent = 0.0
    kinds: dict[str, int] = {}
    for cell in cells:
        stmt = cell["stmt"].replace("{W}", runner.wide)
        if "{T}" in stmt:
            stmt = stmt.replace("{T}", runner.fresh())
        if cell["ansi"] == "off":
            runner.spark.conf.set("spark.sql.ansi.enabled", "false")
        if cell["case"] == "sensitive":
            runner.spark.conf.set("spark.sql.caseSensitive", "true")
        start = time.perf_counter()
        result = runner.execute(cell, stmt)
        spent += time.perf_counter() - start
        runner.spark.conf.set("spark.sql.ansi.enabled", "true")
        runner.spark.conf.set("spark.sql.caseSensitive", "false")
        kinds[result["kind"]] = kinds.get(result["kind"], 0) + 1
    return {"cells": len(cells), "seconds": spent, "kinds": kinds}


def planning(spark: Any) -> dict[str, Any]:
    columns = ", ".join(f"{index} AS c{index}" for index in range(WIDTH))
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW wide_cost AS SELECT {columns}")
    bare = [f"c{index}" for index in range(COMPARISONS, WIDTH)]
    compared = [f"(c{index} = c{index + 1}) AS k{index}" for index in range(COMPARISONS)]
    query = f"EXPLAIN SELECT {', '.join(compared + bare)} FROM wide_cost"
    for _ in range(WARMUP):
        spark.sql(query).collect()
    start = time.perf_counter()
    for _ in range(EXPLAINS):
        spark.sql(query).collect()
    spent = time.perf_counter() - start
    return {"explains": EXPLAINS, "seconds": spent, "output_columns": WIDTH}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=("a", "b"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()
    sys.path.insert(0, str(STEP0))
    import drive_cmp

    import repark
    from repark.spark import functions

    spark = drive_cmp.repark_session(arguments.warehouse)
    spark.conf.set("spark.sql.ansi.enabled", "true")
    if arguments.case == "a":
        runner = drive_cmp.Runner(spark, functions, "hd.sd")
        body = statements(runner, sql_cells())
    else:
        body = planning(spark)
    record = {"case": arguments.case, "label": arguments.label, **body}
    print(repark.__file__, file=sys.stderr)
    with Path(arguments.out).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    print(json.dumps(record, sort_keys=True))


if __name__ == "__main__":
    main()
