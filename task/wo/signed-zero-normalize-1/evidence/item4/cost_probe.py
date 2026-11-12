from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
READS = HERE.parent / "item1" / "raw" / "repark-68b09d18-reads.jsonl"
WIDTH = 300
COMPARISONS = 50
EXPLAINS = 200
WARMUP = 10
OPERATORS = ("=", "<>", "<", "<=", ">", ">=", "<=>")
ZEROS = ("0.0", "-0.0", "0", "CAST('-0.0' AS DOUBLE)")


def session(warehouse: str) -> Any:
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("signed-zero-cost")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", warehouse)
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    return spark


def statements(spark: Any) -> dict[str, Any]:
    records = [json.loads(line) for line in READS.read_text(encoding="utf-8").splitlines() if line]
    for record in records:
        if record.get("record") == "setup" and record["door"] == "spark-sql":
            spark.sql(record["text"]).collect()
    cells = [r for r in records if r.get("record") == "cell" and r["route"] == "spark-sql"]
    spent = 0.0
    answered = 0
    for cell in cells:
        start = time.perf_counter()
        try:
            spark.sql(cell["text"]).collect()
            answered += 1
        except Exception:
            pass
        spent += time.perf_counter() - start
    return {"cells": len(cells), "answered": answered, "seconds": spent}


def planning(spark: Any) -> dict[str, Any]:
    columns = ", ".join(f"CAST({index} AS DOUBLE) AS c{index}" for index in range(WIDTH))
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW wide_cost AS SELECT {columns}").collect()
    compared = [
        f"(c{index} {OPERATORS[index % len(OPERATORS)]} {ZEROS[index % len(ZEROS)]}) AS k{index}"
        for index in range(COMPARISONS)
    ]
    bare = [f"c{index}" for index in range(COMPARISONS, WIDTH)]
    query = f"EXPLAIN SELECT {', '.join(compared + bare)} FROM wide_cost"
    for _ in range(WARMUP):
        spark.sql(query).collect()
    start = time.perf_counter()
    for _ in range(EXPLAINS):
        spark.sql(query).collect()
    return {"explains": EXPLAINS, "seconds": time.perf_counter() - start, "columns": WIDTH}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=("a", "b"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()
    spark = session(arguments.warehouse)
    result = statements(spark) if arguments.case == "a" else planning(spark)
    record = {"case": arguments.case, "label": arguments.label, **result}
    with Path(arguments.out).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    print(json.dumps(record, sort_keys=True))


if __name__ == "__main__":
    main()
