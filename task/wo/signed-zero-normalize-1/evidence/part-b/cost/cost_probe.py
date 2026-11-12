from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
READS = HERE.parent / "after" / "raw" / "repark-4f2108ae-reads.jsonl"
PART_A_READS = HERE.parent.parent / "item1" / "raw" / "repark-68b09d18-reads.jsonl"
WIDTH = 300
VALUES = 20
EXPLAINS = 200
WARMUP = 10
ROWS = 10_000_000
BUILD_ROWS = 1000
REPEAT = 5


def session(warehouse: str) -> Any:
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("signed-zero-keys-cost")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", warehouse)
    spark.sql("CREATE NAMESPACE ice.ns").collect()
    return spark


def native() -> Any:
    import repark
    from repark._native import PyReparkSession

    door = PyReparkSession.native(config={"repark.sql.allowCreateFormatVersion3": "true"})
    repark._ANSI_NATIVE = door
    return repark


def to_rows(frame: Any) -> list[list[Any]]:
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    columns = [table.column(index).to_pylist() for index in range(table.num_columns)]
    return [list(values) for values in zip(*columns, strict=True)] if columns else []


def wide_plan(spark: Any) -> dict[str, Any]:
    keys = [f"CAST({index} AS BIGINT) AS c{index}" for index in range(WIDTH - 10)]
    texts = [f"CAST('s{index}' AS STRING) AS s{index}" for index in range(10)]
    values = [f"CAST({index}.5 AS DOUBLE) AS v{index}" for index in range(VALUES)]
    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW wide_cost AS SELECT " + ", ".join(keys + texts + values)
    ).collect()
    query = (
        "EXPLAIN SELECT a.c0, a.s0, a.ik, a.cnt, a.sv, b.* FROM (SELECT c0, s0, "
        "CAST(c1 AS DOUBLE) / CAST(c2 AS DOUBLE) AS ik, count(*) AS cnt, "
        "sum(v0) AS sv, max(v1) AS mv FROM wide_cost "
        "GROUP BY c0, s0, CAST(c1 AS DOUBLE) / CAST(c2 AS DOUBLE)) a JOIN (SELECT *, "
        "row_number() OVER (PARTITION BY s1 ORDER BY c3) AS rn FROM wide_cost) b "
        "ON a.c0 = b.c0 AND a.s0 = b.s0 ORDER BY a.c0, a.s0, b.c4"
    )
    for _ in range(WARMUP):
        spark.sql(query).collect()
    start = time.perf_counter()
    for _ in range(EXPLAINS):
        spark.sql(query).collect()
    return {"explains": EXPLAINS, "seconds": time.perf_counter() - start, "columns": WIDTH + VALUES}


def statements(spark: Any, path: Path) -> dict[str, Any]:
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
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


def float_source(signed: bool) -> str:
    special = "CAST('-0.0' AS DOUBLE)" if signed else "CAST('NaN' AS DOUBLE)"
    source = f"range(0, {ROWS})"
    build = f"range(0, {BUILD_ROWS})"
    return (
        f"WITH f AS (SELECT id, CASE WHEN id % 4 = 0 THEN {special} "
        f"ELSE CAST(id % 99999 AS DOUBLE) END AS d FROM {source} s), "
        f"b AS (SELECT CAST(id + 1 AS DOUBLE) AS d FROM {build} r) "
    )


FLOAT_STATEMENTS = {
    "group": "SELECT count(*) AS n FROM (SELECT d, count(*) AS c FROM f GROUP BY d) g",
    "join": "SELECT count(*) AS n FROM f JOIN b ON f.d = b.d",
    "window": "SELECT sum(c) AS n FROM (SELECT count(*) OVER (PARTITION BY d) AS c FROM f) w",
    "distinct": "SELECT count(*) AS n FROM (SELECT DISTINCT d FROM f) x",
    "topk": "SELECT d FROM f ORDER BY d LIMIT 10",
}


def float_keys(signed: bool, warehouse: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    doors = {"spark": session(warehouse), "native": native()}
    for door, handle in doors.items():
        prefix = float_source(signed)
        for name, text in FLOAT_STATEMENTS.items():
            sql = prefix + text
            to_rows(handle.sql(sql))
            start = time.perf_counter()
            for _ in range(REPEAT):
                rows = to_rows(handle.sql(sql))
            out[f"{door}:{name}"] = {
                "seconds": time.perf_counter() - start,
                "answer": rows[0][0] if name != "topk" else len(rows),
            }
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=("n1", "n2", "a", "f1", "f2"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()
    import repark

    record: dict[str, Any] = {
        "case": arguments.case,
        "label": arguments.label,
        "module": str(Path(repark.__file__).resolve().parent.name),
        "load1": os.getloadavg()[0],
    }
    if arguments.case == "n1":
        record.update(wide_plan(session(arguments.warehouse)))
    elif arguments.case == "n2":
        record.update(statements(session(arguments.warehouse), READS))
    elif arguments.case == "a":
        record.update(statements(session(arguments.warehouse), PART_A_READS))
    else:
        record["statements"] = float_keys(arguments.case == "f1", arguments.warehouse)
    with Path(arguments.out).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    print(json.dumps(record, sort_keys=True))


if __name__ == "__main__":
    main()
