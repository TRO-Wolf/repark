from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
STEP0 = HERE / "step0"
UNIT_1292 = HERE.parents[1] / "struct-compare-by-position-1" / "evidence"
TABLES = 20
EXPLAINS = 200
WARMUP = 10


def session(warehouse: str) -> Any:
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("family-cost")
        .config("spark.sql.session.timeZone", "UTC")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", warehouse)
        .getOrCreate()
    )


def timed(thunk: Any) -> float:
    start = time.perf_counter()
    thunk()
    return time.perf_counter() - start


def statements(spark: Any) -> dict[str, Any]:
    import probe_family as pf
    import probe_family_add as pa

    from repark.spark import functions

    for _name, statement in pf.view_statements("head"):
        spark.sql(statement).collect()
    queries = [cell for cell in pf.gen_query_cells() + pa.gen_add_cells() if cell["door"] == "sql"]
    spent = 0.0
    for cell in queries:
        sensitive = "true" if cell["case"] == "sensitive" else "false"
        spark.conf.set("spark.sql.caseSensitive", sensitive)
        spent += timed(lambda cell=cell: pf.run_query(spark, functions, cell, "head"))
    spark.conf.set("spark.sql.caseSensitive", "false")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.vp").collect()
    writes = pf.gen_dml_cells() + pa.gen_add_dml_cells()
    for cell in writes:
        spent += timed(lambda cell=cell: pf.run_dml(spark, cell))
    return {"queries": len(queries), "writes": len(writes), "seconds": spent}


def using_joins(spark: Any) -> dict[str, Any]:
    for index in range(TABLES):
        spark.sql(
            f"CREATE OR REPLACE TEMPORARY VIEW uj{index} AS "
            f"SELECT 1 AS a{index}, 1 AS a{index + 1}, {index} AS v{index}"
        ).collect()
    joins = " ".join(f"JOIN uj{index} USING (a{index})" for index in range(1, TABLES))
    query = f"EXPLAIN SELECT * FROM uj0 {joins}"
    for _ in range(WARMUP):
        spark.sql(query).collect()
    spent = sum(timed(lambda: spark.sql(query).collect()) for _ in range(EXPLAINS))
    return {"explains": EXPLAINS, "seconds": spent, "tables": TABLES}


def planning_1292(spark: Any) -> dict[str, Any]:
    spec = importlib.util.spec_from_file_location("cost_1292", UNIT_1292 / "cost_probe.py")
    if spec is None or spec.loader is None:
        raise SystemExit("the #1292 cost probe is missing")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    spark.conf.set("spark.sql.ansi.enabled", "true")
    return module.planning(spark)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=("a", "b", "c"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", required=True)
    arguments = parser.parse_args()
    sys.path.insert(0, str(STEP0))
    import repark

    spark = session(arguments.warehouse)
    if arguments.case == "a":
        body = statements(spark)
    elif arguments.case == "b":
        body = planning_1292(spark)
    else:
        body = using_joins(spark)
    spark.stop()
    record = {"case": arguments.case, "label": arguments.label, **body}
    sys.stderr.write(f"{repark.__file__}\n")
    with Path(arguments.out).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    sys.stdout.write(json.dumps(record, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
