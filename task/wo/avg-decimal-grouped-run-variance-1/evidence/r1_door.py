import json
import os
import sys
from pathlib import Path

import pyarrow.parquet as pq
import r1_cells

import repark._native as native
from repark import ReparkSession
from repark.spark import functions
from repark.spark.session import _reset_active_session_for_tests

M = "99999999999999999999999999999999999999"
POS = f"CAST('{M}' AS DECIMAL(38,0))"
NEG = f"CAST('-{M}' AS DECIMAL(38,0))"
MAKE = [
    "CREATE NAMESPACE IF NOT EXISTS lc.ns",
    "CREATE TABLE lc.ns.r4 USING iceberg TBLPROPERTIES ('format-version'='2',"
    "'write.delete.mode'='merge-on-read') AS SELECT g, id, c38_0 FROM t4",
    f"INSERT INTO lc.ns.r4 VALUES (26, 900, {POS}), (26, 901, {POS})",
    f"INSERT INTO lc.ns.r4 VALUES (26, 902, {NEG}), (26, 903, CAST(5 AS DECIMAL(38,0)))",
    "DELETE FROM lc.ns.r4 WHERE id = 901",
]


def session_for(warehouse: Path, ansi: str) -> object:
    _reset_active_session_for_tests()
    return (
        ReparkSession.builder.appName("avg-r1-door")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(warehouse))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )


def make(session: object, warehouse: Path) -> dict[str, dict[str, int]]:
    results = {}
    for i, statement in enumerate(MAKE):
        results[f"make/{i}"] = r1_cells.count(lambda s=statement: session.sql(s).collect(), 1)
    for path in sorted(warehouse.rglob("*.parquet")):
        columns = ",".join(pq.read_schema(path).names)
        results[f"file/{path.parent.name}/{path.name[-14:]}"] = {columns: 1}
    return results


def ice(session: object, runs: int) -> dict[tuple[int, str], dict[str, int]]:
    sql = {
        "ice/sql/g/avg": "SELECT g, avg(c38_0) FROM lc.ns.r4 GROUP BY g",
        "ice/sql/g/try_avg": "SELECT g, try_avg(c38_0) FROM lc.ns.r4 GROUP BY g",
        "ice/sql/u26/avg": "SELECT avg(c38_0) FROM lc.ns.r4 WHERE g = 26",
    }
    results = {}
    for tp in (1, 2, 4, 8):
        session.sql(f"SET datafusion.execution.target_partitions = {tp}").collect()
        for key, query in sql.items():
            results[(tp, key)] = r1_cells.count(lambda q=query: session.sql(q).collect(), runs)
        frame = session.table("lc.ns.r4").groupBy("g").agg(functions.avg("c38_0"))
        results[(tp, "ice/table/g/avg")] = r1_cells.count(frame.collect, runs)
    return results


def door(session: object, runs: int) -> dict[str, dict[str, int]]:
    frames = {
        "big/table/g/avg": lambda: session.table("big").groupBy("g").agg(functions.avg("c")),
        "big/table/g/try_avg": lambda: (
            session.table("big").groupBy("g").agg(functions.try_avg("c"))
        ),
        "t4/table/g/avg": lambda: session.table("t4").groupBy("g").agg(functions.avg("c38_0")),
        "big/table/u/avg": lambda: session.table("big").agg(functions.avg("c")),
    }
    results = {}
    for key, frame in frames.items():
        results[key] = r1_cells.count(lambda frame=frame: frame().collect(), runs)
    return results


def main() -> None:
    out_path, phase, tp = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
    ansi, runs = sys.argv[4], int(sys.argv[5])
    step0, warehouse = Path(sys.argv[6]), Path(sys.argv[7])
    cpus = len(os.sched_getaffinity(0))
    print("native", native.__file__, "cpus", cpus, "phase", phase, "tp", tp, ansi, flush=True)
    session = session_for(warehouse, ansi)
    session.sql(f"SET datafusion.execution.target_partitions = {tp}").collect()
    for name in ("big", "t4"):
        session.read.parquet(str(step0 / name)).createOrReplaceTempView(name)
    if phase == "ice":
        results = {(tp, k): v for k, v in make(session, warehouse).items()}
        results |= ice(session, runs)
    else:
        results = {(tp, k): v for k, v in door(session, runs).items()}
    session.stop()
    _reset_active_session_for_tests()
    build = native.__file__.split("/")[-3]
    base = {"engine": "repark", "build": build, "cpus": cpus, "ansi": ansi}
    with out_path.open("a") as out:
        for (setting, key), answers in results.items():
            row = base | {"tp": setting, "query": key, "answers": answers}
            out.write(json.dumps(row) + "\n")
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
