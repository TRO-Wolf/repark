import json
import re
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

case, label, data, warehouse, out = sys.argv[1:6]
builder = (
    ReparkSession.builder.appName("cu-cost")
    .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", warehouse)
)
s = builder.getOrCreate()
s.read.parquet(f"{data}/c2").createOrReplaceTempView("c2")
s.read.parquet(f"{data}/c3src").createOrReplaceTempView("src")
s.sql("SELECT 1 AS id, CAST(0.5 AS DECIMAL(10,2)) AS d").createOrReplaceTempView("one")
MODES = {
    "cow": "'format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'",
    "mor": "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'",
}
FILTERS = {
    "eq": "CAST(c AS INT) = 1",
    "eq_hand": "c >= 1 AND c < 2",
    "lt": "CAST(c AS INT) < 1",
    "lt_hand": "c < 1",
}
REPS = 10
records = []


def emit(name: str, seconds: float, reps: int = 1, **extra: object) -> None:
    records.append({"case": name, "label": label, "seconds": seconds, "reps": reps, **extra})


def repeat(sql: str, reps: int = REPS) -> tuple:
    got = s.sql(sql).collect()
    start = time.perf_counter()
    for _ in range(reps):
        got = s.sql(sql).collect()
    return time.perf_counter() - start, got


def iceberg(mode: str, name: str) -> str:
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    table = f"lc.ns.{name}_{mode}"
    s.sql(
        f"CREATE TABLE {table} (id BIGINT, c DECIMAL(10,2), b BIGINT, s DECIMAL(10,2)) "
        f"USING iceberg TBLPROPERTIES ({MODES[mode]})"
    ).collect()
    for part in range(100):
        lo, hi = part * 100_000, (part + 1) * 100_000
        s.sql(
            f"INSERT INTO {table} SELECT id, c, b, s FROM src WHERE id >= {lo} AND id < {hi}"
        ).collect()
    if mode == "mor":
        s.sql(f"DELETE FROM {table} WHERE id % 1000 = 7").collect()
    return table


def scan_tasks(sql: str) -> list:
    text = " ".join(" ".join(str(r[-1]).split()) for r in s.sql("EXPLAIN ANALYZE " + sql).collect())
    return re.findall(r"IcebergTableScan projection:\[[^\]]*\] predicate:\[[^\]]*\] N=(\d+)", text)


def pruning(sql: str) -> dict:
    text = " ".join(" ".join(str(r[-1]).split()) for r in s.sql("EXPLAIN ANALYZE " + sql).collect())
    return {
        k: re.findall(rf"{k}=([0-9]+ total → [0-9]+ matched|[0-9]+)", text)
        for k in (
            "files_ranges_pruned_statistics",
            "row_groups_pruned_statistics",
            "page_index_rows_pruned",
        )
    }


if case == "plan_pred200":
    sql = "SELECT id FROM one WHERE " + " AND ".join(f"id <> {i}" for i in range(2, 202))
    secs, _ = repeat(sql, 20)
    emit(case, secs, 20)
elif case == "c2":
    for f, cond in FILTERS.items():
        sql = f"SELECT count(*), sum(id) FROM c2 WHERE {cond}"
        secs, got = repeat(sql)
        emit(f"c2_{f}", secs, REPS, answer=str(list(got[0])), pruning=pruning(sql))
elif case == "c4":
    for store in ("pq", "ice"):
        src = "c2" if store == "pq" else iceberg("cow", "c4")
        for which, cond in (("bigint", "CAST(b AS INT) = 1"), ("tiny", "CAST(s AS TINYINT) = 1")):
            secs, got = repeat(f"SELECT count(*) FROM {src} WHERE {cond}")
            emit(f"c4_{store}_{which}", secs, REPS, answer=str(list(got[0])))
elif case == "in40":
    sql = (
        "SELECT count(*) FROM c2 WHERE CAST(c AS INT) IN ("
        + ", ".join(str(k) for k in range(1, 41))
        + ")"
    )
    secs, got = repeat(sql)
    plan = " ".join(str(r[-1]) for r in s.sql("EXPLAIN " + sql).collect())
    emit(case, secs, REPS, answer=str(list(got[0])), per_row="__repark_cast_shield__" in plan)
elif case.startswith("c3_"):
    mode = case[3:]
    table = iceberg(mode, "t")
    for f, cond in FILTERS.items():
        sql = f"SELECT count(*), sum(id) FROM {table} WHERE {cond}"
        secs, got = repeat(sql)
        emit(f"c3_{mode}_{f}", secs, REPS, answer=str(list(got[0])), tasks=scan_tasks(sql))
elif case.startswith("del_"):
    mode = case[4:]
    for f in ("eq", "eq_hand"):
        table = iceberg(mode, f"d{f}")
        start = time.perf_counter()
        s.sql(f"DELETE FROM {table} WHERE {FILTERS[f]}").collect()
        secs = time.perf_counter() - start
        got = s.sql(f"SELECT count(*), sum(id) FROM {table}").collect()
        emit(f"del_{mode}_{f}", secs, 1, answer=str(list(got[0])))
with Path(out).open("a") as fh:
    for r in records:
        r["native"] = "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1]
        fh.write(json.dumps(r, ensure_ascii=False) + "\n")
