import json
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

case, label, data, warehouse, out = sys.argv[1:6]

s = (
    ReparkSession.builder.appName("ddu-cost")
    .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", warehouse)
    .getOrCreate()
)
s.read.parquet(f"{data}/big").createOrReplaceTempView("t")
s.read.parquet(f"{data}/mid").createOrReplaceTempView("m")
s.sql(
    "SELECT 1 AS id, CAST(0.5 AS DECIMAL(10,2)) AS d, CAST(0.25 AS DOUBLE) AS v"
).createOrReplaceTempView("one")
ROWS = {
    "filter_eq_d10": ("SELECT count(*) FROM t WHERE d10 = v", 100_000_000),
    "filter_lt_d10": ("SELECT count(*) FROM t WHERE d10 < v", 100_000_000),
    "filter_lt_d38": ("SELECT count(*) FROM t WHERE d38 < v", 100_000_000),
    "union_d10_v": (
        "SELECT sum(c) FROM (SELECT d10 AS c FROM t UNION ALL SELECT v FROM t)",
        200_000_000,
    ),
    "cast_v_d10": ("SELECT max(CAST(v AS DECIMAL(10,2))) FROM t", 100_000_000),
    "cast_v_d38": ("SELECT max(CAST(v AS DECIMAL(38,18))) FROM t", 100_000_000),
    "case30": (
        "SELECT sum("
        + "".join(f"CASE WHEN id % 50 = {i} THEN d10 ELSE " for i in range(30))
        + "v"
        + " END" * 30
        + ") FROM t",
        100_000_000,
    ),
    "case50_flat": (
        "SELECT sum(CASE "
        + "".join(f"WHEN id % 50 = {i} THEN d10 " for i in range(50))
        + "ELSE v END) FROM t",
        100_000_000,
    ),
    "plan_free200": (
        "SELECT " + ", ".join(f"id + {i} AS c{i}" for i in range(200)) + " FROM one",
        0,
    ),
    "plan_wide_dd": (
        "SELECT "
        + ", ".join(f"d + {i} AS c{i}, v * {i} AS w{i}" for i in range(100))
        + " FROM one",
        0,
    ),
    "plan_union200": (
        " UNION ALL ".join(
            ("SELECT d AS c FROM one" if i % 2 else "SELECT v AS c FROM one") for i in range(200)
        ),
        0,
    ),
    "plan_values10k": (
        "SELECT count(*) FROM VALUES "
        + ", ".join(f"({i}, 0.5)" for i in range(9999))
        + ", (9999, CAST(0.25 AS DOUBLE)) AS x(a, b)",
        0,
    ),
    "plan_proj1000": (
        "SELECT "
        + ", ".join(("d" if i % 2 else "v") + f" AS c{i}" for i in range(1000))
        + " FROM one",
        0,
    ),
}
records = []
if case.startswith("plan_"):
    sql, _ = ROWS[case]
    reps = 20
    start = time.perf_counter()
    for _ in range(reps):
        s.sql(sql).collect()
    elapsed = time.perf_counter() - start
    records.append({"case": case, "label": label, "seconds": elapsed, "values": 0, "reps": reps})
elif case in ROWS:
    sql, values = ROWS[case]
    s.sql(sql).collect()
    start = time.perf_counter()
    got = s.sql(sql).collect()
    elapsed = time.perf_counter() - start
    records.append(
        {
            "case": case,
            "label": label,
            "seconds": elapsed,
            "values": values,
            "answer": str(got[0][0]),
        }
    )
elif case in ("prune_v_declit", "prune_d_dbl", "delete_d_v"):
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    s.sql("CREATE TABLE lc.ns.p (id BIGINT, d10 DECIMAL(10,2), v DOUBLE) USING iceberg").collect()
    for part in range(4):
        s.sql(f"INSERT INTO lc.ns.p SELECT id, d10, v FROM m WHERE id % 4 = {part}").collect()
    sql = {
        "prune_v_declit": "SELECT count(*) FROM lc.ns.p WHERE v = 0.5",
        "prune_d_dbl": "SELECT count(*) FROM lc.ns.p WHERE d10 = CAST(0.5 AS DOUBLE)",
        "delete_d_v": "DELETE FROM lc.ns.p WHERE d10 = v",
    }[case]
    start = time.perf_counter()
    got = s.sql(sql).collect()
    elapsed = time.perf_counter() - start
    plan = "" if case == "delete_d_v" else str(s.sql("EXPLAIN " + sql).collect()[-1][-1])[:2000]
    records.append(
        {
            "case": case,
            "label": label,
            "seconds": elapsed,
            "values": 20_000_000,
            "answer": str(list(got[0])) if got else "",
            "plan": plan,
        }
    )
with Path(out).open("a") as fh:
    for r in records:
        r["native"] = "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1]
        fh.write(json.dumps(r) + "\n")
