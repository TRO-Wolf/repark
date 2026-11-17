import json
import re
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

case, label, data, warehouse, out = sys.argv[1:6]
s = (
    ReparkSession.builder.appName("dct-cost")
    .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", warehouse)
    .getOrCreate()
)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
s.sql(
    "SELECT 1 AS id, CAST(0.5 AS DECIMAL(10,2)) AS c10, CAST(0.25 AS DECIMAL(12,4)) AS c12, "
    "CAST(1.5 AS DECIMAL(18,2)) AS d18, CAST(1.25 AS DECIMAL(20,4)) AS d20, "
    "CAST(1 AS DECIMAL(38,0)) AS a38, CAST(1.1 AS DECIMAL(38,1)) AS d38, "
    "CAST(0.5 AS DECIMAL(38,38)) AS b38, CAST(0.75 AS DECIMAL(20,20)) AS f20"
).createOrReplaceTempView("one")
COW = (
    "'format-version'='2', 'write.delete.mode'='copy-on-write', "
    "'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write'"
)
PRED200 = " AND ".join(f"c10 <> c12 + {i}" for i in range(200))
PRED200_WIDE = " AND ".join(f"c10 <> a38 + {i}" for i in range(200))
UNION32 = " UNION ALL ".join(
    ("SELECT d18 AS v FROM one" if i % 2 else "SELECT d20 AS v FROM one") for i in range(32)
)
UNION32_WIDE = " UNION ALL ".join(
    ("SELECT c10 AS v FROM one" if i % 2 else "SELECT a38 AS v FROM one") for i in range(32)
)
CASE200 = (
    "SELECT CASE "
    + " ".join(f"WHEN id = {i} THEN {'c10' if i % 2 else 'c12'}" for i in range(200))
    + " ELSE c10 END AS v FROM one"
)
UNION5 = " UNION ALL ".join(
    f"SELECT {column} AS v FROM one" for column in ("c10", "a38", "d38", "f20", "d18")
)
VALUES10K_MIXED = (
    "SELECT count(*) FROM VALUES "
    + ", ".join(f"({i}, CAST('{i}' AS DECIMAL(38,0)))" for i in range(9999))
    + ", (9999, CAST('1.25' AS DECIMAL(10,2))) AS x(a, b)"
)
PLANS = {
    "k1_pred200": f"SELECT id FROM one WHERE {PRED200}",
    "k1_union32": UNION32,
    "k4_case200": CASE200,
    "k2_pred200_wide": f"SELECT id FROM one WHERE {PRED200_WIDE}",
    "k2_union32_wide": UNION32_WIDE,
    "r_union5": UNION5,
    "r_values10k_mixed": VALUES10K_MIXED,
}
records = []


def emit(name: str, seconds: float, reps: int, **extra: object) -> None:
    records.append({"case": name, "label": label, "seconds": seconds, "reps": reps, **extra})


def timed(sql: str, reps: int) -> tuple:
    got = s.sql(sql).collect()
    start = time.perf_counter()
    for _ in range(reps):
        got = s.sql(sql).collect()
    return time.perf_counter() - start, got


def analyzed(sql: str) -> str:
    return " ".join(" ".join(str(r[-1]).split()) for r in s.sql("EXPLAIN ANALYZE " + sql).collect())


if case in PLANS:
    reps = 3 if case == "r_values10k_mixed" else 20
    secs, got = timed(PLANS[case], reps)
    emit(case, secs, reps, answer=str(sorted(str(list(r)) for r in got))[:200])
elif case == "k5_dml":
    s.read.parquet(f"{data}/k5").createOrReplaceTempView("k5src")
    for kind in ("delete", "update"):
        total = 0.0
        reps = 5
        answers = []
        for rep in range(reps):
            table = f"lc.ns.k5_{kind}_{rep}"
            s.sql(
                f"CREATE TABLE {table} (id BIGINT, c10 DECIMAL(10,2), c12 DECIMAL(12,4), "
                f"tag STRING) USING iceberg TBLPROPERTIES ({COW})"
            ).collect()
            s.sql(f"INSERT INTO {table} SELECT id, c10, c12, 'o' FROM k5src").collect()
            statement = (
                f"DELETE FROM {table} WHERE {PRED200}"
                if kind == "delete"
                else f"UPDATE {table} SET tag = 'u' WHERE {PRED200}"
            )
            start = time.perf_counter()
            s.sql(statement).collect()
            total += time.perf_counter() - start
            answers.append(
                str(
                    list(
                        s.sql(f"SELECT count(*), count(tag = 'u' OR NULL) FROM {table}").collect()[
                            0
                        ]
                    )
                )
            )
        emit(f"k5_{kind}", total, reps, answer=answers[0])
elif case == "k3_prune":
    table = "lc.ns.k3"
    s.sql(
        f"CREATE TABLE {table} (id BIGINT, c DECIMAL(10,2)) USING iceberg PARTITIONED BY (c) "
        "TBLPROPERTIES ('format-version'='2')"
    ).collect()
    s.read.parquet(f"{data}/k3").createOrReplaceTempView("k3src")
    s.sql(f"INSERT INTO {table} SELECT id, c FROM k3src").collect()
    for name, cond in (
        ("k3_cast", "c = CAST('1' AS DECIMAL(38,0))"),
        ("k3_hand", "c >= 0.50 AND c < 1.50"),
    ):
        sql = f"SELECT count(*), sum(id) FROM {table} WHERE {cond}"
        secs, got = timed(sql, 10)
        tasks = re.findall(
            r"IcebergTableScan projection:\[[^\]]*\] predicate:\[[^\]]*\] N=(\d+)", analyzed(sql)
        )
        files = re.findall(
            r"(?:files|data_files|file_scan_tasks|tasks)[_a-z]*=(\d+)", analyzed(sql)
        )
        emit(name, secs, 10, answer=str(list(got[0])), tasks=tasks, files=files[:6])
elif case == "k2_exec":
    s.read.parquet(f"{data}/k2").createOrReplaceTempView("k2")
    for name, sql in (
        (
            "k2_exec_union",
            "SELECT count(*), sum(v) FROM (SELECT a AS v FROM k2 UNION ALL SELECT c FROM k2)",
        ),
        ("k2_exec_where", "SELECT count(*) FROM k2 WHERE a = c"),
    ):
        secs, got = timed(sql, 1)
        emit(name, secs, 1, answer=str(list(got[0])))
with Path(out).open("a") as fh:
    for r in records:
        r["native"] = "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1]
        fh.write(json.dumps(r) + "\n")
