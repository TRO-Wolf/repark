import json
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

label, data, out, kinds = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(",")
s = ReparkSession.builder.appName("ddu-in-cost").getOrCreate()
s.read.parquet(f"{data}/big").createOrReplaceTempView("t")
REPS = 3
TYPES = {"d10": "DECIMAL(10,2)", "d38": "DECIMAL(38,18)"}


def value(k: int) -> str:
    return f"{(k * 37 % 4000 - 2000) / 4:.2f}"


def dbl_list(n: int) -> str:
    return ", ".join(f"CAST({value(k)} AS DOUBLE)" for k in range(n))


def dec_list(column: str, n: int) -> str:
    return ", ".join(f"CAST('{value(k)}' AS {TYPES[column]})" for k in range(n))


def ranges(column: str, n: int) -> str:
    kind = TYPES[column]
    terms = []
    for k in range(n):
        low = value(k)
        high = f"{float(low) + 0.005:.3f}"
        terms.append(
            f"({column} >= CAST('{low}' AS {kind}) AND {column} < CAST('{high}' AS {kind}))"
        )
    return " OR ".join(terms)


def query(kind: str) -> str:
    form, column, n = kind.split(":")
    count = int(n)
    if form == "in_dbl":
        return f"SELECT count(*) FROM t WHERE {column} IN ({dbl_list(count)})"
    if form == "notin_dbl":
        return f"SELECT count(*) FROM t WHERE {column} NOT IN ({dbl_list(count)})"
    if form == "in_dec":
        return f"SELECT count(*) FROM t WHERE {column} IN ({dec_list(column, count)})"
    if form == "or_rng":
        return f"SELECT count(*) FROM t WHERE {ranges(column, count)}"
    if form == "plan_in_dbl":
        return f"SELECT id FROM t WHERE {column} IN ({dbl_list(count)}) LIMIT 0"
    raise ValueError(kind)


rows = []
for kind in kinds:
    sql = query(kind)
    got = s.sql(sql).collect()
    reps = 20 if kind.startswith("plan_") else REPS
    times = []
    for _ in range(3):
        start = time.perf_counter()
        for _ in range(reps):
            got = s.sql(sql).collect()
        times.append((time.perf_counter() - start) / reps)
    plan = str(s.sql("EXPLAIN " + sql).collect()[-1][-1])
    shape = "per-row" if "__repark_decimal_to_double__" in plan else "decimal"
    rows.append(
        {
            "case": kind,
            "label": label,
            "seconds": sorted(times)[1],
            "reps_per_sample": reps,
            "answer": str(got[0][0]) if got else "",
            "shape": shape,
            "values": 0 if kind.startswith("plan_") else 100_000_000,
            "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
        }
    )
with Path(out).open("a") as fh:
    for r in rows:
        fh.write(json.dumps(r) + "\n")
