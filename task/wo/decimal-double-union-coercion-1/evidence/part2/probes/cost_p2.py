import json
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

case, label, data, warehouse, out = sys.argv[1:6]
s = (
    ReparkSession.builder.appName("ddu-cost-p2")
    .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", warehouse)
    .getOrCreate()
)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()


def values_rows(count: int, cell: str, odd: str | None) -> str:
    rows = [f"({i}, {cell})" for i in range(count - 1)]
    rows.append(f"({count - 1}, {odd or cell})")
    return ", ".join(rows)


def p16_constants(count: int) -> str:
    return ", ".join(f"CAST({4.0 + k * 0.18!r} AS DOUBLE)" for k in range(count))


def d10_constants(count: int) -> str:
    return ", ".join(f"CAST({(k * 37 % 4000 - 2000) / 4:.2f} AS DOUBLE)" for k in range(count))


def timed(run: object, reps: int) -> tuple[float, object]:
    got = run()
    times = []
    for _ in range(3):
        start = time.perf_counter()
        for _ in range(reps):
            got = run()
        times.append((time.perf_counter() - start) / reps)
    return sorted(times)[1], got


record: dict = {"case": case, "label": label}
if case.startswith("plan_values10k"):
    odd = "CAST(0.25 AS DOUBLE)" if case == "plan_values10k_dbl" else None
    sql = f"SELECT count(*) FROM VALUES {values_rows(10_000, '0.5', odd)} AS x(a, b)"
    seconds, got = timed(lambda: s.sql(sql).collect(), 20)
    record.update(seconds=seconds, values=0, reps_per_sample=20, answer=str(got[0][0]))
elif case.startswith("ins_values10k"):
    kind = "DECIMAL(10,2)" if "_dec" in case else "DOUBLE"
    cell = "0.5" if kind.startswith("DECIMAL") else "CAST(0.5 AS DOUBLE)"
    odd = None
    if case.endswith("_mixed"):
        odd = "CAST(0.25 AS DOUBLE)" if kind.startswith("DECIMAL") else "0.25"
    rows = values_rows(10_000, cell, odd)
    seconds_all = []
    for rep in range(3):
        table = f"lc.ns.i_{rep}"
        s.sql(f"CREATE TABLE {table} (id INT, c {kind}) USING iceberg").collect()
        start = time.perf_counter()
        s.sql(f"INSERT INTO {table} VALUES {rows}").collect()
        seconds_all.append(time.perf_counter() - start)
    count = s.sql("SELECT count(*), sum(c) FROM lc.ns.i_0").collect()[0]
    record.update(seconds=sorted(seconds_all)[1], values=10_000, answer=str(list(count)))
elif case.startswith("merge_values1000"):
    odd = "CAST(0.25 AS DOUBLE)" if case.endswith("_mixed") else None
    rows = values_rows(1_000, "0.5", odd)
    seconds_all = []
    for rep in range(3):
        table = f"lc.ns.m_{rep}"
        s.sql(f"CREATE TABLE {table} (id INT, c DOUBLE) USING iceberg").collect()
        s.sql(
            f"INSERT INTO {table} SELECT CAST(id AS INT), v FROM range(1000) AS r(id) "
            "CROSS JOIN (SELECT CAST(1.0 AS DOUBLE) AS v)"
        ).collect()
        start = time.perf_counter()
        s.sql(
            f"MERGE INTO {table} t USING (SELECT * FROM VALUES {rows} AS s0(id, c)) s "
            "ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.c = s.c"
        ).collect()
        seconds_all.append(time.perf_counter() - start)
    got = s.sql("SELECT count(*), sum(c) FROM lc.ns.m_0").collect()[0]
    record.update(seconds=sorted(seconds_all)[1], values=1_000, answer=str(list(got)))
elif case.startswith("p16_in"):
    count = int(case.rsplit("_", 1)[1])
    s.read.parquet(f"{data}/p16").createOrReplaceTempView("p")
    sql = f"SELECT count(*) FROM p WHERE d16 IN ({p16_constants(count)})"
    seconds, got = timed(lambda: s.sql(sql).collect(), 3)
    plan = str(s.sql("EXPLAIN " + sql).collect()[-1][-1])
    record.update(
        seconds=seconds,
        values=100_000_000,
        reps_per_sample=3,
        answer=str(got[0][0]),
        shape="per-row" if "__repark_decimal_to_double__" in plan else "decimal",
    )
elif case == "ice_in20":
    s.read.parquet(f"{data}/mid").createOrReplaceTempView("m")
    s.sql("CREATE TABLE lc.ns.p (id BIGINT, d10 DECIMAL(10,2), v DOUBLE) USING iceberg").collect()
    for part in range(6):
        s.sql(f"INSERT INTO lc.ns.p SELECT id, d10, v FROM m WHERE id % 6 = {part}").collect()
    sql = f"SELECT count(*) FROM lc.ns.p WHERE d10 IN ({d10_constants(20)})"
    seconds, got = timed(lambda: s.sql(sql).collect(), 6)
    analyzed = "\n".join(
        str(value) for row in s.sql("EXPLAIN ANALYZE " + sql).collect() for value in row
    )
    plan = "\n".join(str(value) for row in s.sql("EXPLAIN " + sql).collect() for value in row)
    record.update(
        seconds=seconds,
        values=20_000_000,
        reps_per_sample=6,
        answer=str(got[0][0]),
        plan=plan[-1500:],
        analyzed=analyzed[-3000:],
    )
else:
    raise ValueError(case)
record["native"] = "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1]
with Path(out).open("a") as fh:
    fh.write(json.dumps(record) + "\n")
