import json
import sys
import time
from collections.abc import Callable
from pathlib import Path

import cells
import lib

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
other = "false" if ansi == "true" else "true"
wh = str(Path(wh_parent) / f"rev-{engine}-{ansi}")
Path(wh).mkdir(parents=True, exist_ok=False)
s, sf, Window, prov = lib.session(engine, ansi, wh, jar, {})
records: list = [dict(key="provenance", ansi=ansi, **prov)]
records.append(
    {"key": "conf/ansi", "result": lib.attempt(lambda: s.conf.get("spark.sql.ansi.enabled"))}
)


def record(key: str, run: Callable) -> None:
    t0 = time.time()
    res = lib.attempt(run)
    records.append({"key": key, "result": res, "ms": round((time.time() - t0) * 1000, 1)})


def sql(key: str, text: str) -> None:
    record(key, lambda: lib.answer_of(s.sql(text)))


def frame(key: str, build: Callable) -> None:
    record(key, lambda: lib.answer_of(build()))


sn, sr = cells.s_rows()
raw = s.createDataFrame(sr, ", ".join(f"{n} STRING" for n in sn))
types = cells.s_types()
typed = raw.selectExpr(*[f"CAST({n} AS {types[n]}) AS {n}" for n in sn])
pq_path = str(Path(wh) / "pq_s")
typed.write.mode("append").parquet(pq_path)
s.read.parquet(pq_path).createOrReplaceTempView("s")
T = s.table("s")

for c in ("c38_0", "c38_38"):
    sql(f"rev/rollup/g1_7/{c}", f"SELECT g, sum({c}) FROM s WHERE g IN (1, 7) GROUP BY ROLLUP(g)")
    sql(
        f"rev/gsets/g1_7/{c}",
        f"SELECT g, sum({c}) FROM s WHERE g IN (1, 7) GROUP BY GROUPING SETS ((g), ())",
    )
    sql(f"rev/cube/g1_5/{c}", f"SELECT g, sum({c}) FROM s WHERE g IN (1, 5) GROUP BY CUBE(g)")
    sql(
        f"rev/rollup_total/g1_4/{c}",
        f"SELECT g, sum({c}) FROM s WHERE g IN (1, 4) GROUP BY ROLLUP(g)",
    )
    sql(
        f"rev/gsets_total/g1_4/{c}",
        f"SELECT g, sum({c}) FROM s WHERE g IN (1, 4) GROUP BY GROUPING SETS ((g), ())",
    )
    sql(f"rev/cube_total/g1_4/{c}", f"SELECT g, sum({c}) FROM s WHERE g IN (1, 4) GROUP BY CUBE(g)")
    sql(f"rev/filter/g5/{c}", f"SELECT sum({c}) FILTER (WHERE g = 5) FROM s")
    sql(
        f"rev/filter_grouped/g7/{c}",
        f"SELECT g, sum({c}) FILTER (WHERE id > 0) FROM s WHERE g = 7 GROUP BY g",
    )
    sql(
        f"rev/sum_case/g5/{c}",
        f"SELECT sum(CASE WHEN g = 5 THEN {c} ELSE 0 END) FROM s WHERE g = 5",
    )
    sql(
        f"rev/sum_case_grouped/g7/{c}",
        f"SELECT g, sum(CASE WHEN id > 0 THEN {c} END) FROM s WHERE g = 7 GROUP BY g",
    )
    sql(
        f"rev/having_lt/g1_7/{c}",
        f"SELECT g FROM s WHERE g IN (1, 7) GROUP BY g HAVING sum({c}) < 0",
    )
    sql(f"rev/having_gt/all/{c}", f"SELECT g FROM s GROUP BY g HAVING sum({c}) > 0")
    sql(
        f"rev/distinct_two/g5/{c}",
        f"SELECT g, sum(DISTINCT {c}), sum(DISTINCT c38_10) FROM s WHERE g = 5 GROUP BY g",
    )
    sql(
        f"rev/distinct_mixed/g5/{c}",
        f"SELECT g, sum(DISTINCT {c}), count(c38_10) FROM s WHERE g = 5 GROUP BY g",
    )
    sql(
        f"rev/distinct_mixed/g7/{c}",
        f"SELECT g, sum(DISTINCT {c}), sum(c10_2) FROM s WHERE g = 7 GROUP BY g",
    )
    sql(
        f"rev/distinct_mixed_ungrouped/g5/{c}",
        f"SELECT sum(DISTINCT {c}), count(c38_10) FROM s WHERE g = 5",
    )
    sql(
        f"rev/union/g4_14/{c}",
        f"SELECT sum(x) FROM (SELECT {c} x FROM s WHERE g = 4 "
        f"UNION ALL SELECT {c} FROM s WHERE g = 14)",
    )
    sql(
        f"rev/union_partial/g5_14/{c}",
        f"SELECT sum(x) FROM (SELECT {c} x FROM s WHERE g = 5 "
        f"UNION ALL SELECT {c} FROM s WHERE g = 14)",
    )
    sql(
        f"rev/union_grouped/g4_14/{c}",
        f"SELECT k, sum(x) FROM (SELECT 1 k, {c} x FROM s WHERE g = 4 "
        f"UNION ALL SELECT 1, {c} FROM s WHERE g = 14) GROUP BY k",
    )
sql("rev/sum_mul1/g5/c38_0", "SELECT sum(c38_0 * 1) FROM s WHERE g = 5")
sql("rev/sum_mul1_grouped/g7/c38_0", "SELECT g, sum(c38_0 * 1) FROM s WHERE g = 7 GROUP BY g")
sql(
    "rev/cast_dbl/g7",
    "SELECT sum(CAST(CAST('5e37' AS DOUBLE) AS DECIMAL(38,0))) FROM s WHERE g = 7",
)
sql(
    "rev/cast_dbl_grouped/g6",
    "SELECT g, sum(CAST(CAST('9e37' AS DOUBLE) AS DECIMAL(38,0))) FROM s WHERE g = 6 GROUP BY g",
)
sql(
    "rev/cast_dbl_col/g7",
    "SELECT sum(CAST(CAST(c38_0 AS DOUBLE) / 2 AS DECIMAL(38,0))) FROM s WHERE g = 7",
)
record(
    "data/union_parts",
    lambda: (
        s.sql(
            "SELECT c38_0 FROM s WHERE g = 4 UNION ALL SELECT c38_0 FROM s WHERE g = 14"
        ).rdd.getNumPartitions()
        if engine == "spark"
        else "n/a"
    ),
)

frame(
    "rev/df_pivot/g1_7/c38_0",
    lambda: T.where("g IN (1, 7)").groupBy().pivot("g", [1, 7]).sum("c38_0"),
)
frame(
    "rev/df_pivot_agg/g1_7/c38_0",
    lambda: T.where("g IN (1, 7)").groupBy().pivot("g", [1, 7]).agg(sf.sum("c38_0")),
)
frame("rev/df_rollup/g1_7/c38_0", lambda: T.where("g IN (1, 7)").rollup("g").agg(sf.sum("c38_0")))
frame(
    "rev/df_rollup_total/g1_4/c38_0",
    lambda: T.where("g IN (1, 4)").rollup("g").agg(sf.sum("c38_0")),
)
frame("rev/df_cube_total/g1_4/c38_0", lambda: T.where("g IN (1, 4)").cube("g").sum("c38_0"))
if engine == "spark":
    frame(
        "rev/df_gsets_total/g1_4/c38_0",
        lambda: T.where("g IN (1, 4)").groupingSets([["g"], []], "g").agg(sf.sum("c38_0")),
    )
else:
    frame(
        "rev/df_gsets_total/g1_4/c38_0",
        lambda: T.where("g IN (1, 4)").groupingSets("g").agg(sf.sum("c38_0")),
    )
frame(
    "rev/df_call_function/g7/c38_0",
    lambda: T.where("g = 7").groupBy("g").agg(sf.call_function("sum", sf.col("c38_0"))),
)
frame(
    "rev/df_call_function_ungrouped/g5/c38_0",
    lambda: T.where("g = 5").agg(sf.call_function("sum", "c38_0")),
)
for g, c in ((10, "c38_0"), (11, "c38_0"), (10, "c38_38"), (11, "c38_38")):
    frame(
        f"rev/df_try_sum_ungrouped/g{g}/{c}",
        lambda g=g, c=c: T.where(f"g = {g}").agg(sf.try_sum(c)),
    )
    frame(
        f"rev/df_try_sum_grouped/g{g}/{c}",
        lambda g=g, c=c: T.where(f"g = {g}").groupBy("g").agg(sf.try_sum(c)),
    )
frame("rev/df_try_sum_ungrouped/g5/c38_0", lambda: T.where("g = 5").agg(sf.try_sum("c38_0")))
if engine == "repark":
    from repark.spark import functions as facade

    record("route/scalar_sum", lambda: facade._scalar("sum", "c38_0")._agg_name)

s.sql("CREATE OR REPLACE TEMP VIEW v_sum AS SELECT g, sum(c38_0) AS x FROM s GROUP BY g")
s.sql("CREATE OR REPLACE TEMP VIEW v_avg AS SELECT g, avg(c38_0) AS x FROM s GROUP BY g")
T.groupBy("g").agg(sf.sum("c38_0").alias("x")).createOrReplaceTempView("v_dfsum")
for g in (5, 7):
    sql(f"rev/view_same/g{g}/c38_0", f"SELECT x FROM v_sum WHERE g = {g}")
sql("rev/view_same_avg/g5/c38_0", "SELECT x FROM v_avg WHERE g = 5")
s.conf.set("spark.sql.ansi.enabled", other)
records.append(
    {
        "key": "conf/ansi_flipped",
        "result": lib.attempt(lambda: s.conf.get("spark.sql.ansi.enabled")),
    }
)
for g in (5, 7):
    sql(f"rev/view_flip/g{g}/c38_0", f"SELECT x FROM v_sum WHERE g = {g}")
    sql(f"rev/dfview_flip/g{g}/c38_0", f"SELECT x FROM v_dfsum WHERE g = {g}")
sql("rev/view_flip_avg/g5/c38_0", "SELECT x FROM v_avg WHERE g = 5")
sql("rev/flip_direct/g5/c38_0", "SELECT sum(c38_0) FROM s WHERE g = 5")
sql("rev/flip_direct_avg/g5/c38_0", "SELECT avg(c38_0) FROM s WHERE g = 5")
s.conf.set("spark.sql.ansi.enabled", ansi)
records.append(
    {
        "key": "conf/ansi_restored",
        "result": lib.attempt(lambda: s.conf.get("spark.sql.ansi.enabled")),
    }
)

sql(
    "obs/pivot_sql/g1_7",
    "SELECT * FROM (SELECT g, c38_0 FROM s WHERE g IN (1, 7)) PIVOT (sum(c38_0) FOR g IN (1, 7))",
)
sql("obs/caststr_lit", "SELECT CAST('1000000000000000000000000000000' AS DECIMAL(34,4)) AS x")
s.createDataFrame([["1000000000000000000000000000000"], ["5"]], "t STRING").createOrReplaceTempView(
    "strs"
)
sql("obs/caststr_col", "SELECT CAST(t AS DECIMAL(34,4)) AS x FROM strs")
sql("obs/caststr_col_sum", "SELECT sum(CAST(t AS DECIMAL(34,4))) AS x FROM strs")
frame("obs/summary/g5", lambda: T.where("g = 5").select("c38_0").summary())
frame("obs/describe/g5", lambda: T.where("g = 5").select("c38_0").describe())
frame("obs/describe/g13", lambda: T.where("g = 13").select("c38_0").describe())

if engine == "repark":
    windows = {
        "sum_rows": "sum(c38_0) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        "try_sum_rows": "try_sum(c38_0) OVER "
        "(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        "avg_rows": "avg(c38_0) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        "sum_running": "sum(c38_0) OVER (ORDER BY id)",
        "try_sum_running": "try_sum(c38_0) OVER (ORDER BY id)",
        "bigint_rows": "sum(CAST(id AS BIGINT)) OVER "
        "(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
        "double_rows": "sum(CAST(id AS DOUBLE)) OVER "
        "(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)",
    }
    for name, expr in windows.items():
        text = f"EXPLAIN VERBOSE SELECT id, {expr} AS w FROM s WHERE g = 10"
        record(
            f"explain/rescan/{name}",
            lambda text=text: [
                [r[0], " ".join(str(r[1]).split())[:700]] for r in s.sql(text).collect()
            ],
        )
    record(
        "explain/distinct_mixed",
        lambda: [
            [r[0], " ".join(str(r[1]).split())[:900]]
            for r in s.sql(
                "EXPLAIN SELECT g, sum(DISTINCT c38_0), count(c38_10) FROM s WHERE g = 5 GROUP BY g"
            ).collect()
        ],
    )

s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
lc = Path(wh) / "db"


def stored(table: str) -> object:
    import pyarrow.parquet as pq

    found = []
    for f in sorted((lc / table / "data").rglob("*.parquet")):
        found.append([lib.norm(v) for v in pq.read_table(f).column("s").to_pylist()])
    return found


def run_statements(key: str, table: str, statements: list) -> None:
    results = [lib.attempt(lambda st=st: (s.sql(st).collect(), "ok")[1]) for st in statements]
    records.append({"key": key, "result": results})
    sql(key + "/readback", f"SELECT * FROM lc.db.{table}")
    records.append({"key": key + "/files", "result": lib.attempt(lambda: stored(table))})


src_groups = "g IN (1, 5, 6)"
for c, dt in (("c38_0", "DECIMAL(38,0)"), ("c38_38", "DECIMAL(38,38)")):
    src = f"SELECT g, sum({c}) AS s FROM s WHERE {src_groups} GROUP BY g"
    for mode in ("none", "counts", "full"):
        props = f"'write.metadata.metrics.default'='{mode}'"
        tb = f"ins_{mode}_{c}"
        run_statements(
            f"ice/insert/metrics_{mode}/{c}",
            tb,
            [
                f"CREATE TABLE lc.db.{tb} (g INT, s {dt}) USING iceberg TBLPROPERTIES ({props})",
                f"INSERT INTO lc.db.{tb} {src}",
            ],
        )
        tb = f"ctas_{mode}_{c}"
        run_statements(
            f"ice/ctas/metrics_{mode}/{c}",
            tb,
            [f"CREATE TABLE lc.db.{tb} USING iceberg TBLPROPERTIES ({props}) AS {src}"],
        )
        if c != "c38_0":
            continue
        for kind, write_mode in (("merge_cow", "copy-on-write"), ("merge_mor", "merge-on-read")):
            tb = f"{kind}_{mode}_{c}"
            run_statements(
                f"ice/{kind}/metrics_{mode}/{c}",
                tb,
                [
                    f"CREATE TABLE lc.db.{tb} (g INT, s {dt}) USING iceberg TBLPROPERTIES "
                    f"('format-version'='2', 'write.merge.mode'='{write_mode}', "
                    f"'write.update.mode'='{write_mode}', "
                    f"'write.delete.mode'='{write_mode}', {props})",
                    f"INSERT INTO lc.db.{tb} VALUES (1, 0)",
                    f"MERGE INTO lc.db.{tb} AS tg USING ({src}) AS sr ON tg.g = sr.g "
                    "WHEN MATCHED THEN UPDATE SET tg.s = sr.s "
                    "WHEN NOT MATCHED THEN INSERT (g, s) VALUES (sr.g, sr.s)",
                ],
            )

for width in (100, 101):
    fillers = ", ".join(f"0 AS z{i:03d}" for i in range(1, width - 1))
    wide_src = f"SELECT g, {fillers}, sum(c38_0) AS s FROM s WHERE {src_groups} GROUP BY g"
    tb = f"wide{width}_ctas"
    run_statements(
        f"ice/ctas/wide{width}/c38_0", tb, [f"CREATE TABLE lc.db.{tb} USING iceberg AS {wide_src}"]
    )
    cols = ", ".join(f"z{i:03d} INT" for i in range(1, width - 1))
    tb = f"wide{width}_insert"
    run_statements(
        f"ice/insert/wide{width}/c38_0",
        tb,
        [
            f"CREATE TABLE lc.db.{tb} (g INT, {cols}, s DECIMAL(38,0)) USING iceberg",
            f"INSERT INTO lc.db.{tb} {wide_src}",
        ],
    )
    record(f"ice/wide{width}/columns", lambda tb=tb: len(s.table(f"lc.db.{tb}").columns))

records.append({"key": "warehouse", "result": "<scratchpad>/" + wh.split("/scratchpad/", 1)[-1]})
Path(out).write_text("".join(json.dumps(r) + "\n" for r in records))
print("done", len(records), flush=True)
