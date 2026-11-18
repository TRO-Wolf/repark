import json
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import cells
import lib

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"wh-sum-{engine}-{ansi}-", dir=wh_parent)
s, sf, Window, prov = lib.session(engine, ansi, wh, jar, {})
records: list = [dict(key="provenance", ansi=ansi, **prov)]
records.append(
    {
        "key": "conf/get/spark.sql.ansi.enabled",
        "result": lib.attempt(lambda: s.conf.get("spark.sql.ansi.enabled", "unset")),
    }
)


def record(
    key: str, family: str, door: str, pos: str, col: str, group: object, run: Callable
) -> None:
    t0 = time.time()
    res = lib.attempt(run)
    records.append(
        {
            "key": key,
            "family": family,
            "door": door,
            "pos": pos,
            "col": col,
            "group": group,
            "result": res,
            "ms": round((time.time() - t0) * 1000, 1),
        }
    )


def make_table(name: str, names: list, rows: list, types: dict, parts: int) -> None:
    schema = ", ".join(f"{n} STRING" for n in names)
    raw = s.createDataFrame(rows, schema)
    typed = raw.selectExpr(*[f"CAST({n} AS {types[n]}) AS {n}" for n in names])
    if parts == 0:
        typed.createOrReplaceTempView(name)
        records.append({"key": f"data/{name}_files", "result": 0})
        return
    path = str(Path(wh) / f"pq_{name}")
    if parts == 1:
        typed.write.mode("append").parquet(path)
    else:
        for k in range(parts):
            typed.where(f"f = {k}").write.mode("append").parquet(path)
    s.read.parquet(path).createOrReplaceTempView(name)
    records.append({"key": f"data/{name}_files", "result": len(list(Path(path).glob("*.parquet")))})


sn, sr = cells.s_rows()
make_table("s", sn, sr, cells.s_types(), 1)
make_table("s4", sn, sr, cells.s_types(), 4)
p2_names, p2_data = cells.p2_rows()
make_table("p2", p2_names, p2_data, cells.p2_types(), 2)
bn, br = cells.big_rows()
make_table("big", bn, br, cells.big_types(), 1)
kn, kr = cells.k_rows()
make_table("k", kn, kr, cells.k_types(), 0)
make_table("kp", kn, kr, cells.k_types(), 1)
records.append({"key": "data/s", "result": lib.attempt(lambda: lib.answer_of(s.table("s")))})
records.append({"key": "data/k", "result": lib.attempt(lambda: lib.answer_of(s.table("k")))})
records.append(
    {
        "key": "data/big_counts",
        "result": lib.attempt(
            lambda: lib.answer_of(s.sql("SELECT g, count(*) AS n FROM big GROUP BY g"))
        ),
    }
)
records.append(
    {
        "key": "setup/sv_sum",
        "result": lib.attempt(
            lambda: (
                s.sql(
                    "CREATE OR REPLACE TEMPORARY VIEW sv_sum AS "
                    "SELECT g, sum(c38_0) AS m FROM s GROUP BY g"
                ).collect()
                and "ok"
            )
        ),
    }
)


def q(sql: str) -> Callable:
    return lambda: lib.answer_of(s.sql(sql))


for key, fam, pos, col, g, _table, sql in cells.sum_sql_cells():
    record(key, fam, "sql", pos, col, g, q(sql))


def plan_text(sql: str) -> str:
    if engine == "spark":
        return s.sql(sql)._jdf.queryExecution().executedPlan().toString()[:2500]
    rows = s.sql("EXPLAIN " + sql).collect()
    return "\n".join(str(v) for r in rows for v in r)[:2500]


for g in (41, 42):
    sql = f"SELECT g, sum(b) AS m FROM kp WHERE g = {g} GROUP BY g"
    record(f"side/parquet_bigint_stats/g{g}/b", "side", "sql", "parquet_stats", "b", g, q(sql))
for name, sql in cells.EXPLAIN_CELLS.items():
    records.append(
        {"key": f"explain/{name}", "result": lib.attempt(lambda sql=sql: plan_text(sql))}
    )


def tbl(g: object) -> Any:
    t = s.table("s")
    return t if g is None else t.where(sf.col("g") == g)


def df_forms(c: str, g: object) -> dict:
    w2 = Window.partitionBy("g").orderBy("id").rowsBetween(-1, 0)
    wp = Window.partitionBy("g")
    return {
        "F.sum": lambda: tbl(g).groupBy("g").agg(sf.sum(c).alias("m")),
        "GroupedData.sum": lambda: tbl(g).groupBy("g").sum(c),
        "agg_dict": lambda: tbl(g).groupBy("g").agg({c: "sum"}),
        "F.expr": lambda: tbl(g).groupBy("g").agg(sf.expr(f"sum({c})").alias("m")),
        "selectExpr": lambda: tbl(g).selectExpr(f"sum({c}) AS m"),
        "select_F.sum": lambda: tbl(g).select(sf.sum(c).alias("m")),
        "agg_F.sum": lambda: tbl(g).agg(sf.sum(c).alias("m")),
        "F.try_sum": lambda: tbl(g).groupBy("g").agg(sf.try_sum(c).alias("m")),
        "window_rows2": lambda: tbl(g).select("g", "id", sf.sum(c).over(w2).alias("m")),
        "window_part": lambda: tbl(g).select("g", "id", sf.sum(c).over(wp).alias("m")),
        "repartition": lambda: tbl(g).repartition(4).groupBy("g").agg(sf.sum(c).alias("m")),
        "F.sum_distinct": lambda: tbl(g).groupBy("g").agg(sf.sum_distinct(c).alias("m")),
    }


scopes = [(None, c) for c in cells.cols_all()]
scopes += [(g, c) for g in cells.DF_GROUPS for c in cells.names_of(cells.WIDE)]
for g, c in scopes:
    for form, build in df_forms(c, g).items():
        scope = "all" if g is None else f"g{g}"
        fam = "try_sum" if form == "F.try_sum" else "sum"
        record(
            f"df/{form}/{scope}/{c}",
            fam,
            "dataframe",
            form,
            c,
            g,
            lambda b=build: lib.answer_of(b()),
        )
for g in (42, 44, 41):
    kt = s.table("k").where(sf.col("g") == g)
    record(
        f"intdf/F.sum/g{g}/b",
        "control",
        "dataframe",
        "int_F.sum",
        "b",
        g,
        lambda kt=kt: lib.answer_of(kt.groupBy("g").agg(sf.sum("b").alias("m"))),
    )


def file_values(table: str) -> object:
    import pyarrow.parquet as pq

    found = []
    for path in sorted(Path(wh).glob(f"ns/{table}/data/**/*.parquet")):
        tab = pq.read_table(path)
        if "m" not in tab.column_names:
            continue
        col = tab.column("m")
        found.append(
            {
                "type": str(col.type),
                "values": sorted(lib.norm(v) for v in col.to_pylist()),
            }
        )
    return found


def ice_stmt(key: str, family: str, pos: str, col: str, table: str, setup: list, stmt: str) -> None:
    full = f"lc.ns.{table}"
    rec = {"key": key, "family": family, "door": "iceberg", "pos": pos, "col": col, "group": None}
    for st in setup:
        r = lib.attempt(lambda st=st: s.sql(st.format(t=full)).collect())
        if isinstance(r, str):
            rec["result"] = "SETUP-" + r
            records.append(rec)
            return
    r = lib.attempt(lambda: s.sql(stmt.format(t=full)).collect())
    rec["statement"] = r if isinstance(r, str) else "ok"
    rec["result"] = lib.attempt(lambda: lib.answer_of(s.sql(f"SELECT g, m FROM {full}")))
    rec["schema"] = lib.attempt(
        lambda: [f.dataType.simpleString() for f in s.table(full).schema.fields]
    )
    rec["files"] = lib.attempt(lambda: file_values(table))
    records.append(rec)


def iceberg_cells() -> None:
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    fit = ", ".join(str(g) for g in cells.FIT_GROUPS)
    for p, sc in cells.ICE_COLS:
        c = cells.col_name(p, sc)
        sum_type = f"DECIMAL({min(38, p + 10)},{sc})"
        for scope, where in (
            ("all", ""),
            ("fit", f"WHERE g IN ({fit})"),
            ("wrap", "WHERE g IN (1, 7)"),
            ("back", "WHERE g IN (1, 10, 11)"),
        ):
            src = f"SELECT g, sum({c}) AS m FROM s {where} GROUP BY g"
            ice_stmt(
                f"ice/ctas/{scope}/{c}",
                "sum",
                "ctas",
                c,
                f"ctas_{scope}_{c}",
                [],
                f"CREATE TABLE {{t}} USING iceberg AS {src}",
            )
            create = f"CREATE TABLE {{t}} (g INT, m {sum_type}) USING iceberg"
            ice_stmt(
                f"ice/insert/{scope}/{c}",
                "sum",
                "insert",
                c,
                f"ins_{scope}_{c}",
                [create],
                f"INSERT INTO {{t}} {src}",
            )
            for mode, props in (
                ("cow", "'write.merge.mode'='copy-on-write'"),
                ("mor", "'write.merge.mode'='merge-on-read','format-version'='2'"),
            ):
                ice_stmt(
                    f"ice/merge_{mode}/{scope}/{c}",
                    "sum",
                    f"merge_{mode}",
                    c,
                    f"mrg_{mode}_{scope}_{c}",
                    [
                        create + f" TBLPROPERTIES ({props})",
                        "INSERT INTO {t} VALUES (1, 0), (5, 0), (6, 0), (13, 0)",
                    ],
                    f"MERGE INTO {{t}} USING ({src}) src ON {{t}}.g = src.g "
                    "WHEN MATCHED THEN UPDATE SET m = src.m "
                    "WHEN NOT MATCHED THEN INSERT (g, m) VALUES (src.g, src.m)",
                )
    ice_stmt(
        "ice/ctas_big/all/c34_4",
        "sum",
        "ctas_big",
        "c34_4",
        "ctas_big_c34_4",
        [],
        "CREATE TABLE {t} USING iceberg AS SELECT g, sum(c34_4) AS m FROM big GROUP BY g",
    )
    ice_stmt(
        "ice/ctas_big/wrap/c34_4",
        "sum",
        "ctas_big",
        "c34_4",
        "ctas_big_wrap_c34_4",
        [],
        "CREATE TABLE {t} USING iceberg AS SELECT g, sum(c34_4) AS m FROM big "
        "WHERE g IN (32, 33) GROUP BY g",
    )
    ice_stmt(
        "ice/ctas_try/all/c38_0",
        "try_sum",
        "ctas_try",
        "c38_0",
        "ctas_try_c38_0",
        [],
        "CREATE TABLE {t} USING iceberg AS SELECT g, try_sum(c38_0) AS m FROM s GROUP BY g",
    )


iceberg_cells()
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), flush=True)
