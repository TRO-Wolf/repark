import json
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import cells
import lib

engine, ansi, conf, wh_parent, jar, out = sys.argv[1:7]
wh = tempfile.mkdtemp(prefix=f"wh-{engine}-{ansi}-{conf}-", dir=wh_parent)
extra = {}
if conf in cells.CONF_KEYS:
    k, v = cells.CONF_KEYS[conf]
    extra[k] = v
s, sf, Window, prov = lib.session(engine, ansi, wh, jar, extra)
records: list = [dict(key="provenance", ansi=ansi, conf=conf, **prov)]
for k in [c[0] for c in cells.CONF_KEYS.values()] + ["spark.sql.ansi.enabled"]:
    records.append(
        {"key": f"conf/get/{k}", "result": lib.attempt(lambda k=k: s.conf.get(k, "unset"))}
    )


def record(key: str, family: str, door: str, pos: str, col: str, sql: str, run: Callable) -> None:
    t0 = time.time()
    res = lib.attempt(run)
    records.append(
        {
            "key": key,
            "family": family,
            "door": door,
            "pos": pos,
            "col": col,
            "sql": sql,
            "result": res,
            "ms": round((time.time() - t0) * 1000, 1),
        }
    )


def q(sql: str) -> Callable:
    return lambda: lib.answer_of(s.sql(sql))


def make_table(name: str, names: list, rows: list, types: dict, parts: int) -> None:
    schema = ", ".join(f"{n} STRING" for n in names)
    raw = s.createDataFrame(rows, schema)
    typed = raw.selectExpr(*[f"CAST({n} AS {types[n]}) AS {n}" for n in names])
    path = str(Path(wh) / f"pq_{name}")
    for k in range(parts):
        typed.where(f"id % {parts} = {k}").write.mode("append").parquet(path)
    s.read.parquet(path).createOrReplaceTempView(name)


an, ar = cells.avg_rows()
make_table("t", an, ar, cells.avg_types(), 1)
make_table("t4", an, ar, cells.avg_types(), 4)
dn, dr = cells.div_rows()
make_table("v", dn, dr, cells.div_types(), 1)
records.append({"key": "data/t", "result": lib.attempt(lambda: lib.answer_of(s.table("t")))})
records.append({"key": "data/v", "result": lib.attempt(lambda: lib.answer_of(s.table("v")))})
records.append(
    {"key": "data/t4_files", "result": len(list((Path(wh) / "pq_t4").glob("*.parquet")))}
)

for key, fam, pos, col, sql in cells.avg_sql_cells("t"):
    if conf != "base" and pos not in ("grouped", "ungrouped", "win_rows3"):
        continue
    record(key, fam, "sql", pos, col, sql, q(sql))
for p, sc in cells.AVG_COLS:
    c = cells.col_name(p, sc)
    sql = cells.AVG_SQL_FORMS["grouped"].format(c=c, t="t4", normal=cells.normal_list(), case="")
    record(f"avg/grouped_t4/{c}", "avg", "sql", "grouped_t4", c, sql, q(sql))
for key, fam, pos, col, sql in cells.div_sql_cells("v"):
    record(key, fam, "sql", pos, col, sql, q(sql))

NORMAL_G = list(cells.NORMAL)


def avg_frame_cells() -> None:
    def base() -> Any:
        return s.table("t").where(sf.col("g").isin(NORMAL_G))

    for p, sc in cells.AVG_COLS:
        c = cells.col_name(p, sc)
        forms = {
            "F.avg": lambda c=c: base().groupBy("g").agg(sf.avg(c).alias("m")),
            "F.mean": lambda c=c: base().groupBy("g").agg(sf.mean(c).alias("m")),
            "GroupedData.avg": lambda c=c: base().groupBy("g").avg(c),
            "GroupedData.mean": lambda c=c: base().groupBy("g").mean(c),
            "agg_dict_avg": lambda c=c: base().groupBy("g").agg({c: "avg"}),
            "agg_dict_mean": lambda c=c: base().groupBy("g").agg({c: "mean"}),
            "select_F.avg": lambda c=c: s.table("t").where("g = 2").select(sf.avg(c).alias("m")),
            "repartition": lambda c=c: (
                s.table("t")
                .repartition(4)
                .where(sf.col("g").isin(NORMAL_G))
                .groupBy("g")
                .agg(sf.avg(c).alias("m"))
            ),
            "F.expr": lambda c=c: base().groupBy("g").agg(sf.expr(f"avg({c})").alias("m")),
            "window_rows": lambda c=c: base().select(
                "g",
                "id",
                sf.avg(c).over(Window.partitionBy("g").orderBy("id").rowsBetween(-2, 0)).alias("m"),
            ),
            "window_part": lambda c=c: base().select(
                "g", "id", sf.avg(c).over(Window.partitionBy("g")).alias("m")
            ),
        }
        for form, build in forms.items():
            if conf != "base" and form not in ("F.avg", "F.expr"):
                continue
            record(
                f"avgdf/{form}/{c}",
                "avg",
                "dataframe",
                form,
                c,
                form,
                lambda b=build: lib.answer_of(b()),
            )
    if conf != "base":
        return

    def sub() -> Any:
        return s.table("t").where(sf.col("g").isin([2, 9, 12]))

    for c in ("c10_2", "c28_2", "c38_18", "c10_0"):
        record(
            f"avgdf/describe/{c}",
            "avg",
            "dataframe",
            "describe",
            c,
            "describe",
            lambda c=c: lib.answer_of(sub().describe(c)),
        )
        record(
            f"avgdf/summary_default/{c}",
            "avg",
            "dataframe",
            "summary_default",
            c,
            "summary()",
            lambda c=c: lib.answer_of(sub().select(c).summary()),
        )
        record(
            f"avgdf/summary_five/{c}",
            "avg",
            "dataframe",
            "summary_five",
            c,
            "summary(count,mean,stddev,min,max)",
            lambda c=c: lib.answer_of(
                sub().select(c).summary("count", "mean", "stddev", "min", "max")
            ),
        )


def div_frame_cells() -> None:
    def v() -> Any:
        return s.table("v")

    for p, sc in cells.DIV_COLS:
        d = f"d{p}_{sc}"
        forms = {
            "col_div_int": lambda d=d: v().select("id", (sf.col(d) / 3).alias("q")),
            "col_div_lit": lambda d=d: v().select("id", (sf.col(d) / sf.lit(3)).alias("q")),
            "int_rdiv_col": lambda d=d: v().select("id", (3 / sf.col(d)).alias("q")),
            "col_div_intcol": lambda d=d: v().select("id", (sf.col(d) / sf.col("bi")).alias("q")),
            "F.expr": lambda d=d: v().select("id", sf.expr(f"{d} / 3").alias("q")),
            "selectExpr": lambda d=d: v().selectExpr("id", f"{d} / 3 AS q"),
            "col_mul_int": lambda d=d: v().select("id", (sf.col(d) * 3).alias("q")),
            "where_round": lambda d=d: (
                v().where(sf.col(d) / 3 == sf.round(sf.col(d) / 3, 6)).select("id")
            ),
        }
        for form, build in forms.items():
            record(
                f"divdf/{form}/{d}",
                "div",
                "dataframe",
                form,
                d,
                form,
                lambda b=build: lib.answer_of(b()),
            )


avg_frame_cells()
div_frame_cells()


def h_cells() -> None:
    s.sql("CREATE OR REPLACE TEMPORARY VIEW tv_h AS SELECT id, d10_2 / 2 AS h FROM v").collect()
    records.append(
        {
            "key": "setup/sv_h",
            "result": lib.attempt(
                lambda: (
                    (
                        s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect(),
                        s.sql("CREATE TABLE lc.ns.vt USING iceberg AS SELECT * FROM v").collect(),
                        s.sql(
                            "CREATE VIEW default.sv_h AS SELECT id, d10_2 / 2 AS h FROM lc.ns.vt"
                        ).collect(),
                    )
                    and "ok"
                )
            ),
        }
    )
    for scope, tmpl in cells.H_SCOPES.items():
        sql = tmpl.format(t="v")
        record(f"hdiv/{scope}", "div", "sql", f"h_{scope}", "d10_2", sql, q(sql))


if conf == "base":
    h_cells()


def ice_stmt(key: str, family: str, pos: str, col: str, setup: list, stmt: str, read: str) -> None:
    rec = {"key": key, "family": family, "door": "iceberg", "pos": pos, "col": col, "sql": stmt}
    for st in setup:
        r = lib.attempt(lambda st=st: s.sql(st).collect())
        if isinstance(r, str):
            rec["result"] = "SETUP-" + r
            records.append(rec)
            return
    r = lib.attempt(lambda: s.sql(stmt).collect())
    rec["statement"] = r if isinstance(r, str) else "ok"
    rec["result"] = lib.attempt(lambda: lib.answer_of(s.sql(read)))
    rec["schema"] = lib.attempt(
        lambda: [
            f.dataType.simpleString()
            for f in s.table(read.split(" FROM ")[1].split()[0]).schema.fields
        ]
    )
    records.append(rec)


def iceberg_cells() -> None:
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    normal = cells.normal_list()
    for c in ("c10_0", "c10_2", "c28_2", "c38_18"):
        n = f"lc.ns.ctas_avg_{c}"
        ice_stmt(
            f"ice/ctas_avg/{c}",
            "avg",
            "ctas",
            c,
            [],
            f"CREATE TABLE {n} USING iceberg AS SELECT g, avg({c}) AS m FROM t "
            f"WHERE g IN ({normal}) GROUP BY g",
            f"SELECT g, m FROM {n}",
        )
    for mode, props in (
        ("cow", "'write.update.mode'='copy-on-write','write.merge.mode'='copy-on-write'"),
        (
            "mor",
            "'write.update.mode'='merge-on-read','write.merge.mode'='merge-on-read','format-version'='2'",
        ),
    ):
        for c in ("c10_2", "c10_0"):
            tg = f"lc.ns.ins_avg_{mode}_{c}"
            create = (
                f"CREATE TABLE {tg} (k INT, n DECIMAL(10,5), w DECIMAL(24,10), src DECIMAL(10,2)) "
                f"USING iceberg TBLPROPERTIES ({props})"
            )
            if mode == "cow":
                ice_stmt(
                    f"ice/insert_avg/{mode}/{c}",
                    "avg",
                    "insert",
                    c,
                    [create],
                    f"INSERT INTO {tg} SELECT g, avg({c}), avg({c}), NULL FROM t "
                    f"WHERE g IN ({normal}) GROUP BY g",
                    f"SELECT k, n, w FROM {tg}",
                )
            tm = f"lc.ns.merge_avg_{mode}_{c}"
            ice_stmt(
                f"ice/merge_avg/{mode}/{c}",
                "avg",
                "merge",
                c,
                [
                    create.replace(tg, tm),
                    f"INSERT INTO {tm} VALUES (1, 0, 0, NULL), (2, 0, 0, NULL), (9, 0, 0, NULL)",
                ],
                f"MERGE INTO {tm} USING (SELECT g, avg({c}) AS m FROM t "
                f"WHERE g IN ({normal}) GROUP BY g) s "
                f"ON {tm}.k = s.g WHEN MATCHED THEN UPDATE SET n = s.m, w = s.m "
                "WHEN NOT MATCHED THEN INSERT (k, n, w, src) VALUES (s.g, s.m, s.m, NULL)",
                f"SELECT k, n, w FROM {tm}",
            )
        for d in ("d10_2", "d28_2", "d38_2"):
            if mode == "cow":
                n = f"lc.ns.ctas_div_{d}"
                ice_stmt(
                    f"ice/ctas_div/{d}",
                    "div",
                    "ctas",
                    d,
                    [],
                    f"CREATE TABLE {n} USING iceberg AS SELECT id, {d} / 3 AS q, "
                    f"{d} / 11 AS q11 FROM v",
                    f"SELECT id, q, q11 FROM {n}",
                )
            dcreate = (
                "CREATE TABLE {n} (id INT, d DECIMAL("
                + d[1:].replace("_", ",")
                + "), n DECIMAL(10,5), "
                f"w DECIMAL(24,10)) USING iceberg TBLPROPERTIES ({props})"
            )
            ti = f"lc.ns.ins_div_{mode}_{d}"
            if mode == "cow":
                ice_stmt(
                    f"ice/insert_div/{mode}/{d}",
                    "div",
                    "insert",
                    d,
                    [dcreate.format(n=ti)],
                    f"INSERT INTO {ti} SELECT id, {d}, {d} / 11, {d} / 3 FROM v WHERE id <> 10",
                    f"SELECT id, n, w FROM {ti}",
                )
            tu = f"lc.ns.upd_div_{mode}_{d}"
            ice_stmt(
                f"ice/update_div/{mode}/{d}",
                "div",
                "update",
                d,
                [
                    dcreate.format(n=tu),
                    f"INSERT INTO {tu} SELECT id, {d}, NULL, NULL FROM v WHERE id <> 10",
                ],
                f"UPDATE {tu} SET n = d / 11, w = d / 3",
                f"SELECT id, n, w FROM {tu}",
            )
            tmd = f"lc.ns.merge_div_{mode}_{d}"
            ice_stmt(
                f"ice/merge_div/{mode}/{d}",
                "div",
                "merge",
                d,
                [
                    dcreate.format(n=tmd),
                    f"INSERT INTO {tmd} SELECT id, {d}, NULL, NULL FROM v WHERE id <= 4",
                ],
                f"MERGE INTO {tmd} USING (SELECT id, {d} AS d FROM v WHERE id <> 10) s "
                f"ON {tmd}.id = s.id "
                "WHEN MATCHED THEN UPDATE SET n = s.d / 11, w = s.d / 3 "
                "WHEN NOT MATCHED THEN INSERT (id, d, n, w) VALUES (s.id, s.d, s.d / 11, s.d / 3)",
                f"SELECT id, n, w FROM {tmd}",
            )


if conf == "base":
    iceberg_cells()
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, conf, len(records), flush=True)
