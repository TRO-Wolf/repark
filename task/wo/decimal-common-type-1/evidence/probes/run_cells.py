import json
import sys
import tempfile
import time
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import cells

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"wh-{engine}-{ansi}-", dir=wh_parent)
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

    builder = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config("spark.sql.extensions", EXT)
        .config("spark.sql.shuffle.partitions", "1")
    )
    prov = {"engine": "spark", "version": pyspark.__version__, "jar": Path(jar).name}
else:
    import repark
    import repark._native as native
    from repark import ReparkSession as Builder
    from repark import functions as sf

    print("native", native.__file__, flush=True)
    builder = Builder.builder.appName("dct-step0")
    prov = {
        "engine": "repark",
        "version": getattr(repark, "__version__", "?"),
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
s = (
    builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
records: list = []


def err(e: Exception) -> str:
    cls = None
    for attr in ("getCondition", "getErrorClass"):
        fn = getattr(e, attr, None)
        if fn is not None:
            try:
                cls = fn()
            except Exception:
                cls = None
            if cls:
                break
    head = " ".join(str(e).split())[:240]
    return f"ERR:{type(e).__name__}:{cls}:{head}"


def norm(v: object) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, dict):
        return (
            "{"
            + ",".join(
                f"{norm(k)}->{norm(x)}" for k, x in sorted(v.items(), key=lambda i: str(i[0]))
            )
            + "}"
        )
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(norm(x) for x in v) + "]"
    return str(v)


def answer_of(frame: Any) -> dict:
    rows = frame.collect()
    types = [f.dataType.simpleString() for f in frame.schema.fields]
    return {"rows": sorted("|".join(norm(x) for x in r) for r in rows), "types": types}


def attempt(run: Callable[[], object]) -> object:
    try:
        return run()
    except Exception as e:
        return err(e)


def record(
    key: str, group: str, door: str, pos: str, pair: str, sql: str, run: Callable[[], object]
) -> None:
    t0 = time.time()
    res = attempt(run)
    records.append(
        {
            "key": key,
            "group": group,
            "door": door,
            "pos": pos,
            "pair": pair,
            "sql": sql,
            "result": res,
            "ms": round((time.time() - t0) * 1000, 1),
        }
    )


def q(sql: str) -> Callable[[], object]:
    return lambda: answer_of(s.sql(sql))


def run_sql_cells(prefix: str, tables: tuple) -> None:
    for t in tables:
        for key, family, pos, tmpl in cells.pair_cells():
            if t == "tx" and pos in cells.T_ONLY:
                continue
            sql = tmpl.replace("{t}", t)
            record(f"{prefix}pair/{key}/{t}/{pos}", "pair:" + family, "sql", pos, key, sql, q(sql))
        for ck, pred in cells.CONSTS:
            sel = f"SELECT id, {pred} AS r FROM {t}"
            record(f"{prefix}const/{ck}/{t}/select", "const", "sql", "select", ck, sel, q(sel))
            wh_sql = f"SELECT id FROM {t} WHERE {pred}"
            record(f"{prefix}const/{ck}/{t}/where", "const", "sql", "where", ck, wh_sql, q(wh_sql))
        for group, table in (
            ("three", cells.THREE),
            ("agg", cells.AGGS),
            ("literal", cells.LITERALS),
            ("special", cells.SPECIAL),
        ):
            for ck, tmpl in table.items():
                sql = tmpl.replace("{t}", t)
                record(f"{prefix}{group}/{ck}/{t}", group, "sql", ck, ck, sql, q(sql))
    for ck, tmpl in cells.VALUES.items():
        sql = tmpl.replace("{t}", "t")
        record(f"{prefix}values/{ck}", "values", "sql", ck, ck, sql, q(sql))


def frame_cells(t: str) -> None:
    def tab() -> Any:
        return s.table(t)

    for key in ("ac", "ab", "bc", "gh", "i64h", "i32w30", "c0c"):
        _, x, y, family, _ = cells.pair(key)
        cx, cy = sf.col(x), sf.col(y)
        p = f"df/{key}/{t}"
        g = "df:" + family
        record(
            f"{p}/union",
            g,
            "dataframe",
            "union",
            key,
            "union",
            lambda x=x, y=y: answer_of(
                tab().select(sf.col(x).alias("v")).union(tab().select(sf.col(y).alias("v")))
            ),
        )
        record(
            f"{p}/unionByName",
            g,
            "dataframe",
            "unionByName",
            key,
            "unionByName",
            lambda x=x, y=y: answer_of(
                tab().select(sf.col(y).alias("v")).unionByName(tab().select(sf.col(x).alias("v")))
            ),
        )
        record(
            f"{p}/when",
            g,
            "dataframe",
            "when",
            key,
            "when/otherwise",
            lambda cx=cx, cy=cy: answer_of(
                tab().select("id", sf.when(sf.col("id") % 2 == 0, cx).otherwise(cy).alias("r"))
            ),
        )
        record(
            f"{p}/isin",
            g,
            "dataframe",
            "isin",
            key,
            "isin(col)",
            lambda cx=cx, cy=cy: answer_of(tab().select("id", cy.isin(cx).alias("r"))),
        )
        record(
            f"{p}/between",
            g,
            "dataframe",
            "between",
            key,
            "between(col, col)",
            lambda cx=cx, cy=cy: answer_of(tab().select("id", cx.between(cy, cy).alias("r"))),
        )
        record(
            f"{p}/filter_eq",
            g,
            "dataframe",
            "filter_eq",
            key,
            "filter(x == y)",
            lambda cx=cx, cy=cy: answer_of(tab().filter(cx == cy).select("id")),
        )
        record(
            f"{p}/select_eq",
            g,
            "dataframe",
            "select_eq",
            key,
            "select(y == x)",
            lambda cx=cx, cy=cy: answer_of(tab().select("id", (cy == cx).alias("r"))),
        )
        record(
            f"{p}/coalesce",
            g,
            "dataframe",
            "coalesce",
            key,
            "F.coalesce(y, x)",
            lambda cx=cx, cy=cy: answer_of(tab().select("id", sf.coalesce(cy, cx).alias("r"))),
        )
        record(
            f"{p}/greatest",
            g,
            "dataframe",
            "greatest",
            key,
            "F.greatest(x, y)",
            lambda cx=cx, cy=cy: answer_of(tab().select("id", sf.greatest(cx, cy).alias("r"))),
        )
        record(
            f"{p}/join",
            g,
            "dataframe",
            "join",
            key,
            "join(l.x == r.y)",
            lambda x=x, y=y: answer_of(
                tab()
                .select(sf.col("id").alias("li"), sf.col(x).alias("lx"))
                .join(
                    tab().select(sf.col("id").alias("ri"), sf.col(y).alias("ry")),
                    sf.col("lx") == sf.col("ry"),
                )
                .select("li", "ri")
            ),
        )
        record(
            f"{p}/fexpr_eq",
            "fexpr:" + family,
            "fexpr",
            "fexpr_eq",
            key,
            f"F.expr({x} = {y})",
            lambda x=x, y=y: answer_of(tab().select("id", sf.expr(f"{x} = {y}").alias("r"))),
        )
        record(
            f"{p}/fexpr_coalesce",
            "fexpr:" + family,
            "fexpr",
            "fexpr_coalesce",
            key,
            f"F.expr(coalesce({y}, {x}))",
            lambda x=x, y=y: answer_of(
                tab().select("id", sf.expr(f"coalesce({y}, {x})").alias("r"))
            ),
        )
        record(
            f"{p}/fexpr_filter",
            "fexpr:" + family,
            "fexpr",
            "fexpr_filter",
            key,
            f"filter(F.expr({x} = {y}))",
            lambda x=x, y=y: answer_of(tab().filter(sf.expr(f"{x} = {y}")).select("id")),
        )
    lit_a1 = sf.lit("1").cast("decimal(38,0)")
    lit_c125 = sf.lit("1.25").cast("decimal(10,2)")
    consts = {
        "df_c_eq_a1": lambda: tab().filter(sf.col("c") == lit_a1),
        "df_a1_eq_c": lambda: tab().filter(lit_a1 == sf.col("c")),
        "df_a_eq_c125": lambda: tab().filter(sf.col("a") == lit_c125),
        "df_c125_eq_a": lambda: tab().filter(lit_c125 == sf.col("a")),
        "df_c_isin_a1": lambda: tab().filter(sf.col("c").isin(lit_a1)),
        "df_a_eq_pydec": lambda: tab().filter(sf.col("a") == Decimal("1.25")),
        "df_a_isin_pydec": lambda: tab().filter(sf.col("a").isin(Decimal("1.25"), Decimal("2.5"))),
        "df_c_between_a1": lambda: tab().filter(sf.col("c").between(lit_a1, lit_a1)),
        "fexpr_c_eq_a1": lambda: tab().filter(sf.expr("c = CAST('1' AS DECIMAL(38,0))")),
        "fexpr_a_eq_lit": lambda: tab().filter(sf.expr("a = 1.25")),
    }
    for ck, build in consts.items():
        door = "fexpr" if ck.startswith("fexpr") else "dataframe"
        record(
            f"dfconst/{ck}/{t}",
            "dfconst",
            door,
            ck,
            ck,
            ck,
            lambda b=build: answer_of(b().select("id")),
        )


def iceberg_cells() -> None:
    src = cells.ICE_SRC
    cols = ", ".join(f"{n} {ty}" for n, ty, _ in cells.COLUMNS)
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    s.sql(f"CREATE TABLE {src} ({cols}) USING iceberg").collect()
    s.sql(f"INSERT INTO {src} SELECT * FROM t").collect()
    for ck, (create, stmt) in cells.ICE_READS.items():
        n = f"lc.ns.r_{ck}"
        sql = stmt.format(n=n, s=src)
        rec = {
            "key": f"ice/{ck}",
            "group": "iceberg",
            "door": "sql",
            "pos": ck,
            "pair": ck,
            "sql": sql,
        }
        if create:
            setup = attempt(lambda c=create, n=n: s.sql(c.format(n=n)).collect())
            if isinstance(setup, str):
                rec["result"] = "SETUP-" + setup
                records.append(rec)
                continue
        st = attempt(lambda sql=sql: s.sql(sql).collect())
        rec["statement"] = st if isinstance(st, str) else "ok"
        rec["result"] = attempt(lambda n=n: answer_of(s.table(n)))
        records.append(rec)
    mcols = (
        "id INT, a DECIMAL(38,0), c DECIMAL(10,2), i64 BIGINT, h DECIMAL(38,20), a2 "
        "DECIMAL(38,2), tag STRING"
    )
    for mode, props in cells.MODES.items():
        for ck, stmt in cells.ICE_DML.items():
            m = f"lc.ns.m_{mode}_{ck}"
            sql = stmt.format(m=m, s=src)
            rec = {
                "key": f"icedml/{mode}/{ck}",
                "group": "icedml",
                "door": "sql",
                "pos": ck,
                "pair": mode,
                "sql": sql,
            }
            setup = attempt(
                lambda m=m, props=props: (
                    s.sql(
                        f"CREATE TABLE {m} ({mcols}) USING iceberg TBLPROPERTIES ({props})"
                    ).collect(),
                    s.sql(
                        f"INSERT INTO {m} SELECT id, a, c, i64, h, "
                        "CAST(NULL AS DECIMAL(38,2)), 'o' FROM t"
                    ).collect(),
                )
            )
            if isinstance(setup, str):
                rec["result"] = "SETUP-" + setup
                records.append(rec)
                continue
            st = attempt(lambda sql=sql: s.sql(sql).collect())
            rec["statement"] = st if isinstance(st, str) else "ok"
            rec["result"] = attempt(lambda m=m: answer_of(s.sql(f"SELECT id, a2, tag FROM {m}")))
            records.append(rec)
        for ck, (col, stmt) in cells.PART_DML.items():
            qn = f"lc.ns.pd_{mode}_{ck}"
            sql = stmt.format(q=qn)
            rec = {
                "key": f"icepart/{mode}/{ck}",
                "group": "icepart",
                "door": "sql",
                "pos": ck,
                "pair": mode,
                "sql": sql,
            }
            setup = attempt(
                lambda qn=qn, col=col, props=props: (
                    s.sql(
                        f"CREATE TABLE {qn} (id INT, a DECIMAL(38,0), c DECIMAL(10,2), "
                        f"h DECIMAL(38,20)) USING iceberg PARTITIONED BY ({col}) "
                        f"TBLPROPERTIES ({props})"
                    ).collect(),
                    s.sql(f"INSERT INTO {qn} SELECT id, a, c, h FROM t").collect(),
                )
            )
            if isinstance(setup, str):
                rec["result"] = "SETUP-" + setup
                records.append(rec)
                continue
            st = attempt(lambda sql=sql: s.sql(sql).collect())
            rec["statement"] = st if isinstance(st, str) else "ok"
            rec["result"] = attempt(lambda qn=qn: answer_of(s.sql(f"SELECT id FROM {qn}")))
            records.append(rec)
    made = set()
    for ck, (col, stmt) in cells.PART_READS.items():
        qn = f"lc.ns.p_{col}"
        if col not in made:
            s.sql(
                f"CREATE TABLE {qn} (id INT, a DECIMAL(38,0), c DECIMAL(10,2), "
                f"h DECIMAL(38,20)) USING iceberg PARTITIONED BY ({col})"
            ).collect()
            s.sql(f"INSERT INTO {qn} SELECT id, a, c, h FROM t").collect()
            made.add(col)
        sql = stmt.format(q=qn)
        record(f"icepart/read/{ck}", "icepart", "sql", ck, col, sql, q(sql))


def legacy_cells() -> None:
    key = cells.LEGACY_KEY
    set_res = attempt(lambda: s.sql(f"SET {key}=true").collect() and "ok")
    records.append(
        {
            "key": "legacy/set",
            "group": "legacy",
            "result": set_res if isinstance(set_res, str) else "ok",
        }
    )
    for t in ("t", "tx"):
        for pk, family, pos, tmpl in cells.pair_cells():
            if pos not in cells.LEGACY_POS:
                continue
            sql = tmpl.replace("{t}", t)
            record(f"legacy/pair/{pk}/{t}/{pos}", "legacy:" + family, "sql", pos, pk, sql, q(sql))
    pools = dict(cells.VALUES)
    pools.update(cells.THREE)
    pools.update(cells.AGGS)
    pools.update(cells.LITERALS)
    pools.update({k: f"SELECT id FROM {{t}} WHERE {v}" for k, v in cells.CONSTS})
    for ck in cells.LEGACY_EXTRA:
        sql = pools[ck].replace("{t}", "t")
        record(f"legacy/extra/{ck}", "legacy", "sql", ck, ck, sql, q(sql))
    n = "lc.ns.legacy_insert"
    s.sql(f"CREATE TABLE {n} (x DECIMAL(38,2)) USING iceberg").collect()
    st = attempt(
        lambda: s.sql(
            f"INSERT INTO {n} SELECT a FROM {cells.ICE_SRC} UNION ALL SELECT c FROM {cells.ICE_SRC}"
        ).collect()
    )
    records.append(
        {
            "key": "legacy/ice_insert_union",
            "group": "legacy",
            "pos": "ice_insert_union",
            "statement": st if isinstance(st, str) else "ok",
            "result": attempt(lambda: answer_of(s.table(n))),
        }
    )
    reset = attempt(lambda: s.sql(f"RESET {key}").collect() and "ok")
    records.append(
        {
            "key": "legacy/reset",
            "group": "legacy",
            "result": reset if isinstance(reset, str) else "ok",
        }
    )
    records.append(
        {
            "key": "legacy/after_reset_conf",
            "group": "legacy",
            "result": attempt(lambda: s.conf.get(key, "unset")),
        }
    )
    for pos in ("union_all", "eq"):
        sql = cells.PLANS.get(pos) or ("SELECT id, " + cells.EXPRS[pos] + " AS r FROM {t}")
        sql = sql.replace("{x}", "a").replace("{y}", "c").replace("{t}", "t")
        record(f"legacy/after_reset/{pos}", "legacy", "sql", pos, "ac", sql, q(sql))


start = time.time()
base = s.range(8).selectExpr(*cells.setup_exprs())
for name, cond in (("t", "id <= 6"), ("tx", "id >= 7")):
    path = str(Path(wh) / f"pq_{name}")
    base.filter(cond).write.parquet(path)
    s.read.parquet(path).createOrReplaceTempView(name)
schema = {
    name: [f"{f.name}:{f.dataType.simpleString()}" for f in s.table(name).schema.fields]
    for name in ("t", "tx")
}
records.append(dict(key="provenance", ansi=ansi, warehouse=Path(wh).name, schema=schema, **prov))
records.append({"key": "data/t", "result": attempt(lambda: answer_of(s.table("t")))})
records.append({"key": "data/tx", "result": attempt(lambda: answer_of(s.table("tx")))})
print("setup", round(time.time() - start, 1), flush=True)
run_sql_cells("", ("t", "tx"))
print("sql", len(records), round(time.time() - start, 1), flush=True)
frame_cells("t")
frame_cells("tx")
print("frames", len(records), round(time.time() - start, 1), flush=True)
iceberg_cells()
print("iceberg", len(records), round(time.time() - start, 1), flush=True)
legacy_cells()
records.append({"key": "elapsed", "seconds": round(time.time() - start, 1)})
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), round(time.time() - start, 1), wh)
