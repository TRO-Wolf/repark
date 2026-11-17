import json
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import cells
import lib
import rev_cells

engine, ansi, wh_parent, jar, out_dir = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"rev-{engine}-{ansi}-", dir=wh_parent)
s, sf, Window, prov = lib.session(engine, ansi, wh, jar, {})
records: list = [dict(key="provenance", ansi=ansi, conf="base", **prov)]


def record(key: str, family: str, door: str, pos: str, col: str, run: Callable) -> None:
    records.append(
        {
            "key": key,
            "family": family,
            "door": door,
            "pos": pos,
            "col": col,
            "result": lib.attempt(run),
        }
    )


def make_table(name: str, names: list, rows: list, types: dict) -> None:
    raw = s.createDataFrame(rows, ", ".join(f"{n} STRING" for n in names))
    typed = raw.selectExpr(*[f"CAST({n} AS {types[n]}) AS {n}" for n in names])
    path = str(Path(wh) / f"pq_{name}")
    typed.write.parquet(path)
    s.read.parquet(path).createOrReplaceTempView(name)


an, ar = cells.avg_rows()
make_table("t", an, ar, cells.avg_types())
dn, dr = cells.div_rows()
make_table("v", dn, dr, cells.div_types())
make_table(
    "s04",
    ["k", "x"],
    [[str(i), x] for i, x in enumerate(rev_cells.S04)],
    {"k": "INT", "x": "DECIMAL(38,0)"},
)
SQL = rev_cells.mean_cells("t") + rev_cells.shape_cells("t", "v")
SQL.append(("sum/s04", "sum", "nonsticky", "x", "SELECT avg(x) AS m FROM s04"))
SQL.append(("sum/s04_mean", "sum", "nonsticky", "x", "SELECT mean(x) AS m FROM s04"))
SQL.append(
    ("sum/s04_head", "sum", "overflow_end", "x", "SELECT avg(x) AS m FROM s04 WHERE k IN (0, 1, 4)")
)
for key, fam, pos, col, sql in SQL:
    record(key, fam, "sql", pos, col, lambda q=sql: lib.answer_of(s.sql(q)))
NORMAL_G = list(cells.NORMAL)


def tg() -> Any:
    return s.table("t").where(sf.col("g").isin(NORMAL_G))


for c in [cells.col_name(p, sc) for p, sc in cells.AVG_COLS]:
    forms = {
        "agg_F.expr_mean": lambda c=c: tg().groupBy("g").agg(sf.expr(f"mean({c})").alias("m")),
        "selectExpr_mean": lambda c=c: s.table("t").where("g = 2").selectExpr(f"mean({c}) AS m"),
        "selectExpr_avg": lambda c=c: s.table("t").where("g = 2").selectExpr(f"avg({c}) AS m"),
        "select_F.expr_mean": lambda c=c: s.table("t").where("g = 2").select(sf.expr(f"mean({c})")),
        "select_F.expr_avg": lambda c=c: s.table("t").where("g = 2").select(sf.expr(f"avg({c})")),
        "agg_dict_mean": lambda c=c: tg().groupBy("g").agg({c: "mean"}),
    }
    for form, build in forms.items():
        record(
            f"meandf/{form}/{c}", "mean", "dataframe", form, c, lambda b=build: lib.answer_of(b())
        )
for p, sc in cells.DIV_COLS:
    d = f"d{p}_{sc}"
    forms = {
        "round_col_div": lambda d=d: s.table("v").select(
            "id", sf.round(sf.col(d) / 3, 2).alias("q")
        ),
        "pct": lambda d=d: s.table("v").select("id", (sf.col(d) % 3).alias("q")),
        "F.pmod": lambda d=d: s.table("v").select("id", sf.pmod(sf.col(d), sf.lit(3)).alias("q")),
        "F.expr_mod": lambda d=d: s.table("v").select("id", sf.expr(f"mod({d}, 3)").alias("q")),
        "F.expr_pmod": lambda d=d: s.table("v").select("id", sf.expr(f"pmod({d}, 3)").alias("q")),
        "F.expr_fold": lambda d=d: s.table("v").select("id", sf.expr(f"{d} / (1 + 2)").alias("q")),
    }
    for form, build in forms.items():
        record(f"moddf/{form}/{d}", "mod", "dataframe", form, d, lambda b=build: lib.answer_of(b()))


def ice() -> None:
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    s.sql("CREATE TABLE lc.ns.vt USING iceberg AS SELECT * FROM v").collect()
    for d in rev_cells.DIV_SAMPLE:
        record(
            f"ice/where_div/{d}",
            "div",
            "iceberg",
            "where_div",
            d,
            lambda d=d: lib.answer_of(s.sql(f"SELECT id FROM lc.ns.vt WHERE {d} / 3 = 0.836667")),
        )
    for c in ("c10_2", "c28_2"):
        n = f"lc.ns.ctas_mean_{c}"
        st = lib.attempt(
            lambda c=c, n=n: s.sql(
                f"CREATE TABLE {n} USING iceberg AS SELECT g, mean({c}) AS m FROM t "
                f"WHERE g IN ({cells.normal_list()}) GROUP BY g"
            ).collect()
        )
        rec = {
            "key": f"ice/ctas_mean/{c}",
            "family": "mean",
            "door": "iceberg",
            "pos": "ctas",
            "col": c,
        }
        rec["statement"] = st if isinstance(st, str) else "ok"
        rec["result"] = lib.attempt(lambda n=n: lib.answer_of(s.sql(f"SELECT g, m FROM {n}")))
        rec["schema"] = lib.attempt(
            lambda n=n: [f.dataType.simpleString() for f in s.table(n).schema.fields]
        )
        records.append(rec)


ice()
Path(out_dir).mkdir(parents=True, exist_ok=True)
with (Path(out_dir) / f"{engine}-{ansi}-base.jsonl").open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
if engine == "repark":
    import repark

    def view_sql(names: list, rows: list, types: dict) -> str:
        body = ", ".join(
            "(" + ", ".join("NULL" if x is None else f"'{x}'" for x in r) + ")" for r in rows
        )
        items = ", ".join(f"CAST(column{n + 1} AS {types[m]}) AS {m}" for n, m in enumerate(names))
        return f"SELECT {items} FROM (VALUES {body})"

    native = [{"key": "provenance", "engine": "native", "native": prov["native"]}]
    for name, (ns, rs, ts) in {
        "nt": (an, ar, cells.avg_types()),
        "nv": (dn, dr, cells.div_types()),
    }.items():
        create = f"CREATE VIEW {name} AS {view_sql(ns, rs, ts)}"
        native.append(
            {
                "key": f"setup/{name}",
                "result": lib.attempt(lambda q=create: repark.sql(q).collect() and "ok"),
            }
        )
    for key, fam, pos, col, sql in SQL:
        if fam not in ("mixed", "mod") and not (fam == "mean" and pos in ("grouped", "distinct")):
            continue
        if col not in ("c10_2", "c28_2", "d10_2", "d28_2"):
            continue
        q = sql.replace(" FROM t ", " FROM nt ").replace(" FROM v", " FROM nv")
        native.append(
            {
                "key": key,
                "family": fam,
                "door": "native",
                "pos": pos,
                "col": col,
                "result": lib.attempt(lambda q=q: lib.answer_of(repark.sql(q))),
            }
        )
    with (Path(out_dir) / "native.jsonl").open("w") as fh:
        for r in native:
            fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), flush=True)
