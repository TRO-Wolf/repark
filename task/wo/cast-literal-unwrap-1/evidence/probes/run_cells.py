import datetime
import json
import os
import sys
import tempfile
import time
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import cells

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
only = os.environ.get("KIND_ONLY", "")
wh = tempfile.mkdtemp(prefix=f"wh-{engine}-{ansi}-", dir=wh_parent)
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
MODES = {
    "cow": "'format-version'='2', 'write.delete.mode'='copy-on-write', "
    "'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write'",
    "mor": "'format-version'='2', 'write.delete.mode'='merge-on-read', "
    "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read'",
}
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
    import repark._native as native
    from repark import ReparkSession as Builder
    from repark import functions as sf

    print("native", native.__file__, flush=True)
    builder = Builder.builder.appName("cast-unwrap")
    prov = {
        "engine": "repark",
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
records = [dict(key="provenance", ansi=ansi, warehouse=Path(wh).name, **prov)]


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
    head = " ".join(str(e).split())[:200]
    return f"ERR:{type(e).__name__}:{cls}:{head}"


def show(v: object) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return str(v).lower()
    return str(v)


def rows_of(frame: Any) -> list:
    rows = frame.collect()
    if len(frame.columns) == 1:
        return sorted(int(r[0]) for r in rows)
    return sorted(":".join(show(x) for x in r) for r in rows)


def answer(run: Callable[[], object]) -> object:
    try:
        return run()
    except Exception as e:
        return err(e)


def record(
    key: str, kind: str, storage: str, door: str, pos: str, sql: str, run: Callable[[], object]
) -> None:
    t0 = time.time()
    res = answer(run)
    records.append(
        {
            "key": key,
            "kind": kind,
            "storage": storage,
            "door": door,
            "pos": pos,
            "sql": sql,
            "result": res,
            "ms": round((time.time() - t0) * 1000, 1),
        }
    )


def pyval(v: object) -> object:
    if isinstance(v, str) and v.startswith("date:"):
        return datetime.date.fromisoformat(v[5:])
    if isinstance(v, str) and v.startswith("ts:"):
        return datetime.datetime.fromisoformat(v[3:])
    if isinstance(v, str):
        return Decimal(v)
    return v


def run_sql(sql: str) -> Callable[[], object]:
    return lambda: rows_of(s.sql(sql))


def setup(kind: cells.Kind) -> dict:
    name = kind.name
    views = {}
    s.sql(cells.select_sql(kind)).createOrReplaceTempView(f"v_{name}")
    views["view"] = f"v_{name}"
    path = str(Path(wh) / f"pq_{name}")
    s.table(f"v_{name}").write.parquet(path)
    s.read.parquet(path).createOrReplaceTempView(f"p_{name}")
    views["parquet"] = f"p_{name}"
    u = f"lc.ns.u_{name}"
    s.sql(f"CREATE TABLE {u} (id INT, c {kind.col_type}) USING iceberg").collect()
    for i, v in enumerate(kind.values):
        s.sql(f"INSERT INTO {u} VALUES ({i + 1}, CAST({v} AS {kind.col_type}))").collect()
    views["ice_files"] = u
    q = f"lc.ns.q_{name}"
    s.sql(
        f"CREATE TABLE {q} (id INT, c {kind.col_type}) USING iceberg PARTITIONED BY (c)"
    ).collect()
    s.sql(f"INSERT INTO {q} SELECT id, c FROM v_{name}").collect()
    views["ice_part"] = q
    return views


def frame_cells(kind: cells.Kind, storage: str, table: str) -> None:
    name = kind.name
    pref = f"{name}/{storage}/df"
    cast = cells.cast_expr(kind)
    const = kind.consts[0]

    def base() -> Any:
        return s.table(table)

    def ids(frame: Callable[[], Any]) -> Callable[[], object]:
        return lambda: sorted(int(r[0]) for r in frame().select("id").collect())

    if kind.pylit is not None:
        lit = pyval(kind.pylit)
        a, b = (pyval(x) for x in kind.pyin)
        lo, hi = (pyval(x) for x in kind.pybetween)

        def tcol() -> Any:
            return sf.col("c").cast(kind.df_type)

        record(
            f"{pref}/col_eq",
            name,
            storage,
            "dataframe",
            "where",
            f"col.cast == {lit!r}",
            ids(lambda: base().filter(tcol() == lit)),
        )
        record(
            f"{pref}/col_gt",
            name,
            storage,
            "dataframe",
            "where",
            f"col.cast > {lit!r}",
            ids(lambda: base().filter(tcol() > lit)),
        )
        record(
            f"{pref}/lit_eq",
            name,
            storage,
            "dataframe",
            "where",
            "F.lit == col.cast",
            ids(lambda: base().filter(sf.lit(lit) == tcol())),
        )
        record(
            f"{pref}/isin",
            name,
            storage,
            "dataframe",
            "where",
            f"isin({a!r}, {b!r})",
            ids(lambda: base().filter(tcol().isin(a, b))),
        )
        record(
            f"{pref}/between",
            name,
            storage,
            "dataframe",
            "where",
            f"between({lo!r}, {hi!r})",
            ids(lambda: base().filter(tcol().between(lo, hi))),
        )
        record(
            f"{pref}/select_eq",
            name,
            storage,
            "dataframe",
            "select",
            "select col.cast == lit",
            lambda: rows_of(base().select("id", (tcol() == lit).alias("x"))),
        )
    pred = f"{cast} = {const}"
    record(
        f"{pref}/fexpr_eq",
        name,
        storage,
        "fexpr",
        "where",
        pred,
        ids(lambda: base().filter(sf.expr(pred))),
    )
    record(
        f"{pref}/fexpr_select",
        name,
        storage,
        "fexpr",
        "select",
        pred,
        lambda: rows_of(base().select("id", sf.expr(pred).alias("x"))),
    )
    record(
        f"{pref}/filter_str_eq",
        name,
        storage,
        "filter_str",
        "where",
        pred,
        ids(lambda: base().filter(pred)),
    )
    gt = f"{cast} > {const}"
    record(
        f"{pref}/filter_str_gt",
        name,
        storage,
        "filter_str",
        "where",
        gt,
        ids(lambda: base().filter(gt)),
    )


def dml_cells(kind: cells.Kind, view: str) -> None:
    name = kind.name
    const = kind.consts[0]
    cast_t = cells.cast_expr(kind, "t.c")
    n = len(kind.values)
    src = "(SELECT * FROM VALUES " + ", ".join(f"({i + 1})" for i in range(n)) + " AS x(sid))"
    stmts = {
        "delete_eq": "DELETE FROM {t} WHERE " + cells.predicate(kind, "eq", const, "r"),
        "delete_gt": "DELETE FROM {t} WHERE " + cells.predicate(kind, "gt", const, "r"),
        "update_eq": "UPDATE {t} SET tag = 'u' WHERE " + cells.predicate(kind, "eq", const, "r"),
        "merge_on": "MERGE INTO {t} t USING (SELECT 1 AS k) s ON "
        + f"{cast_t} = {const} WHEN MATCHED THEN UPDATE SET tag = 'm'",
        "merge_and": "MERGE INTO {t} t USING "
        + src
        + f" s ON t.id = s.sid WHEN MATCHED AND {cast_t} = {const} THEN UPDATE SET tag = 'm'",
    }
    for mode, props in MODES.items():
        for sname, stmt in stmts.items():
            t = f"lc.ns.m_{name}_{mode}_{sname}"
            sql = stmt.format(t=t)
            rec = {
                "key": f"{name}/dml/{mode}/{sname}",
                "kind": name,
                "storage": f"ice_{mode}",
                "door": "sql",
                "pos": sname,
                "sql": sql,
            }
            try:
                s.sql(
                    f"CREATE TABLE {t} (id INT, c {kind.col_type}, tag STRING) USING iceberg "
                    f"TBLPROPERTIES ({props})"
                ).collect()
                s.sql(f"INSERT INTO {t} SELECT id, c, 'o' FROM {view}").collect()
            except Exception as e:
                rec["result"] = "SETUP-" + err(e)
                records.append(rec)
                continue
            try:
                s.sql(sql).collect()
                rec["statement"] = "ok"
            except Exception as e:
                rec["statement"] = err(e)
            rec["result"] = answer(
                lambda t=t: sorted(
                    f"{r[0]}:{r[1]}" for r in s.sql(f"SELECT id, tag FROM {t}").collect()
                )
            )
            records.append(rec)


s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
start = time.time()
for kind in cells.KINDS:
    if only and kind.name not in only.split(","):
        continue
    try:
        views = setup(kind)
    except Exception as e:
        records.append({"key": f"{kind.name}/setup", "kind": kind.name, "result": err(e)})
        continue
    for storage, table in views.items():
        dense = storage == "view"
        for ckey, pred, _const in cells.where_cells(kind, dense):
            sql = f"SELECT id FROM {table} WHERE {pred}"
            record(
                f"{kind.name}/{storage}/where/{ckey}",
                kind.name,
                storage,
                "sql",
                "where",
                sql,
                run_sql(sql),
            )
        positions = cells.position_cells(kind, table, table)
        if not dense:
            positions = [p for p in positions if p[0] in ("select/eq", "join/eq")]
        for pkey, sql in positions:
            record(
                f"{kind.name}/{storage}/{pkey}",
                kind.name,
                storage,
                "sql",
                pkey.split("/")[0],
                sql,
                run_sql(sql),
            )
    frame_cells(kind, "view", views["view"])
    frame_cells(kind, "ice_part", views["ice_part"])
    if engine != "spark":
        for storage in ("view", "ice_part"):
            sql = f"EXPLAIN SELECT id FROM {views[storage]} WHERE " + cells.predicate(
                kind, "eq", kind.consts[0], "r"
            )
            records.append(
                {
                    "key": f"{kind.name}/{storage}/explain",
                    "kind": kind.name,
                    "storage": storage,
                    "door": "sql",
                    "pos": "explain",
                    "sql": sql,
                    "result": answer(
                        lambda q=sql: [
                            " ".join(str(r[-1]).split())[:900] for r in s.sql(q).collect()
                        ]
                    ),
                }
            )
    if kind.dml:
        dml_cells(kind, views["view"])
    print(kind.name, len(records), round(time.time() - start, 1), flush=True)
records.append({"key": "elapsed", "seconds": round(time.time() - start, 1)})
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), round(time.time() - start, 1), wh)
