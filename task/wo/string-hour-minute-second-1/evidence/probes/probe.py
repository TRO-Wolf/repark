import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from cells import (
    CONFIGS,
    FUNCS,
    G_DOORS,
    G_FUNCS,
    G_SHAPES,
    INPUTS,
    P_DOORS,
    SHAPES,
    func_map,
    sql_text,
)

JAR = os.environ.get("ICEBERG_JAR", "")
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")


def condition(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return found.group(1) if found else text[:160]


def session(engine: str, wh: str) -> tuple[Any, Any]:
    if engine == "spark":
        import pyspark.sql.functions as fn
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", JAR)
            .config("spark.sql.extensions", EXT)
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.warehouse.dir", wh + "/sw")
        )
    else:
        import repark._native as native
        import repark.spark.functions as fn
        from repark import ReparkSession

        print("NATIVE", native.__file__, flush=True)
        builder = ReparkSession.builder.appName("string-hms")
    s = (
        builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", wh + "/lc")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    if engine == "spark":
        s.sparkContext.setLogLevel("ERROR")
    return s, fn


class Out:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.handle = self.path.open("a")

    def emit(self, record: dict[str, Any]) -> None:
        self.handle.write(json.dumps(record) + "\n")
        self.handle.flush()


def text(value: Any) -> str:
    return "None" if value is None else str(value)


def frame_of(s: Any, rows: list[tuple[int, str | None]]) -> Any:
    return s.createDataFrame(rows, "k INT, c STRING")


def setup(s: Any, shared: str) -> None:
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    for sid, value in SHAPES.items():
        s.sql("CREATE TABLE lc.db.s_" + sid + " (k INT, c STRING) USING iceberg").collect()
        s.sql("INSERT INTO lc.db.s_" + sid + " VALUES (1, " + sql_text(value) + ")").collect()
    s.sql("CREATE TABLE lc.db.g (k INT, c STRING) USING iceberg").collect()
    rows = ", ".join(
        "(" + str(i) + ", " + sql_text(SHAPES[sid]) + ")" for i, sid in enumerate(G_SHAPES)
    )
    s.sql("INSERT INTO lc.db.g VALUES " + rows).collect()
    if shared != "-":
        for sid, value in SHAPES.items():
            frame_of(s, [(1, value)]).coalesce(1).write.parquet(shared + "/" + sid)
        g_rows = [(i, SHAPES[sid]) for i, sid in enumerate(G_SHAPES)]
        frame_of(s, g_rows).coalesce(1).write.parquet(shared + "/g")


def configure(s: Any, cfg: str) -> None:
    ansi, zone, ttype = CONFIGS[cfg]
    s.sql("SET spark.sql.ansi.enabled=" + ansi).collect()
    s.sql("SET spark.sql.session.timeZone=" + zone).collect()
    s.sql("SET spark.sql.timestampType=" + (ttype or "TIMESTAMP_LTZ")).collect()


def sources(s: Any, shared: str, sid: str) -> dict[str, tuple[str | None, Any]]:
    value = SHAPES[sid] if sid != "g" else None
    rows = [(i, SHAPES[x]) for i, x in enumerate(G_SHAPES)] if sid == "g" else [(1, value)]
    view = frame_of(s, rows)
    view.createOrReplaceTempView("fv_" + sid)
    pq = s.read.parquet(shared + "/" + sid)
    pq.createOrReplaceTempView("pv_" + sid)
    table = "lc.db.s_" + sid if sid != "g" else "lc.db.g"
    return {
        "I": (table, s.table(table)),
        "P": ("pv_" + sid, pq),
        "V": ("fv_" + sid, view),
    }


def projection_plan(
    door: str, src: str | None, df: Any, ref_sql: str, ref_col: Any
) -> list[tuple[str, Any]]:
    plans = []
    for fid, sql, build in FUNCS:
        expr = sql.replace("{c}", ref_sql)
        plans.append((fid, (door, expr, build, src, df, ref_col)))
    return plans


def run_projection(s: Any, f: Any, items: list[tuple[str, Any]]) -> list[tuple[str, Any]]:
    door, _, _, src, df, ref_col = items[0][1]
    names = ["a" + str(i).zfill(2) for i in range(len(items))]
    if door == "sql":
        cols = ", ".join(
            spec[1] + " AS " + name for (_, spec), name in zip(items, names, strict=True)
        )
        frame = s.sql("SELECT " + cols + (" FROM " + src if src else ""))
    elif door == "fcol":
        frame = df.select(
            *[spec[2](f, ref_col).alias(name) for (_, spec), name in zip(items, names, strict=True)]
        )
    elif door == "sexpr":
        frame = df.selectExpr(
            *[spec[1] + " AS " + name for (_, spec), name in zip(items, names, strict=True)]
        )
    else:
        frame = df.select(
            *[f.expr(spec[1]).alias(name) for (_, spec), name in zip(items, names, strict=True)]
        )
    rows = frame.collect()
    types = [kind for _, kind in frame.dtypes]
    if len(rows) != 1:
        return [(fid, {"rows": len(rows)}) for fid, _ in items]
    return [(fid, {"v": text(rows[0][i]), "t": types[i]}) for i, (fid, _) in enumerate(items)]


def answer_projection(s: Any, f: Any, items: list[tuple[str, Any]]) -> list[tuple[str, Any]]:
    try:
        return run_projection(s, f, items)
    except Exception as error:
        if len(items) == 1:
            return [(items[0][0], {"err": condition(error)})]
    out = []
    for item in items:
        out.extend(answer_projection(s, f, [item]))
    return out


def projection(s: Any, f: Any, shared: str, cfg: str, inputs: str, out: Out) -> None:
    for sid, value in SHAPES.items():
        srcs = sources(s, shared, sid)
        for inp in inputs:
            if inp == "L":
                src, df = None, s.sql("SELECT 1 AS k")
                ref_sql = sql_text(value)
                ref_col = f.lit(value) if value is not None else f.lit(None).cast("string")
            else:
                src, df = srcs[inp]
                ref_sql, ref_col = "c", f.col("c")
            for door in P_DOORS:
                started = time.time()
                items = projection_plan(door, src, df, ref_sql, ref_col)
                for fid, got in answer_projection(s, f, items):
                    record = {"cfg": cfg, "in": inp, "sh": sid, "door": door, "fn": fid}
                    record.update(got)
                    out.emit(record)
                print("P", cfg, inp, sid, door, round(time.time() - started, 2), flush=True)


def g_frame(s: Any, f: Any, door: str, fid: str, src: str, df: Any) -> tuple[Any, bool]:
    sql, build = func_map()[fid]
    lit_sql, lit_build = G_FUNCS[fid]
    expr = sql.replace("{c}", "c")
    col = build(f, f.col("c"))
    if door == "where_sql":
        return s.sql("SELECT k FROM " + src + " WHERE " + expr + " = " + lit_sql), False
    if door == "group_sql":
        query = "SELECT CAST(" + expr + " AS STRING) AS g, count(*) AS n FROM " + src
        return s.sql(query + " GROUP BY " + expr), False
    if door == "order_sql":
        return s.sql("SELECT k FROM " + src + " ORDER BY " + expr + " ASC NULLS FIRST, k"), True
    if door == "where_f":
        return df.filter(col == lit_build(f)).select("k"), False
    if door == "group_f":
        return df.groupBy(col.cast("string").alias("g")).count(), False
    return df.orderBy(col.asc_nulls_first(), f.col("k")).select("k"), True


def g_doors(s: Any, f: Any, shared: str, cfg: str, out: Out) -> None:
    srcs = sources(s, shared, "g")
    for inp in ("I", "P", "V"):
        src, df = srcs[inp]
        for fid in G_FUNCS:
            for door in G_DOORS:
                record: dict[str, Any] = {"cfg": cfg, "in": inp, "sh": "g", "door": door}
                record["fn"] = fid
                try:
                    frame, ordered = g_frame(s, f, door, fid, src, df)
                    rows = [[text(v) for v in row] for row in frame.collect()]
                    record["v"] = json.dumps(rows if ordered else sorted(rows))
                except Exception as error:
                    record["err"] = condition(error)
                out.emit(record)
        print("G", cfg, inp, flush=True)


def explain(s: Any, cfg: str, out: Out) -> None:
    queries = [
        "SELECT hour(c) FROM lc.db.s_full",
        "SELECT year(c) FROM lc.db.s_full",
        "SELECT extract(HOUR FROM c) FROM lc.db.s_full",
        "SELECT hour(CAST(c AS TIMESTAMP)) FROM lc.db.s_full",
        "SELECT date_trunc('HOUR', c) FROM lc.db.s_full",
        "SELECT date_format(c, 'HH') FROM lc.db.s_full",
        "SELECT unix_timestamp(c) FROM lc.db.s_full",
    ]
    for query in queries:
        record: dict[str, Any] = {"cfg": cfg, "query": query}
        try:
            record["plan"] = [text(v) for row in s.sql("EXPLAIN " + query).collect() for v in row]
        except Exception as error:
            record["err"] = condition(error)
        out.emit(record)


def main() -> None:
    engine, wh, shared, out_path, part = sys.argv[1:6]
    if engine != "spark":
        wh = tempfile.mkdtemp(prefix="part-", dir=wh)
    s, f = session(engine, wh)
    out = Out(out_path)
    if part == "setup" or engine != "spark":
        setup(s, shared if engine == "spark" else "-")
        print("SETUP", engine, wh, flush=True)
    if part == "setup":
        return
    cfg, kind = part.split(":")[:2]
    configure(s, cfg)
    if kind == "proj":
        inputs = part.split(":")[2] if part.count(":") > 1 else "".join(INPUTS)
        projection(s, f, shared, cfg, inputs, out)
    elif kind == "g":
        g_doors(s, f, shared, cfg, out)
    else:
        explain(s, cfg, out)
    print("DONE", engine, part, flush=True)


main()
