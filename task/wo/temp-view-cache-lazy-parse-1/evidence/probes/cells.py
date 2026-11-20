import contextlib
import functools
import io
import json
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pandas as pd

engine, parent, out_path = sys.argv[1:4]
GROUPS = sys.argv[4].split(",") if len(sys.argv) > 4 else ["R", "C", "U", "L"]
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
SHOWN = re.compile(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d|\b1\d{9}\b")
LA, UTC, TK = "America/Los_Angeles", "UTC", "Asia/Tokyo"
SCHEMA = "n INT, ts TIMESTAMP"
ROWS = [(1, "2024-01-01 03:00:00"), (2, "2024-06-01 23:30:00")]
DIRECTIONS = [("la_utc", LA, UTC, TK), ("utc_tk", UTC, TK, LA)]
MODES = sys.argv[5].split(",") if len(sys.argv) > 5 else ["true", "false"]
READERS = ["json_s", "csv_s", "csv_i", "pq", "ice"]
wh = tempfile.mkdtemp(prefix="cl-" + engine + "-", dir=parent)

if engine == "spark":
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as fn

    builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", os.environ["ICEBERG_JAR"])
        .config("spark.sql.extensions", os.environ["ICEBERG_EXT"])
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", wh + "/sw")
    )
else:
    import repark._native as native
    from repark import ReparkSession, StorageLevel
    from repark.spark import functions as fn

    print("NATIVE", native.__file__, flush=True)
    builder = ReparkSession.builder.appName("cache-lazy")
s = (
    builder.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh + "/lc")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "true")
    .getOrCreate()
)
if engine == "spark":
    s.sparkContext.setLogLevel("ERROR")
OUT = Path(out_path)
OUT.write_text("")
counter = [0]


def condition(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return found.group(1) if found else type(error).__name__ + ": " + text[:160]


def conf(zone: str, session: Any = None) -> None:
    (session or s).sql("SET spark.sql.session.timeZone=" + zone).collect()


def ansi(mode: str) -> None:
    s.sql("SET spark.sql.ansi.enabled=" + mode).collect()


def emit(record: dict[str, Any]) -> None:
    with OUT.open("a") as handle:
        handle.write(json.dumps(record) + "\n")


def setup() -> None:
    folder = Path(wh, "json")
    folder.mkdir(parents=True)
    (folder / "a.json").write_text("".join(json.dumps({"n": n, "ts": t}) + "\n" for n, t in ROWS))
    folder = Path(wh, "csv")
    folder.mkdir(parents=True)
    (folder / "a.csv").write_text("n,ts\n" + "".join(f"{n},{t}\n" for n, t in ROWS))
    conf(UTC)
    values = "(1, TIMESTAMP '2024-01-01 03:00:00'), (2, TIMESTAMP '2024-06-01 23:30:00')"
    s.sql("SELECT * FROM VALUES " + values + " AS v(n, ts)").write.mode("overwrite").parquet(
        wh + "/pq"
    )
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    s.sql("CREATE TABLE lc.db.src (n INT, ts TIMESTAMP) USING iceberg").collect()
    s.sql("INSERT INTO lc.db.src VALUES " + values).collect()


def reader(key: str, session: Any = None) -> Any:
    target = session or s
    if key == "json_s":
        return target.read.schema(SCHEMA).json(wh + "/json")
    if key == "csv_s":
        return target.read.option("header", "true").schema(SCHEMA).csv(wh + "/csv")
    if key == "csv_i":
        return target.read.option("header", "true").option("inferSchema", "true").csv(wh + "/csv")
    if key == "pq":
        return target.read.parquet(wh + "/pq")
    return target.table("lc.db.src")


def shape(k: int, ts: bool = False) -> list[str]:
    head = ["n", "ts"] if ts else ["n"]
    body = ["CAST(ts AS STRING) AS t", "hour(ts) AS h", "unix_timestamp(ts) AS u"]
    return [*head, *body, f"{k} AS k"]


def base(key: str, k: int, session: Any = None, ts: bool = False) -> Any:
    return reader(key, session).selectExpr(*shape(k, ts))


def rows(frame: Any) -> list[list[Any]]:
    got = [[None if v is None else str(v) for v in r] for r in frame.collect()]
    return sorted(got, key=json.dumps)


def quiet(call: Callable[[], Any]) -> str:
    with contextlib.redirect_stdout(io.StringIO()) as text:
        call()
    return text.getvalue()


def pq_side() -> Any:
    return s.read.parquet(wh + "/pq").selectExpr("n AS m", "hour(ts) AS ph")


def plus_one(x: pd.Series) -> pd.Series:
    return x + 1


def arrow_same(batches: Iterator[Any]) -> Iterator[Any]:
    yield from batches


def pandas_same(frames: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    yield from frames


def udf_frame(kind: str, key: str, k: int, ts: bool = False) -> Any:
    r = reader(key)
    tail = shape(k, ts)
    if kind == "udf":
        made = fn.udf(lambda x: x + 1, "int")
        return r.select("n", "ts", made("n").alias("m")).selectExpr(*tail, "m")
    if kind == "pudf":
        made = fn.pandas_udf(plus_one, "int")
        return r.select("n", "ts", made("n").alias("m")).selectExpr(*tail, "m")
    typed = r.selectExpr("CAST(n AS INT) AS n", "ts")
    if kind == "mia":
        return typed.mapInArrow(arrow_same, SCHEMA).selectExpr(*tail)
    return typed.mapInPandas(pandas_same, SCHEMA).selectExpr(*tail)


TOP = "n INT, t STRING, h INT, u BIGINT, k INT"


def top_frame(kind: str, key: str, k: int) -> Any:
    typed = base(key, k).selectExpr(
        "CAST(n AS INT) AS n", "t", "CAST(h AS INT) AS h", "CAST(u AS BIGINT) AS u", "k"
    )
    if kind == "udf":
        return typed.select("n", "t", "h", "u", "k", fn.udf(lambda x: x + 1, "int")("n").alias("m"))
    if kind == "pudf":
        return typed.select("n", "t", "h", "u", "k", fn.pandas_udf(plus_one, "int")("n").alias("m"))
    if kind == "mia":
        return typed.mapInArrow(arrow_same, TOP)
    return typed.mapInPandas(pandas_same, TOP)


def act(action: str, f: Any, third: str) -> Any:
    if action == "count":
        n = f.count()
        conf(third)
        return {"count": n, "after": rows(f)}
    if action == "collect":
        return rows(f)
    if action == "show":
        return sorted(SHOWN.findall(quiet(lambda: f.show(truncate=False))))
    if action == "derived":
        return rows(f.filter("n > 0").select("n", "t", "h", "u"))
    if action == "join":
        side = pq_side()
        return rows(f.join(side, f.n == side.m).select("n", "t", "h", "u", "ph"))
    path = wh + "/out/w" + str(counter[0])
    f.write.mode("overwrite").parquet(path)
    return rows(s.read.parquet(path).select("n", "t", "h", "u"))


def door(name: str, key: str, k: int, v: str) -> Callable[[], Any]:
    if name == "none":
        f = base(key, k)
        return lambda: f
    if name == "mat":
        f = base(key, k).cache()
        f.count()
        return lambda: f
    if name == "cache":
        f = base(key, k).cache()
        return lambda: f
    if name == "persist_mo":
        f = base(key, k).persist(StorageLevel.MEMORY_ONLY)
        return lambda: f
    if name == "persist_disk":
        f = base(key, k).persist(StorageLevel.DISK_ONLY)
        return lambda: f
    if name in ("cache_view", "cache_view_sql"):
        f = base(key, k).cache()
        f.createOrReplaceTempView(v)
        if name == "cache_view":
            return lambda: s.table(v)
        return lambda: s.sql("SELECT * FROM " + v)
    if name == "recache":
        f = base(key, k).cache()
        f.count()
        f.unpersist()
        f.cache()
        return lambda: f
    if name == "recache_after":
        f = base(key, k).cache()
        f.count()
        f.unpersist()
        return lambda: f.cache()
    if name == "after_set":
        f = base(key, k)
        return lambda: f.cache()
    base(key, k).createOrReplaceTempView(v)
    if name.startswith("ctable"):
        s.sql("CACHE TABLE " + v).collect()
    elif name.startswith("clazy"):
        s.sql("CACHE LAZY TABLE " + v).collect()
    else:
        s.catalog.cacheTable(v)
    if name.endswith("_sql"):
        return lambda: s.sql("SELECT * FROM " + v)
    return lambda: s.table(v)


def release(v: str) -> None:
    with contextlib.suppress(Exception):
        s.catalog.clearCache()
    with contextlib.suppress(Exception):
        s.catalog.dropTempView(v)


def guarded(tag: str, make: Callable[[], Any]) -> None:
    try:
        emit({"tag": tag, "v": make()})
    except Exception as error:
        emit({"tag": tag, "err": condition(error)})


C_DOORS = ["mat", "cache", "persist_mo", "persist_disk", "cache_view", "cache_view_sql"]
C_DOORS += ["ctable", "ctable_sql", "clazy", "clazy_sql", "catcache", "recache"]
C_DOORS += ["recache_after", "after_set"]
ACTIONS = ["count", "collect", "show", "derived", "join", "write"]


Direction = tuple[str, str, str, str]


def c_cell(mode: str, direction: Direction, key: str, name: str, action: str) -> None:
    label, made, read, third = direction
    counter[0] += 1
    k, v = counter[0], "cv" + str(counter[0])
    tag = "/".join(["N" if name == "none" else "C", mode, label, key, name, action])
    conf(made)
    try:
        get = door(name, key, k, v)
    except Exception as error:
        emit({"tag": tag, "build_err": condition(error)})
        release(v)
        return
    conf(read)
    guarded(tag, lambda: act(action, get(), third))
    release(v)


def u_cell(
    mode: str, direction: Direction, key: str, kind: str, when: str, top: bool = False
) -> None:
    label, made, read, _ = direction
    counter[0] += 1
    k = counter[0]
    tag = "/".join(["T" if top else "U", mode, label, key, kind, when])
    conf(made)
    try:
        g = top_frame(kind, key, k) if top else udf_frame(kind, key, k)
    except Exception as error:
        emit({"tag": tag, "build_err": condition(error)})
        return
    early: dict[str, Any] = {}
    if when == "second":
        try:
            early["first"] = rows(g)
        except Exception as error:
            early["first"] = "ERR " + condition(error)
    if when == "countfirst":
        try:
            early["first"] = g.count()
        except Exception as error:
            early["first"] = "ERR " + condition(error)
    conf(read)
    guarded(tag, lambda: {**early, "rows": rows(g)})


def l_cell(mode: str, direction: Direction, key: str, src: str, how: str) -> None:
    label, made, read, _ = direction
    counter[0] += 1
    k, v = counter[0], "lv" + str(counter[0])
    table = "lc.db.t" + str(k)
    tag = "/".join(["L", mode, label, key, src, how])
    conf(made)
    try:
        f = base(key, k, ts=True).cache() if src == "cache" else udf_frame("udf", key, k, ts=True)
        if how == "insert":
            extra = ", m INT" if src == "udf" else ""
            s.sql(
                "CREATE TABLE "
                + table
                + " (n INT, ts TIMESTAMP, t STRING, h INT, u BIGINT, k INT"
                + extra
                + ") USING iceberg TBLPROPERTIES ('format-version'='2')"
            ).collect()
            f.createOrReplaceTempView(v)
    except Exception as error:
        emit({"tag": tag, "build_err": condition(error)})
        release(v)
        return
    conf(read)

    def write_read() -> Any:
        if how == "create":
            f.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
        else:
            s.sql("INSERT INTO " + table + " SELECT * FROM " + v).collect()
        back = s.table(table).selectExpr("n", "t", "h", "u", "unix_timestamp(ts) AS su")
        return rows(back)

    guarded(tag, write_read)
    release(v)


def fresh() -> int:
    counter[0] += 1
    return counter[0]


def r_lazy_two(key: str) -> Any:
    conf(LA)
    f = base(key, fresh()).cache()
    conf(UTC)
    first = rows(f)
    conf(TK)
    return [first, rows(f), rows(f.select("*")), f.count()]


def r_own_then_cache(key: str) -> Any:
    conf(LA)
    f = base(key, fresh())
    conf(UTC)
    own = rows(f)
    f.cache()
    conf(TK)
    again = rows(f)
    counted = f.count()
    conf(LA)
    return [own, again, counted, rows(f.select("*")), rows(f)]


def r_child_before(key: str) -> Any:
    conf(LA)
    f = base(key, fresh()).cache()
    g = f.select("*")
    conf(UTC)
    child = rows(g)
    conf(TK)
    return [child, rows(f), rows(g)]


def r_explain_first(key: str) -> Any:
    conf(LA)
    f = base(key, fresh()).cache()
    quiet(f.explain)
    conf(UTC)
    return rows(f)


def r_newsess(key: str, mat: bool) -> Any:
    k = fresh()
    conf(LA)
    other = s.newSession()
    conf(LA, other)
    f = base(key, k).cache()
    if mat:
        f.count()
    conf(TK, other)
    g = base(key, k, other)
    conf(UTC)
    return [rows(g), rows(f)]


def r_udf_cache(key: str, kind: str) -> Any:
    conf(LA)
    g = udf_frame(kind, key, fresh()).cache()
    conf(UTC)
    return rows(g)


def r_clazy_then(key: str) -> Any:
    k = fresh()
    v = "rv" + str(k)
    conf(LA)
    f = base(key, k)
    f.createOrReplaceTempView(v)
    s.sql("CACHE LAZY TABLE " + v).collect()
    conf(UTC)
    first = rows(s.sql("SELECT * FROM " + v))
    conf(TK)
    return [first, rows(s.table(v)), rows(f)]


R_STEPS: dict[str, Callable[[str], Any]] = {
    "lazy_two": r_lazy_two,
    "own_then_cache": r_own_then_cache,
    "child_before": r_child_before,
    "explain_first": r_explain_first,
    "newsess_lazy": functools.partial(r_newsess, mat=False),
    "newsess_mat": functools.partial(r_newsess, mat=True),
    "udf_cache": functools.partial(r_udf_cache, kind="udf"),
    "mia_cache": functools.partial(r_udf_cache, kind="mia"),
    "clazy_then": r_clazy_then,
}


def q_bare(key: str, mat: bool, other_session: bool) -> Any:
    k = fresh()
    conf(LA)
    r = reader(key).selectExpr("n", "ts", f"{k} AS k").cache()
    if mat:
        r.count()
    target = s
    if other_session:
        target = s.newSession()
        conf(TK, target)
    else:
        conf(UTC)
    g = reader(key, target).selectExpr("n", "ts", f"{k} AS k").selectExpr(*shape(k)[:-1], "k")
    conf(UTC)
    first = rows(g)
    conf(TK)
    return [first, rows(r.selectExpr(*shape(k)[:-1], "k"))]


def q_twin(key: str) -> Any:
    k = fresh()
    conf(LA)
    one = base(key, k).cache()
    two = base(key, k)
    conf(UTC)
    first = rows(two)
    conf(TK)
    return [first, rows(one)]


Q_STEPS: dict[str, Callable[[str], Any]] = {
    "bare_lazy": functools.partial(q_bare, mat=False, other_session=False),
    "bare_mat": functools.partial(q_bare, mat=True, other_session=False),
    "newsess_bare_lazy": functools.partial(q_bare, mat=False, other_session=True),
    "newsess_bare_mat": functools.partial(q_bare, mat=True, other_session=True),
    "twin_plan": q_twin,
}


def q_cells() -> None:
    for key in ["json_s", "csv_i", "pq"]:
        for name, step in Q_STEPS.items():
            guarded(f"Q/{key}/{name}", functools.partial(step, key))
            release("none")


def r_cells() -> None:
    for key in ["json_s", "csv_i", "pq"]:
        for name, step in R_STEPS.items():
            guarded(f"R/{key}/{name}", functools.partial(step, key))
            release("rv" + str(counter[0]))


setup()
for mode in MODES:
    ansi(mode)
    if "R" in GROUPS and mode == "true":
        r_cells()
    if "Q" in GROUPS and mode == "true":
        q_cells()
    for direction in DIRECTIONS:
        for key in READERS:
            if "C" in GROUPS:
                for name in C_DOORS:
                    for action in ACTIONS:
                        c_cell(mode, direction, key, name, action)
            if "U" in GROUPS:
                for kind in ["udf", "pudf", "mia", "mip"]:
                    for when in ["first", "second", "countfirst"]:
                        u_cell(mode, direction, key, kind, when)
            if "N" in GROUPS:
                for action in ACTIONS:
                    c_cell(mode, direction, key, "none", action)
            if "T" in GROUPS:
                for kind in ["udf", "pudf", "mia", "mip"]:
                    for when in ["first", "second", "countfirst"]:
                        u_cell(mode, direction, key, kind, when, top=True)
            if "L" in GROUPS:
                for src in ["cache", "udf"]:
                    for how in ["create", "insert"]:
                        l_cell(mode, direction, key, src, how)
emit({"tag": "DONE", "cells": counter[0]})
shutil.rmtree(wh, ignore_errors=True)
