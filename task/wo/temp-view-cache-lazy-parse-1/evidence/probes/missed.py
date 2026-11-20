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
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
LA, UTC, TK = "America/Los_Angeles", "UTC", "Asia/Tokyo"
SCHEMA = "n INT, ts TIMESTAMP"
ROWS = [(1, "2024-01-01 03:00:00"), (2, "2024-06-01 23:30:00")]
TEXT = ["json_s", "csv_s", "csv_i"]
wh = tempfile.mkdtemp(prefix="ms-" + engine + "-", dir=parent)

if engine == "spark":
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
    from repark import ReparkSession
    from repark.spark import functions as fn

    print("NATIVE", native.__file__, flush=True)
    builder = ReparkSession.builder.appName("cache-missed")
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


def fresh() -> int:
    counter[0] += 1
    return counter[0]


def condition(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return found.group(1) if found else type(error).__name__ + ": " + text[:160]


def conf(zone: str, session: Any = None) -> None:
    (session or s).sql("SET spark.sql.session.timeZone=" + zone).collect()


def ansi(mode: str) -> None:
    s.sql("SET spark.sql.ansi.enabled=" + mode).collect()


def setup() -> None:
    Path(wh, "json").mkdir(parents=True)
    Path(wh, "json", "a.json").write_text(
        "".join(json.dumps({"n": n, "ts": t}) + "\n" for n, t in ROWS)
    )
    Path(wh, "csv").mkdir(parents=True)
    Path(wh, "csv", "a.csv").write_text("n,ts\n" + "".join(f"{n},{t}\n" for n, t in ROWS))
    Path(wh, "badj").mkdir(parents=True)
    Path(wh, "badj", "a.json").write_text(
        json.dumps({"n": 1, "s": "7", "ts": ROWS[0][1]})
        + "\n"
        + json.dumps({"n": 2, "s": "y", "ts": "nope"})
        + "\n"
    )
    Path(wh, "badc").mkdir(parents=True)
    Path(wh, "badc", "a.csv").write_text("n,s,ts\n1,7," + ROWS[0][1] + "\n2,y,nope\n")
    conf(UTC)
    values = "(1, TIMESTAMP '2024-01-01 03:00:00'), (2, TIMESTAMP '2024-06-01 23:30:00')"
    s.sql("SELECT * FROM VALUES " + values + " AS v(n, ts)").write.mode("overwrite").parquet(
        wh + "/pq"
    )
    s.sql("SELECT * FROM VALUES (1, '7'), (2, 'y') AS v(n, s)").write.mode("overwrite").parquet(
        wh + "/pqs"
    )
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    s.sql("CREATE TABLE lc.db.src (n INT, ts TIMESTAMP) USING iceberg").collect()
    s.sql("INSERT INTO lc.db.src VALUES " + values).collect()


def reader(key: str) -> Any:
    if key == "json_s":
        return s.read.schema(SCHEMA).json(wh + "/json")
    if key == "csv_s":
        return s.read.option("header", "true").schema(SCHEMA).csv(wh + "/csv")
    if key == "csv_i":
        csv = s.read.option("header", "true").option("inferSchema", "true").csv(wh + "/csv")
        return csv.selectExpr("CAST(n AS INT) AS n", "ts")
    if key == "pq":
        return s.read.parquet(wh + "/pq")
    return s.table("lc.db.src")


def bad(key: str) -> Any:
    if key == "json":
        return s.read.schema("n INT, s STRING, ts TIMESTAMP").json(wh + "/badj")
    if key == "csv":
        return (
            s.read.option("header", "true")
            .schema("n INT, s STRING, ts TIMESTAMP")
            .csv(wh + "/badc")
        )
    return s.read.parquet(wh + "/pqs")


def shaped(key: str, k: int) -> Any:
    return reader(key).selectExpr(
        "n", "CAST(ts AS STRING) AS t", "hour(ts) AS h", "unix_timestamp(ts) AS u", f"{k} AS k"
    )


def rows(frame: Any) -> list[list[Any]]:
    got = [[None if v is None else str(v) for v in r] for r in frame.collect()]
    return sorted(got, key=json.dumps)


def quiet(call: Callable[[], Any]) -> None:
    with contextlib.redirect_stdout(io.StringIO()):
        call()


def plus_one(x: pd.Series) -> pd.Series:
    return x + 1


def arrow_same(batches: Iterator[Any]) -> Iterator[Any]:
    yield from batches


def bridged(key: str, kind: str) -> Any:
    r = reader(key)
    if kind == "udf":
        return r.select("n", "ts", fn.udf(lambda x: x + 1, "int")("n").alias("m"))
    if kind == "pudf":
        return r.select("n", "ts", fn.pandas_udf(plus_one, "int")("n").alias("m"))
    return r.mapInArrow(arrow_same, SCHEMA)


def top(frame: Any, k: int) -> Any:
    return frame.selectExpr(
        "n", "CAST(ts AS STRING) AS t", "hour(ts) AS h", "unix_timestamp(ts) AS u", f"{k} AS k"
    )


def guarded(tag: str, make: Callable[[], Any]) -> None:
    try:
        record = {"tag": tag, "v": make()}
    except Exception as error:
        record = {"tag": tag, "err": condition(error)}
    with OUT.open("a") as handle:
        handle.write(json.dumps(record) + "\n")
    with contextlib.suppress(Exception):
        s.catalog.clearCache()


def step(call: Callable[[], Any]) -> Any:
    try:
        return call()
    except Exception as error:
        return "ERR " + condition(error)


def m_refresh(key: str) -> Any:
    v = "rf" + str(fresh())
    conf(LA)
    shaped(key, counter[0]).createOrReplaceTempView(v)
    s.sql("CACHE TABLE " + v).collect()
    conf(UTC)
    s.sql("REFRESH TABLE " + v).collect()
    conf(TK)
    return {"table": rows(s.table(v)), "sql": rows(s.sql("SELECT * FROM " + v))}


def m_clear_view(key: str) -> Any:
    v = "cl" + str(fresh())
    conf(LA)
    f = shaped(key, counter[0]).cache()
    f.createOrReplaceTempView(v)
    conf(UTC)
    s.catalog.clearCache()
    conf(TK)
    return {"table": rows(s.table(v)), "own": rows(f)}


def m_clear_mat(key: str) -> Any:
    conf(LA)
    f = shaped(key, fresh()).cache()
    f.count()
    conf(UTC)
    s.catalog.clearCache()
    conf(TK)
    return {"own": rows(f), "child": rows(f.filter("n > 0"))}


def m_ctas(key: str, lazy: bool) -> Any:
    k = fresh()
    v, x = "cs" + str(k), "cx" + str(k)
    conf(LA)
    reader(key).createOrReplaceTempView(v)
    word = "CACHE LAZY TABLE " if lazy else "CACHE TABLE "
    s.sql(word + x + " AS SELECT n, CAST(ts AS STRING) AS t, ts FROM " + v).collect()
    conf(UTC)
    return rows(s.sql("SELECT n, t, CAST(ts AS STRING) AS t2, unix_timestamp(ts) AS u FROM " + x))


def m_unpersist_blocking(key: str) -> Any:
    conf(LA)
    f = shaped(key, fresh()).cache()
    conf(UTC)
    first = rows(f)
    f.unpersist(blocking=True)
    conf(TK)
    return {"first": first, "second": rows(f), "child": rows(f.filter("n > 0"))}


def m_unpersist_reread(key: str) -> Any:
    conf(LA)
    f = shaped(key, fresh()).cache()
    conf(UTC)
    f.count()
    f.unpersist()
    conf(TK)
    return {"own": rows(f), "child": rows(f.filter("n > 0"))}


def m_cache_siblings(key: str) -> Any:
    conf(LA)
    f = shaped(key, fresh()).cache()
    a = f.filter("n > 0")
    b = f.filter("n >= 0")
    conf(UTC)
    first = rows(a)
    conf(TK)
    return {"a": first, "b": rows(b), "own": rows(f)}


def m_selfjoin(key: str) -> Any:
    conf(LA)
    f = shaped(key, fresh()).cache()
    conf(UTC)
    a, b = f.alias("a"), f.alias("b")
    return rows(a.join(b, a["n"] == b["n"]).selectExpr("a.t", "a.u", "b.t AS bt", "b.h"))


def m_bridge(key: str, kind: str, first: str) -> Any:
    conf(LA)
    k = fresh()
    g = bridged(key, kind)
    one, two = top(g, k), top(g, k).filter("n >= 0")
    conf(UTC)
    if first == "sibling":
        early = rows(one)
        conf(TK)
        return {"a": early, "b": rows(two)}
    if first == "count":
        early = one.count()
    elif first == "show":
        early = quiet(one.show)
    elif first == "take":
        early = len(one.take(1))
    else:
        one.write.mode("overwrite").parquet(wh + "/bw" + str(k))
        early = "w"
    conf(TK)
    return {"first": early, "a": rows(one)}


def m_ice(how: str) -> Any:
    name = "lc.db.st" + str(fresh())
    s.sql("CREATE TABLE " + name + " (n INT, ts TIMESTAMP) USING iceberg").collect()
    s.sql("INSERT INTO " + name + " VALUES (1, TIMESTAMP '2024-01-01 03:00:00')").collect()
    if how == "ctable":
        s.sql("CACHE TABLE " + name).collect()
        f = s.table(name)
    elif how in ("held", "held_append"):
        f = s.table(name)
        f.count()
    else:
        f = s.table(name).cache()
        if how != "lazy":
            f.count()
    if how in ("append", "held_append"):
        s.table(name).limit(1).writeTo(name).append()
    else:
        s.sql("INSERT INTO " + name + " VALUES (2, TIMESTAMP '2024-06-01 23:30:00')").collect()
    return {
        "cached": f.count(),
        "table": s.table(name).count(),
        "sql": rows(s.sql("SELECT count(*) AS c FROM " + name)),
    }


def m_ansi_flip(key: str, made: str, read: str, door: str) -> Any:
    ansi(made)
    if door == "cast":
        f = bad(key).selectExpr("n", "CAST(s AS INT) AS c")
    else:
        f = bad(key).selectExpr("n", "CAST(ts AS STRING) AS t")
    if door == "cache" or door == "count":
        f = f.cache()
    ansi(read)
    early = step(f.count) if door == "count" else None
    return {"first": early, "rows": step(functools.partial(rows, f))}


def cells() -> list[tuple[str, Callable[[], Any]]]:
    out: list[tuple[str, Callable[[], Any]]] = []
    for key in [*TEXT, "pq"]:
        out.append((f"M/refresh/{key}", functools.partial(m_refresh, key)))
        out.append((f"M/clear_view/{key}", functools.partial(m_clear_view, key)))
        out.append((f"M/clear_mat/{key}", functools.partial(m_clear_mat, key)))
        out.append((f"M/ctas/{key}", functools.partial(m_ctas, key, False)))
        out.append((f"M/ctas_lazy/{key}", functools.partial(m_ctas, key, True)))
        out.append((f"M/unpersist_blocking/{key}", functools.partial(m_unpersist_blocking, key)))
        out.append((f"M/unpersist_reread/{key}", functools.partial(m_unpersist_reread, key)))
        out.append((f"M/cache_siblings/{key}", functools.partial(m_cache_siblings, key)))
        out.append((f"M/selfjoin/{key}", functools.partial(m_selfjoin, key)))
        for kind in ["udf", "pudf", "mia"]:
            for first in ["sibling", "count", "show", "take", "write"]:
                out.append(
                    (
                        f"M/bridge_{first}/{key}/{kind}",
                        functools.partial(m_bridge, key, kind, first),
                    )
                )
    for how in ["sql", "append", "lazy", "ctable", "held", "held_append"]:
        out.append((f"M/ice_stale/{how}", functools.partial(m_ice, how)))
    for key in ["json", "csv", "pqs"]:
        for made, read in [
            ("false", "true"),
            ("true", "false"),
            ("true", "true"),
            ("false", "false"),
        ]:
            for door in ["cast"] if key == "pqs" else ["plain", "cache", "count", "cast"]:
                tag = f"M/ansi_{door}/{key}/{made}_{read}"
                out.append((tag, functools.partial(m_ansi_flip, key, made, read, door)))
    return out


setup()
for mode in ["true", "false"]:
    for tag, body in cells():
        if tag.startswith("M/ansi_") and mode == "false":
            continue
        ansi(mode)
        guarded(tag.replace("M/", "M/" + mode + "/", 1), body)
ansi("true")
with OUT.open("a") as handle:
    handle.write(json.dumps({"tag": "DONE", "cells": counter[0]}) + "\n")
shutil.rmtree(wh, ignore_errors=True)
