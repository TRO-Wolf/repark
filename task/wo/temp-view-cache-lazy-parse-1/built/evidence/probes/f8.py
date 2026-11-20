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


def guarded(tag: str, make: Callable[[], Any]) -> None:
    try:
        emit({"tag": tag, "v": make()})
    except Exception as error:
        emit({"tag": tag, "err": condition(error)})


COLS = "n INT, ts TIMESTAMP, t STRING, h INT, u BIGINT, k INT"


def verb_cell(mode: str, direction: Any, key: str, verb: str, cached: bool, fill: str) -> None:
    label, made, read, third = direction
    counter[0] += 1
    k = counter[0]
    table = "lc.db.w" + str(k)
    tag = "/".join(["F8", mode, label, key, verb, "cache" if cached else "plain", fill])
    ansi(mode)
    conf(made)
    try:
        f = base(key, k, ts=True)
        if cached:
            f = f.cache()
        if fill == "fill_read":
            conf(read)
            f.count()
            conf(made)
        elif fill == "fill_made":
            f.count()
            conf(read)
        else:
            conf(read)
        if verb in ("append", "overwritePartitions"):
            s.sql("CREATE TABLE " + table + " (" + COLS + ") USING iceberg").collect()
        writer = f.writeTo(table)
        if verb == "create":
            writer.create()
        elif verb == "createOrReplace":
            writer.createOrReplace()
        elif verb == "append":
            writer.append()
        else:
            writer.overwritePartitions()
        conf(UTC)
        guarded(tag, lambda: rows(s.table(table).select("n", "t", "h", "u")))
    except Exception as error:
        emit({"tag": tag, "err": condition(error)})
    with contextlib.suppress(Exception):
        s.catalog.clearCache()
    with contextlib.suppress(Exception):
        s.sql("DROP TABLE IF EXISTS " + table).collect()


setup()
for mode in MODES:
    for direction in DIRECTIONS:
        for key in ["json_s", "csv_s", "csv_i", "pq", "ice"]:
            for verb in ["create", "createOrReplace", "append", "overwritePartitions"]:
                for cached in (True, False):
                    for fill in ("nofill", "fill_read", "fill_made"):
                        if not cached and fill != "nofill":
                            continue
                        verb_cell(mode, direction, key, verb, cached, fill)
shutil.rmtree(wh, ignore_errors=True)
