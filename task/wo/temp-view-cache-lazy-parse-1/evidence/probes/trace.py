import contextlib
import functools
import io
import json
import sys
import tempfile
import traceback
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pandas as pd

import repark._native as native
from repark import ReparkSession
from repark.spark import catalog_surface
from repark.spark import functions as fn
from repark.spark.dataframe import cache_handle, core, surface_a

parent, out_path = sys.argv[1:3]
print("NATIVE", native.__file__, flush=True)
wh = tempfile.mkdtemp(prefix="tr-", dir=parent)
LA, UTC = "America/Los_Angeles", "UTC"
SHAPE = ["n", "CAST(ts AS STRING) AS t", "hour(ts) AS h", "unix_timestamp(ts) AS u"]
s = (
    ReparkSession.builder.appName("cache-trace")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", "true")
    .getOrCreate()
)
folder = Path(wh, "json")
folder.mkdir(parents=True)
(folder / "a.json").write_text(
    json.dumps({"n": 1, "ts": "2024-01-01 03:00:00"})
    + "\n"
    + json.dumps({"n": 2, "ts": "2024-06-01 23:30:00"})
    + "\n"
)
events: list[dict[str, Any]] = []
busy = [False]
OUT = Path(out_path)
OUT.write_text("")


def zone() -> str:
    return s.conf.get("spark.sql.session.timeZone")


def callers() -> list[str]:
    seen = []
    for frame in traceback.extract_stack()[:-2]:
        if "/repark/" in frame.filename and "probes" not in frame.filename:
            seen.append(Path(frame.filename).stem + "." + frame.name)
    return seen[-6:]


def traced(name: str, call: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(call)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not busy[0]:
            busy[0] = True
            try:
                events.append({"event": name, "zone": zone(), "stack": callers()})
            finally:
                busy[0] = False
        return call(*args, **kwargs)

    return wrapper


cache_handle.bind_registered_view = traced("cache_materialised", cache_handle.bind_registered_view)
surface_a._own_plan = traced("own_plan_scope", surface_a._own_plan)
catalog_surface.cache_table = traced("catalog_cache_table", catalog_surface.cache_table)
for method in [
    "_native_for_registration",
    "_execute_map_in_arrow_bridge",
    "_materialize_map_bridge_once",
    "_iter_map_in_arrow_output",
]:
    setattr(core.DataFrame, method, traced(method, getattr(core.DataFrame, method)))


def conf(value: str) -> None:
    s.sql("SET spark.sql.session.timeZone=" + value).collect()


def reader() -> Any:
    return s.read.schema("n INT, ts TIMESTAMP").json(wh + "/json")


def us(frame: Any) -> list[str]:
    return sorted(str(row["u"]) + "@" + str(row["t"]) for row in frame.collect())


def plus_one(x: pd.Series) -> pd.Series:
    return x + 1


def arrow_same(batches: Iterator[Any]) -> Iterator[Any]:
    yield from batches


def pandas_same(frames: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    yield from frames


def mark(label: str) -> None:
    events.append({"event": "step", "zone": zone(), "label": label})


def cache_view() -> Any:
    f = reader().selectExpr(*SHAPE).cache()
    mark("cache() done")
    f.createOrReplaceTempView("tv1")
    mark("view registered")
    conf(UTC)
    return us(s.table("tv1"))


def cache_collect() -> Any:
    f = reader().selectExpr(*SHAPE).cache()
    conf(UTC)
    return us(f)


def cache_count() -> Any:
    f = reader().selectExpr(*SHAPE).cache()
    conf(UTC)
    f.count()
    mark("count done")
    return us(f)


def cache_derived() -> Any:
    f = reader().selectExpr(*SHAPE).cache()
    conf(UTC)
    return us(f.select("*"))


def cache_explain() -> Any:
    f = reader().selectExpr(*SHAPE).cache()
    with contextlib.redirect_stdout(io.StringIO()):
        f.explain()
    mark("explain done")
    conf(UTC)
    return us(f)


def table_door(statement: str, name: str) -> Any:
    reader().selectExpr(*SHAPE).createOrReplaceTempView(name)
    mark("view registered")
    if statement == "catalog":
        s.catalog.cacheTable(name)
    else:
        s.sql(statement + " " + name).collect()
    mark(statement + " done")
    conf(UTC)
    return [us(s.table(name)), us(s.sql("SELECT * FROM " + name))]


def udf_door(kind: str) -> Any:
    r = reader()
    if kind == "udf":
        g = r.select("n", "ts", fn.udf(lambda x: x + 1, "int")("n").alias("m"))
    elif kind == "pudf":
        g = r.select("n", "ts", fn.pandas_udf(plus_one, "int")("n").alias("m"))
    elif kind == "mia":
        g = r.mapInArrow(arrow_same, "n INT, ts TIMESTAMP")
    else:
        g = r.mapInPandas(pandas_same, "n INT, ts TIMESTAMP")
    mark("udf column built")
    shaped = g.selectExpr(*SHAPE)
    mark("shape built")
    conf(UTC)
    return us(shaped)


SCENARIOS: dict[str, Callable[[], Any]] = {
    "cache_view": cache_view,
    "cache_collect": cache_collect,
    "cache_count": cache_count,
    "cache_derived": cache_derived,
    "cache_explain": cache_explain,
    "ctable": functools.partial(table_door, "CACHE TABLE", "tv2"),
    "clazy": functools.partial(table_door, "CACHE LAZY TABLE", "tv3"),
    "catcache": functools.partial(table_door, "catalog", "tv4"),
    "udf": functools.partial(udf_door, "udf"),
    "pudf": functools.partial(udf_door, "pudf"),
    "mia": functools.partial(udf_door, "mia"),
    "mip": functools.partial(udf_door, "mip"),
}
for label, scenario in SCENARIOS.items():
    conf(LA)
    events.clear()
    mark("start")
    try:
        answer: Any = scenario()
    except Exception as error:
        answer = "ERR " + type(error).__name__ + ": " + str(error)[:200]
    with OUT.open("a") as handle:
        handle.write(json.dumps({"scenario": label, "answer": answer, "events": events}) + "\n")
    with contextlib.suppress(Exception):
        s.catalog.clearCache()
