import shutil
import sys
from collections.abc import Callable
from itertools import count
from typing import Any

from lib import Log, answer, attempt, build, norm

ENGINE, WH, JAR, OUT = sys.argv[1:5]
ANSI = "spark.sql.ansi.enabled"
TZ = "spark.sql.session.timeZone"
LA = "America/Los_Angeles"

s, prov = build(ENGINE, WH, JAR, "true")
if ENGINE == "spark":
    from pyspark.sql import functions as fn
else:
    from repark.spark import functions as fn
log = Log(OUT, prov)
serial = count(1)

FIX = {
    "fx": "SELECT CAST(2147483647 AS INT) AS a, CAST(0 AS INT) AS b",
    "f2": "SELECT * FROM VALUES (CAST(0 AS INT), 'abc'), (CAST(2 AS INT), '7') t(b, s)",
    "tzt": "SELECT TIMESTAMP '2024-01-01 02:30:00' AS ts, '2024-01-01 02:30:00' AS s",
}
BODY = {
    "add": ("ansi", "SELECT a + 1 AS m FROM fx"),
    "div": ("ansi", "SELECT 10 / b AS m FROM fx"),
    "hour": ("tz", "SELECT hour(ts) AS m FROM tzt"),
    "str": ("tz", "SELECT CAST(ts AS STRING) AS m FROM tzt"),
    "date": ("tz", "SELECT CAST(ts AS DATE) AS m FROM tzt"),
    "lit": ("tz", "SELECT unix_timestamp(TIMESTAMP '2024-01-01 02:30:00') AS m FROM tzt"),
    "fdiv": ("ansi", "SELECT b, 10 / b AS m FROM f2"),
    "fcast": ("ansi", "SELECT s, CAST(s AS INT) AS m FROM f2"),
    "lts": ("tz", "SELECT hour(localtimestamp()) AS m"),
    "cdate": ("tz", "SELECT CAST(current_date() AS STRING) AS m"),
    "now": ("tz", "SELECT hour(now()) AS m"),
}
KEY = {"ansi": ANSI, "tz": TZ}
DIRS = {
    "on_off": ("true", "false"),
    "off_on": ("false", "true"),
    "on_on": ("true", "true"),
    "utc_la": ("UTC", LA),
    "la_utc": (LA, "UTC"),
}
TYPES = {"hour": "INT", "str": "STRING", "date": "DATE", "lit": "BIGINT", "add": "INT"}


def q(text: str) -> list:
    return s.sql(text).collect()


def setk(key: str, value: str) -> None:
    q("SET " + key + "=" + value)


def baseline() -> None:
    for key, value in ((ANSI, "true"), (TZ, "UTC")):
        attempt(lambda key=key, value=value: setk(key, value) or {})


def name(prefix: str) -> str:
    return prefix + str(next(serial))


def rows(frame: Any) -> str:
    return ";".join(sorted("|".join(norm(x) for x in r) for r in frame.collect()))


def put(cell: str, kind: str, run: Callable[[], dict], **meta: str) -> None:
    result = attempt(run)
    baseline()
    log.put({"cell": cell, "kind": kind, **meta, "result": result})


def make(door: str, body: str, view: str) -> None:
    text = BODY[body][1]
    if door == "sql":
        q("CREATE OR REPLACE TEMP VIEW " + view + " AS " + text)
    elif door == "df":
        s.sql(text).createOrReplaceTempView(view)
    else:
        raise ValueError(door)


def fixtures() -> None:
    for fixture, text in FIX.items():
        path = WH + "/fx-" + fixture
        s.sql(text).write.mode("overwrite").parquet(path)
        s.read.parquet(path).createOrReplaceTempView(fixture)
    q("CREATE NAMESPACE IF NOT EXISTS lc.ns")


def directs() -> None:
    for body in BODY:
        family = BODY[body][0]
        values = ("true", "false") if family == "ansi" else ("UTC", LA)
        for value in values:

            def run(body: str = body, family: str = family, value: str = value) -> dict:
                setk(KEY[family], value)
                return answer(s.sql(BODY[body][1]))

            put("direct/" + body + "/" + value, "direct", run, body=body)


def filters() -> None:
    shapes = [
        ("filter_below", "fdiv", "b <> 0", ("on_off", "off_on", "on_on")),
        ("filter_below", "fcast", "s <> 'abc'", ("on_off", "off_on", "on_on")),
        ("filter_on_m", "fdiv", "m > 1", ("on_off", "off_on")),
        ("limit", "fdiv", "b <> 0 LIMIT 1", ("on_off", "off_on")),
        ("filter_sensitive", "fdiv", "b + 0 <> 0", ("on_off", "off_on")),
    ]
    for door in ("sql", "df"):
        for shape, body, where, dirs in shapes:
            for d in dirs:

                def run(door: str = door, body: str = body, where: str = where, d: str = d) -> dict:
                    cv, rv = DIRS[d]
                    view = name("fv")
                    setk(ANSI, cv)
                    make(door, body, view)
                    setk(ANSI, rv)
                    return answer(s.sql("SELECT m FROM " + view + " WHERE " + where))

                put(shape + "/" + door + "/" + body + "/" + d, "filter", run, body=body, dir=d)


def caches() -> None:
    def sql_then_set() -> dict:
        view = name("cv")
        make("sql", "hour", view)
        q("CACHE TABLE " + view)
        before = rows(s.sql("SELECT m FROM " + view))
        setk(TZ, LA)
        after = rows(s.sql("SELECT m FROM " + view))
        cached = norm(s.catalog.isCached(view))
        q("UNCACHE TABLE " + view)
        return {"rows": ["before=" + before, "after=" + after, "cached=" + cached]}

    def df_then_set() -> dict:
        view = name("cv")
        make("df", "hour", view)
        q("CACHE TABLE " + view)
        setk(TZ, LA)
        after = rows(s.sql("SELECT m FROM " + view))
        cached = norm(s.catalog.isCached(view))
        q("UNCACHE TABLE " + view)
        return {"rows": ["after=" + after, "cached=" + cached]}

    def lazy_then_set() -> dict:
        view = name("cv")
        make("sql", "hour", view)
        q("CACHE LAZY TABLE " + view)
        setk(TZ, LA)
        after = rows(s.sql("SELECT m FROM " + view))
        q("UNCACHE TABLE " + view)
        return {"rows": ["after=" + after]}

    def catalog_then_set() -> dict:
        view = name("cv")
        make("sql", "hour", view)
        s.catalog.cacheTable(view)
        setk(TZ, LA)
        after = rows(s.sql("SELECT m FROM " + view))
        s.catalog.uncacheTable(view)
        return {"rows": ["after=" + after]}

    def set_between_zone() -> dict:
        view = name("cv")
        make("sql", "hour", view)
        setk(TZ, LA)
        q("CACHE TABLE " + view)
        after = rows(s.sql("SELECT m FROM " + view))
        q("UNCACHE TABLE " + view)
        return {"rows": ["after=" + after]}

    def set_between_off_on() -> dict:
        view = name("cv")
        setk(ANSI, "false")
        make("sql", "add", view)
        setk(ANSI, "true")
        q("CACHE TABLE " + view)
        after = rows(s.sql("SELECT m FROM " + view))
        q("UNCACHE TABLE " + view)
        return {"rows": ["after=" + after]}

    def held_then_set() -> dict:
        frame = s.sql(BODY["hour"][1]).cache()
        frame.count()
        setk(TZ, LA)
        after = rows(frame)
        frame.unpersist()
        return {"rows": ["after=" + after]}

    for cell_fn in (
        sql_then_set,
        df_then_set,
        lazy_then_set,
        catalog_then_set,
        set_between_zone,
        set_between_off_on,
        held_then_set,
    ):
        put("cache/" + cell_fn.__name__, "cache", cell_fn)


def derived(op: str, h: Any) -> Any:
    if op == "alias":
        return h.alias(name("al"))
    if op == "union":
        return h.union(h)
    if op == "distinct":
        return h.distinct()
    if op == "filter":
        return h.filter(fn.col("m").isNotNull() | fn.col("m").isNull())
    if op == "selectcol":
        return h.select(fn.col("m"))
    if op == "join":
        other = s.sql("SELECT 1 AS one")
        left, right = name("l"), name("r")
        return (
            h.alias(left)
            .join(other.alias(right), fn.lit(1) == fn.col(right + ".one"))
            .select(left + ".m")
        )
    if op == "crossjoin":
        return h.crossJoin(s.sql("SELECT 1 AS one")).select("m")
    if op == "gagg":
        return h.select(fn.max("m").alias("m"))
    if op == "gagg_lit":
        return h.select(fn.max("m").alias("m"), fn.lit(1).alias("one"))
    if op == "groupagg":
        return h.groupBy().agg(fn.max("m").alias("m"))
    if op == "unpivot":
        return h.withColumn("id", fn.lit(1)).unpivot(["id"], ["m"], "k", "val").select("val")
    if op == "selectexpr":
        return h.selectExpr("m")
    if op == "explode":
        return h.select(fn.explode(fn.array(fn.col("m"))).alias("m"))
    if op == "intersect":
        return h.intersect(h)
    if op == "subtract":
        return h.subtract(h.filter(fn.col("m").isNull()))
    raise ValueError(op)


def derivations() -> None:
    ops = [
        "alias",
        "union",
        "distinct",
        "filter",
        "selectcol",
        "join",
        "crossjoin",
        "gagg",
        "gagg_lit",
        "groupagg",
        "unpivot",
        "selectexpr",
        "explode",
        "intersect",
        "subtract",
        "cache",
        "tempview_after",
        "registertemptable",
    ]
    for op in ops:
        for body, d in (("add", "on_off"), ("hour", "utc_la")):

            def run(op: str = op, body: str = body, d: str = d) -> dict:
                family, text = BODY[body]
                cv, rv = DIRS[d]
                setk(KEY[family], cv)
                h = s.sql(text)
                setk(KEY[family], rv)
                if op == "cache":
                    cached = h.cache()
                    result = answer(cached)
                    cached.unpersist()
                    return result
                if op in ("tempview_after", "registertemptable"):
                    view = name("hv")
                    if op == "tempview_after":
                        h.createOrReplaceTempView(view)
                    else:
                        h.registerTempTable(view)
                    return answer(s.sql("SELECT m FROM " + view))
                return answer(derived(op, h))

            put("derive/" + op + "/" + body + "/" + d, "derive", run, body=body, dir=d)
    for body, d in (("add", "on_off"), ("hour", "utc_la")):

        def created_register(body: str = body, d: str = d) -> dict:
            family, text = BODY[body]
            cv, rv = DIRS[d]
            view = name("rt")
            setk(KEY[family], cv)
            s.sql(text).registerTempTable(view)
            setk(KEY[family], rv)
            return answer(s.sql("SELECT m FROM " + view))

        put("door/registertemptable/" + body + "/" + d, "door", created_register, body=body, dir=d)


def renames() -> None:
    for body, d in (("div", "on_off"), ("hour", "utc_la")):
        for spelling in ("VIEW", "TABLE"):

            def run(body: str = body, d: str = d, spelling: str = spelling) -> dict:
                family = BODY[body][0]
                cv, rv = DIRS[d]
                view = name("rv")
                setk(KEY[family], cv)
                make("sql", body, view)
                setk(KEY[family], rv)
                q("ALTER " + spelling + " " + view + " RENAME TO " + view + "r")
                return answer(s.sql("SELECT m FROM " + view + "r"))

            put(
                "rename/" + spelling.lower() + "/" + body + "/" + d, "rename", run, body=body, dir=d
            )

    def rename_control() -> dict:
        view = name("rv")
        q("CREATE OR REPLACE TEMP VIEW " + view + " AS SELECT 1 AS m")
        q("ALTER VIEW " + view + " RENAME TO " + view + "r")
        return answer(s.sql("SELECT m FROM " + view + "r"))

    put("rename/view/control/same", "rename", rename_control)
    for body, d in (("div", "on_off"), ("hour", "utc_la")):

        def alter_as(body: str = body, d: str = d) -> dict:
            family, text = BODY[body]
            cv, rv = DIRS[d]
            view = name("av")
            setk(KEY[family], cv)
            make("sql", body, view)
            setk(KEY[family], rv)
            q("ALTER VIEW " + view + " AS " + text)
            setk(KEY[family], cv)
            return answer(s.sql("SELECT m FROM " + view))

        put("alter_view_as/" + body + "/" + d, "direct_gap", alter_as, body=body, dir=d)

    def cache_as_select() -> dict:
        view = name("ct")
        q("CACHE TABLE " + view + " AS SELECT * FROM tzt")
        result = answer(s.sql("SELECT hour(ts) AS m FROM " + view))
        q("UNCACHE TABLE " + view)
        return result

    put("cache_table_as_select/hour/same", "direct_gap", cache_as_select)


def current_time() -> None:
    for body in ("lts", "cdate", "now"):
        for door in ("sql", "df"):
            for d in ("utc_la", "la_utc"):

                def run(body: str = body, door: str = door, d: str = d) -> dict:
                    cv, rv = DIRS[d]
                    view = name("tv")
                    setk(TZ, cv)
                    make(door, body, view)
                    setk(TZ, rv)
                    return answer(s.sql("SELECT m FROM " + view))

                put("now_fn/" + door + "/" + body + "/" + d, "now_fn", run, body=body, dir=d)


def shapes() -> None:
    for door in ("sql", "df"):
        for body, d in (("div", "on_off"), ("hour", "utc_la")):

            def replaced(door: str = door, body: str = body, d: str = d) -> dict:
                family = BODY[body][0]
                cv, rv = DIRS[d]
                view = name("cr")
                setk(KEY[family], cv)
                make(door, body, view)
                setk(KEY[family], rv)
                make(door, body, view)
                setk(KEY[family], cv)
                return answer(s.sql("SELECT m FROM " + view))

            put("replace/" + door + "/" + body + "/" + d, "shape", replaced, body=body, dir=d)
        for body, d in (("add", "on_off"), ("hour", "utc_la")):

            def self_join(door: str = door, body: str = body, d: str = d) -> dict:
                family = BODY[body][0]
                cv, rv = DIRS[d]
                view = name("sj")
                setk(KEY[family], cv)
                make(door, body, view)
                setk(KEY[family], rv)
                return answer(
                    s.sql("SELECT x.m, y.m FROM " + view + " x JOIN " + view + " y ON true")
                )

            put("self_join/" + door + "/" + body + "/" + d, "shape", self_join, body=body, dir=d)
        for scope, body, key, value in (("all", "div", ANSI, "false"), ("key", "hour", TZ, LA)):

            def reset(
                door: str = door,
                scope: str = scope,
                body: str = body,
                key: str = key,
                value: str = value,
            ) -> dict:
                view = name("rs")
                setk(key, value)
                make(door, body, view)
                q("RESET" if scope == "all" else "RESET " + key)
                after = {"after_reset": norm(s.conf.get(key))}
                return {**answer(s.sql("SELECT m FROM " + view)), **after}

            put("reset/" + scope + "/" + door + "/" + body, "shape", reset, body=body)
        for body, d in (("div", "on_off"), ("hour", "utc_la")):
            for order in ("conf_then_sql", "sql_then_conf"):

                def conf_set(
                    door: str = door, body: str = body, d: str = d, order: str = order
                ) -> dict:
                    family = BODY[body][0]
                    cv, rv = DIRS[d]
                    view = name("cs")
                    if order == "conf_then_sql":
                        s.conf.set(KEY[family], cv)
                    else:
                        setk(KEY[family], cv)
                    make(door, body, view)
                    if order == "conf_then_sql":
                        setk(KEY[family], rv)
                    else:
                        s.conf.set(KEY[family], rv)
                    return answer(s.sql("SELECT m FROM " + view))

                put(
                    "conf_set/" + order + "/" + door + "/" + body + "/" + d,
                    "shape",
                    conf_set,
                    body=body,
                    dir=d,
                )


def default_catalog() -> None:
    def direct() -> dict:
        setk("spark.sql.defaultCatalog", "lc")
        result = answer(s.sql("SELECT current_catalog() AS c"))
        setk("spark.sql.defaultCatalog", "spark_catalog")
        return result

    put("defcat/direct", "defcat", direct)
    for door in ("sql", "df"):

        def viewed(door: str = door) -> dict:
            view = name("dc")
            text = "SELECT current_catalog() AS c"
            if door == "sql":
                q("CREATE OR REPLACE TEMP VIEW " + view + " AS " + text)
            else:
                s.sql(text).createOrReplaceTempView(view)
            setk("spark.sql.defaultCatalog", "lc")
            result = answer(s.sql("SELECT c FROM " + view))
            setk("spark.sql.defaultCatalog", "spark_catalog")
            return result

        put("defcat/" + door, "defcat", viewed)


def held_writes() -> None:
    cases = [
        ("writeto", b, d) for b in ("hour", "str", "date", "lit") for d in ("utc_la", "la_utc")
    ]
    cases += [
        (w, b, d)
        for w in ("parquet", "saveastable", "insertinto")
        for b in ("hour", "lit")
        for d in ("utc_la", "la_utc")
    ]
    cases += [("writeto", "add", d) for d in ("on_off", "off_on")]
    for door, body, d in cases:

        def run(door: str = door, body: str = body, d: str = d) -> dict:
            family, text = BODY[body]
            cv, rv = DIRS[d]
            table = "lc.ns." + name("hw")
            setk(KEY[family], cv)
            h = s.sql(text)
            setk(KEY[family], rv)
            if door == "writeto":
                h.writeTo(table).create()
            elif door == "saveastable":
                h.write.format("iceberg").saveAsTable(table)
            elif door == "insertinto":
                q("CREATE TABLE " + table + " (m " + TYPES[body] + ") USING iceberg")
                h.write.insertInto(table)
            else:
                path = WH + "/out-" + table.replace(".", "_")
                h.write.parquet(path)
                baseline()
                return answer(s.read.parquet(path))
            baseline()
            return answer(s.sql("SELECT m FROM " + table))

        put("held_write/" + door + "/" + body + "/" + d, "held_write", run, body=body, dir=d)


fixtures()
directs()
filters()
caches()
derivations()
renames()
current_time()
shapes()
default_catalog()
held_writes()
log.close()
if ENGINE == "spark":
    s.stop()
shutil.rmtree(WH, ignore_errors=True)
