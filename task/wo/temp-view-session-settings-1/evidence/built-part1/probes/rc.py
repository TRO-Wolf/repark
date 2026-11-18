import shutil
import sys
from itertools import count
from typing import Any

from lib import Log, answer, attempt, build

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
    "g": "SELECT * FROM VALUES (CAST(0 AS INT), CAST(2147483647 AS INT), "
    "CAST(3000000000 AS BIGINT), 'abc'),"
    " (CAST(2 AS INT), CAST(1 AS INT), CAST(5 AS BIGINT), '7') t(b, a, big, s)",
    "tzt": "SELECT TIMESTAMP '2024-01-01 02:30:00' AS ts",
}
BODY = {
    "fdiv": ("ansi", "SELECT b, 10 / b AS m FROM g"),
    "fadd": ("ansi", "SELECT b, a + 1 AS m FROM g"),
    "fagg": ("ansi", "SELECT b, sum(10 / b) AS m FROM g GROUP BY b"),
    "fcast": ("ansi", "SELECT b, big, CAST(s AS INT) AS m FROM g"),
    "fhour": ("tz", "SELECT ts, hour(ts) AS m FROM tzt"),
    "add": ("ansi", "SELECT a + 1 AS m FROM g WHERE b = 0"),
    "hour": ("tz", "SELECT hour(ts) AS m FROM tzt"),
    "str": ("tz", "SELECT CAST(ts AS STRING) AS m FROM tzt"),
    "date": ("tz", "SELECT CAST(ts AS DATE) AS m FROM tzt"),
}
KEY = {"ansi": ANSI, "tz": TZ}
DIRS = {
    "on_off": ("true", "false"),
    "off_on": ("false", "true"),
    "on_on": ("true", "true"),
    "utc_la": ("UTC", LA),
    "la_utc": (LA, "UTC"),
    "utc_kir": ("UTC", "Pacific/Kiritimati"),
    "utc_pago": ("UTC", "Pacific/Pago_Pago"),
}
TYPES = {"hour": "INT", "str": "STRING", "date": "DATE"}


def q(text: str) -> list:
    return s.sql(text).collect()


def setk(key: str, value: str) -> None:
    q("SET " + key + "=" + value)


def baseline() -> None:
    for key, value in ((ANSI, "true"), (TZ, "UTC")):
        attempt(lambda key=key, value=value: setk(key, value) or {})


def name(prefix: str) -> str:
    return prefix + str(next(serial))


def put(cell: str, run: Any) -> None:
    result = attempt(run)
    baseline()
    log.put({"cell": cell, "result": result})


def make(door: str, body: str, view: str) -> None:
    text = BODY[body][1]
    if door == "sql":
        q("CREATE OR REPLACE TEMP VIEW " + view + " AS " + text)
    else:
        s.sql(text).createOrReplaceTempView(view)


def fixtures() -> None:
    for fixture, text in FIX.items():
        path = WH + "/fx-" + fixture
        s.sql(text).write.mode("overwrite").parquet(path)
        s.read.parquet(path).createOrReplaceTempView(fixture)
    q("CREATE NAMESPACE IF NOT EXISTS lc.ns")


def directs() -> None:
    for body in ("fdiv", "fadd", "fagg", "fcast", "fhour"):
        family = BODY[body][0]
        for value in ("true", "false") if family == "ansi" else ("UTC", LA):

            def run(body: str = body, family: str = family, value: str = value) -> dict:
                setk(KEY[family], value)
                return answer(s.sql(BODY[body][1]))

            put("direct/" + body + "/" + value, run)
    for text in ("CAST(big AS INT) AS c FROM g", "b + 0 AS c FROM g"):
        for value in ("true", "false"):

            def run2(text: str = text, value: str = value) -> dict:
                setk(ANSI, value)
                return answer(s.sql("SELECT " + text))

            put("direct/" + text.split(" AS")[0] + "/" + value, run2)


SHAPES = [
    ("or_free", "fdiv", "b <> 0 OR m > 100", "on_off"),
    ("or_mixed", "fdiv", "b <> 0 OR b + 0 > 100", "on_off"),
    ("or_mixed", "fdiv", "b <> 0 OR b + 0 > 100", "off_on"),
    ("add_key", "fadd", "b = 2", "on_off"),
    ("add_m", "fadd", "m < 0", "off_on"),
    ("add_sens", "fadd", "m + 0 > 0", "on_off"),
    ("agg_key", "fagg", "b <> 0", "on_off"),
    ("agg_key_sens", "fagg", "b + 0 <> 0", "on_off"),
    ("agg_m", "fagg", "m > 1", "on_off"),
    ("agg_null", "fagg", "m IS NULL", "off_on"),
    ("cast_null", "fcast", "m IS NULL", "off_on"),
    ("numcast", "fcast", "CAST(big AS INT) > 0", "on_off"),
    ("numcast", "fcast", "CAST(big AS INT) > 0", "off_on"),
    ("zone_m", "fhour", "m = 2", "utc_la"),
    ("zone_fn", "fhour", "hour(ts) = 18", "utc_la"),
    ("zone_strcast", "fhour", "CAST(ts AS STRING) LIKE '2023-12-31 18%'", "utc_la"),
    ("in_list", "fdiv", "b IN (2, 3)", "on_off"),
    ("between", "fdiv", "b BETWEEN 1 AND 5", "on_off"),
    ("and_free", "fdiv", "NOT (b = 0) AND m > 1", "on_off"),
    ("and_free", "fdiv", "NOT (b = 0) AND m > 1", "on_on"),
    ("and_mixed", "fdiv", "b <> 0 AND b + 0 < 100", "on_off"),
    ("agg_limit", "fagg", "b <> 0 LIMIT 1", "on_off"),
]


def filters() -> None:
    for door in ("sql", "df"):
        for shape, body, where, d in SHAPES:

            def run(door: str = door, body: str = body, where: str = where, d: str = d) -> dict:
                family = BODY[body][0]
                cv, rv = DIRS[d]
                view = name("fv")
                setk(KEY[family], cv)
                make(door, body, view)
                setk(KEY[family], rv)
                return answer(s.sql("SELECT m FROM " + view + " WHERE " + where))

            put("filter/" + shape + "/" + door + "/" + body + "/" + d, run)


def caches() -> None:
    def sql_then_set() -> dict:
        view = name("cv")
        make("sql", "hour", view)
        q("CACHE TABLE " + view)
        setk(TZ, LA)
        out = answer(s.sql("SELECT m FROM " + view))
        out["cached"] = str(s.catalog.isCached(view))
        q("UNCACHE TABLE " + view)
        return out

    def df_lazy_then_set() -> dict:
        view = name("cv")
        make("df", "hour", view)
        q("CACHE LAZY TABLE " + view)
        setk(TZ, LA)
        out = answer(s.sql("SELECT m FROM " + view))
        q("UNCACHE TABLE " + view)
        return out

    def set_then_cache() -> dict:
        view = name("cv")
        make("sql", "hour", view)
        setk(TZ, LA)
        q("CACHE TABLE " + view)
        out = answer(s.sql("SELECT m FROM " + view))
        q("UNCACHE TABLE " + view)
        return out

    def ansi_off_cached_on() -> dict:
        view = name("cv")
        setk(ANSI, "false")
        make("df", "add", view)
        setk(ANSI, "true")
        q("CACHE TABLE " + view)
        out = answer(s.sql("SELECT m FROM " + view))
        q("UNCACHE TABLE " + view)
        return out

    for cell_fn in (sql_then_set, df_lazy_then_set, set_then_cache, ansi_off_cached_on):
        put("cache/" + cell_fn.__name__, cell_fn)


def derived(op: str, h: Any) -> Any:
    if op == "selectexpr":
        return h.selectExpr("m")
    if op == "join":
        other = s.sql("SELECT 1 AS one")
        left, right = name("l"), name("r")
        return (
            h.alias(left)
            .join(other.alias(right), fn.lit(1) == fn.col(right + ".one"))
            .select(left + ".m")
        )
    if op == "unpivot":
        return h.withColumn("id", fn.lit(1)).unpivot(["id"], ["m"], "k", "val").select("val")
    if op == "explode":
        return h.select(fn.explode(fn.array(fn.col("m"))).alias("m"))
    if op == "intersect":
        return h.intersect(h)
    raise ValueError(op)


def derivations() -> None:
    for op in ("selectexpr", "join", "unpivot", "explode", "intersect", "regtt"):
        for body, d in (("add", "on_off"), ("hour", "utc_la")):

            def run(op: str = op, body: str = body, d: str = d) -> dict:
                family, text = BODY[body]
                cv, rv = DIRS[d]
                setk(KEY[family], cv)
                h = s.sql(text)
                setk(KEY[family], rv)
                if op == "regtt":
                    view = name("rt")
                    h.registerTempTable(view)
                    return answer(s.sql("SELECT m FROM " + view))
                return answer(derived(op, h))

            put("derive/" + op + "/" + body + "/" + d, run)
    for body, d in (("add", "on_off"), ("hour", "utc_la")):

        def created_register(body: str = body, d: str = d) -> dict:
            family, text = BODY[body]
            cv, rv = DIRS[d]
            view = name("rt")
            setk(KEY[family], cv)
            s.sql(text).registerTempTable(view)
            setk(KEY[family], rv)
            return answer(s.sql("SELECT m FROM " + view))

        put("door/registertemptable/" + body + "/" + d, created_register)


def renames() -> None:
    def view_no_set() -> dict:
        view = name("rv")
        q("CREATE OR REPLACE TEMP VIEW " + view + " AS SELECT 1 AS m")
        q("ALTER VIEW " + view + " RENAME TO " + view + "r")
        return answer(s.sql("SELECT m FROM " + view + "r"))

    put("rename/view_no_set", view_no_set)


def current_time() -> None:
    for door in ("sql", "df"):
        for d in ("utc_kir", "utc_pago"):

            def run(door: str = door, d: str = d) -> dict:
                cv, rv = DIRS[d]
                view = name("ct")
                setk(TZ, cv)
                text = "SELECT CAST(current_date() AS STRING) AS m"
                if door == "sql":
                    q("CREATE OR REPLACE TEMP VIEW " + view + " AS " + text)
                else:
                    s.sql(text).createOrReplaceTempView(view)
                setk(TZ, rv)
                out = answer(s.sql("SELECT m FROM " + view))
                out["direct"] = answer(s.sql(text))["rows"]
                return out

            put("current_date/" + door + "/" + d, run)


def held_writes() -> None:
    cases = [("writeto", b, d) for b in ("hour", "str", "date") for d in ("utc_la", "la_utc")]
    cases += [("saveastable", "hour", "utc_la"), ("insertinto", "hour", "utc_la")]
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
            else:
                q("CREATE TABLE " + table + " (m " + TYPES[body] + ") USING iceberg")
                h.write.insertInto(table)
            baseline()
            return answer(s.sql("SELECT m FROM " + table))

        put("held_write/" + door + "/" + body + "/" + d, run)


fixtures()
directs()
filters()
caches()
derivations()
renames()
current_time()
held_writes()
log.close()
if ENGINE == "spark":
    s.stop()
shutil.rmtree(WH, ignore_errors=True)
