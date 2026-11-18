from __future__ import annotations

import re
from collections.abc import Callable
from decimal import Decimal
from itertools import count
from typing import Any

ANSI = "spark.sql.ansi.enabled"
TZ = "spark.sql.session.timeZone"
LA = "America/Los_Angeles"
KIRITIMATI = "Pacific/Kiritimati"
CONDITION = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
M38 = "9" * 38

FIXTURES = {
    "fx": "SELECT CAST(2147483647 AS INT) AS a, CAST(0 AS INT) AS b, 'abc' AS s, true AS t",
    "fpw": "SELECT CAST(v AS DECIMAL(38,0)) AS x FROM (VALUES ('"
    + M38
    + "'), ('"
    + M38
    + "')) t(v)",
    "tzt": "SELECT TIMESTAMP '2024-01-01 02:30:00' AS ts, '2024-01-01 02:30:00' AS s, "
    "DATE '2024-01-01' AS dt",
    "fmk": "SELECT map(named_struct('i', 1, 'j', 2), 7) AS mk",
    "cs": "SELECT CAST(1 AS INT) AS a",
    "fdec": "SELECT CAST(k AS INT) AS k, CAST(x0 AS DECIMAL(38,0)) AS x0, "
    "CAST(x10 AS DECIMAL(38,10)) AS x10, CAST(xd AS DECIMAL(10,2)) AS xd FROM (VALUES "
    "(0, '1', NULL, NULL), (1, NULL, '1.5', '2.25')) t(k, x0, x10, xd)",
    "g": "SELECT * FROM VALUES (CAST(0 AS INT), CAST(2147483647 AS INT), CAST(3000000000 AS "
    "BIGINT), 'abc'), (CAST(2 AS INT), CAST(1 AS INT), CAST(5 AS BIGINT), '7') t(b, a, big, s)",
}

ANSI_BODIES = {
    "div": "SELECT 10 / b AS m FROM fx",
    "mod": "SELECT 10 % b AS m FROM fx",
    "add": "SELECT a + 1 AS m FROM fx",
    "avg": "SELECT avg(x) AS m FROM fpw",
    "cast_dbl": "SELECT CAST(s AS DOUBLE) AS m FROM fx",
    "dbl_lit": "SELECT CAST('abc' AS DOUBLE) AS m",
    "int_bin": "SELECT hex(CAST(a AS BINARY)) AS m FROM fx",
    "bool_dec": "SELECT CAST(t AS DECIMAL(1,1)) AS m FROM fx",
    "map_struct": "SELECT mk[named_struct('j', 1, 'k', 2)] AS m FROM fmk",
}
ZONE_BODIES = {
    "hour": "SELECT hour(ts) AS m FROM tzt",
    "str": "SELECT CAST(ts AS STRING) AS m FROM tzt",
    "date": "SELECT CAST(ts AS DATE) AS m FROM tzt",
    "fmt": "SELECT date_format(ts, 'yyyy-MM-dd HH') AS m FROM tzt",
    "parse": "SELECT unix_timestamp(CAST(s AS TIMESTAMP)) AS m FROM tzt",
    "lit": "SELECT unix_timestamp(TIMESTAMP '2024-01-01 02:30:00') AS m",
    "fromunix": "SELECT from_unixtime(0) AS m",
    "date_ts": "SELECT unix_timestamp(CAST(dt AS TIMESTAMP)) AS m FROM tzt",
    "trunc": "SELECT CAST(date_trunc('DAY', ts) AS STRING) AS m FROM tzt",
    "curtz": "SELECT current_timezone() AS m",
}
OTHER_BODIES = {
    "case": ("spark.sql.caseSensitive", "false", "true", "SELECT A AS m FROM cs"),
    "dec_ct": (
        "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate",
        "false",
        "true",
        "SELECT CASE WHEN k = 0 THEN x0 ELSE x10 END AS m FROM fdec",
    ),
    "dec_dbl": (
        "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate",
        "false",
        "true",
        "SELECT coalesce(xd, CAST(1.5 AS DOUBLE)) AS m FROM fdec",
    ),
}
DIRECTIONS = {"ansi": [("true", "false"), ("false", "true")], "zone": [("UTC", LA), (LA, "UTC")]}
POSITIONS = {
    "plain": "SELECT m FROM {V}",
    "cte": "WITH c AS (SELECT m FROM {V}) SELECT m FROM c",
    "join": "SELECT v.m FROM {V} v CROSS JOIN (SELECT 1 AS j) q",
    "subq": "SELECT (SELECT max(m) FROM {V}) AS m",
}
FILTER_BODIES = {
    "fdiv": ("ansi", "SELECT b, 10 / b AS m FROM g"),
    "fadd": ("ansi", "SELECT b, a + 1 AS m FROM g"),
    "fagg": ("ansi", "SELECT b, sum(10 / b) AS m FROM g GROUP BY b"),
    "fcast": ("ansi", "SELECT b, big, CAST(s AS INT) AS m FROM g"),
    "fhour": ("zone", "SELECT ts, hour(ts) AS m FROM tzt"),
    "fab": ("ansi", "SELECT a, b FROM g"),
}
FILTER_DIRECTIONS = {
    "on_off": ("true", "false"),
    "off_on": ("false", "true"),
    "on_on": ("true", "true"),
    "utc_la": ("UTC", LA),
}
FILTERS = [
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
READER_FILTERS = [
    ("reader_sensitive_kept", "fab", "SELECT b FROM {V} WHERE b + 2147483647 < 0", "on_off"),
    ("reader_limit_above", "fab", "SELECT b FROM {V} WHERE b + 0 = 2 LIMIT 1", "on_off"),
    ("reader_limit_free", "fab", "SELECT b FROM {V} WHERE b <> 5 LIMIT 5", "on_off"),
]
RULED_LOUD = {
    "filter/or_mixed/on_off": "DIVIDE_BY_ZERO",
    "filter/agg_key_sens/on_off": "DIVIDE_BY_ZERO",
    "filter/and_free/on_off": "DIVIDE_BY_ZERO",
}
WRITE_TYPES = {"add": "INT", "div": "DOUBLE", "hour": "INT", "str": "STRING", "lit": "BIGINT"}
EXPLAIN_QUERIES = {
    "read": "SELECT m FROM {V}",
    "filter": "SELECT m FROM {V} WHERE b <> 0",
    "limit": "SELECT m FROM {V} WHERE b <> 0 LIMIT 1",
    "join": "SELECT x.m FROM {V} x JOIN g y ON x.b = y.b",
    "nested": "SELECT m FROM {W}",
}
EXPLAIN_NOISE = [
    (re.compile(r"__repark_[a-z_]*?[0-9a-f]{32}"), "<scratch>"),
    (re.compile(r"file_groups=\{[^}]*\}"), "file_groups=<files>"),
    (re.compile(r"/[^\s,\]]*\.parquet"), "<file>"),
    (re.compile(r"RoundRobinBatch\(\d+\)"), "RoundRobinBatch(<n>)"),
    (re.compile(r"input_partitions=\d+"), "input_partitions=<n>"),
]


def norm(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        pairs = sorted(value.items(), key=lambda p: str(p[0]))
        return "{" + ",".join(norm(k) + ":" + norm(v) for k, v in pairs) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(norm(v) for v in value) + "]"
    return str(value)


def answer(frame: Any) -> dict:
    rows = sorted("|".join(norm(x) for x in row) for row in frame.collect())
    return {"rows": rows, "types": [f.dataType.simpleString() for f in frame.schema.fields]}


def attempt(run: Callable[[], dict]) -> dict:
    try:
        return run()
    except Exception as error:
        text = " ".join(str(error).split())
        found = CONDITION.search(text)
        cut = text if len(text) <= 200 else text[:200].rsplit(" ", 1)[0]
        return {"error": found.group(1) if found else "", "text": cut}


class Cells:
    def __init__(self, session: Any, root: str, mode: str) -> None:
        self.s = session
        self.root = root
        self.mode = mode
        self.serial = count(1)
        self.found: dict[str, dict] = {}

    def q(self, text: str) -> list:
        return self.s.sql(text).collect()

    def set(self, key: str, value: str) -> None:
        self.q("SET " + key + "=" + value)

    def baseline(self) -> None:
        if self.mode != "unset":
            self.set(ANSI, self.mode)
        self.set(TZ, "UTC")
        self.set("spark.sql.caseSensitive", "false")
        self.set("spark.sql.legacy.decimal.retainFractionDigitsOnTruncate", "false")

    def name(self, prefix: str = "v") -> str:
        return prefix + str(next(self.serial))

    def put(self, cell: str, run: Callable[[], dict]) -> None:
        self.found[cell] = attempt(run)
        self.baseline()

    def read(self, text: str) -> dict:
        return answer(self.s.sql(text))

    def view(self, body: str) -> str:
        name = self.name()
        self.q("CREATE OR REPLACE TEMP VIEW " + name + " AS " + body)
        return name

    def fixtures(self) -> None:
        for name, text in FIXTURES.items():
            path = self.root + "/fx-" + name
            self.s.sql(text).write.mode("overwrite").parquet(path)
            self.s.read.parquet(path).createOrReplaceTempView(name)
        self.q("CREATE NAMESPACE IF NOT EXISTS lc.ns")

    def cross(self, key: str, created: str, read: str, body: str, reader: str) -> dict:
        self.set(key, created)
        name = self.view(body)
        self.set(key, read)
        return self.read(reader.format(V=name))

    def family_cells(self) -> None:
        families = (("ansi", ANSI, ANSI_BODIES), ("zone", TZ, ZONE_BODIES))
        for family, key, bodies in families:
            for b, body in bodies.items():
                for created, read in DIRECTIONS[family]:
                    tag = family + "/" + b + "/" + created + ">" + read
                    for pos, form in POSITIONS.items():
                        self.put(
                            "read/sql/" + pos + "/" + tag,
                            lambda k=key, c=created, r=read, bd=body, f=form: self.cross(
                                k, c, r, bd, f
                            ),
                        )
                    for door in ("vss", "vds", "tsql", "ren"):
                        self.put(
                            "read/" + door + "/" + tag,
                            lambda d=door, k=key, c=created, r=read, bd=body: self.door(
                                d, k, c, r, bd
                            ),
                        )
        for b, (key, created, read, body) in OTHER_BODIES.items():
            for c, r in ((created, read), (read, created)):
                self.put(
                    "read/sql/plain/other/" + b + "/" + c + ">" + r,
                    lambda k=key, c=c, r=r, bd=body: self.cross(k, c, r, bd, POSITIONS["plain"]),
                )

    def door(self, door: str, key: str, created: str, read: str, body: str) -> dict:
        self.set(key, created)
        inner = self.view(body)
        if door == "vss":
            name = self.view("SELECT m FROM " + inner)
        elif door == "vds":
            name = self.name("d")
            self.s.sql("SELECT m FROM " + inner).createOrReplaceTempView(name)
        elif door == "tsql":
            held = self.s.table(inner)
            self.set(key, read)
            return answer(held)
        else:
            name = self.name("r")
            self.set(key, read)
            self.q("ALTER TABLE " + inner + " RENAME TO " + name)
            return self.read("SELECT m FROM " + name)
        self.set(key, read)
        return self.read("SELECT m FROM " + name)

    def filter_cells(self) -> None:
        for shape, body, where, d in FILTERS:
            self.put(
                "filter/" + shape + "/" + d,
                lambda bd=body, w=where, d=d: self.filtered(bd, "SELECT m FROM {V} WHERE " + w, d),
            )
        for shape, body, reader, d in READER_FILTERS:
            self.put(
                "filter/" + shape + "/" + d, lambda bd=body, r=reader, d=d: self.filtered(bd, r, d)
            )

    def filtered(self, body: str, reader: str, direction: str) -> dict:
        family, text = FILTER_BODIES[body]
        created, read = FILTER_DIRECTIONS[direction]
        key = ANSI if family == "ansi" else TZ
        return self.cross(key, created, read, text, reader)

    def nested_cells(self) -> None:
        def nested() -> dict:
            inner = self.view("SELECT ts, hour(ts) AS m, current_timezone() AS z FROM tzt")
            self.set(TZ, KIRITIMATI)
            outer = self.view("SELECT m, z, hour(ts) AS m2, current_timezone() AS z2 FROM " + inner)
            self.set(TZ, LA)
            return self.read("SELECT m, z, m2, z2, current_timezone() AS r FROM " + outer)

        def nested_ansi() -> dict:
            self.set(ANSI, "true")
            inner = self.view("SELECT b, 10 / b AS m FROM fx")
            self.set(ANSI, "false")
            outer = self.view("SELECT m, 10 / b AS m2 FROM " + inner)
            self.set(ANSI, "true")
            return self.read("SELECT m2 FROM " + outer)

        def now_pair() -> dict:
            self.set(TZ, LA)
            name = self.view("SELECT current_timezone() AS tz, current_timestamp() AS ct")
            self.set(TZ, "UTC")
            return self.read(
                "SELECT tz, current_timezone() AS r, ct = current_timestamp() AS same FROM " + name
            )

        self.put("nest/current_timezone", nested)
        self.put("nest/ansi_outer", nested_ansi)
        self.put("now/current_timezone_and_timestamp", now_pair)

    def shape_cells(self) -> None:
        def replace(key: str, first: str, second: str, read: str, body: str) -> dict:
            self.set(key, first)
            name = self.view(body)
            self.set(key, second)
            self.q("CREATE OR REPLACE TEMP VIEW " + name + " AS " + body)
            self.set(key, read)
            return self.read("SELECT m FROM " + name)

        def self_join(key: str, created: str, read: str, body: str) -> dict:
            return self.cross(
                key, created, read, body, "SELECT x.m, y.m FROM {V} x JOIN {V} y ON true"
            )

        def reset(key: str, created: str, statement: str, body: str) -> dict:
            self.set(key, created)
            name = self.view(body)
            self.q(statement)
            return self.read("SELECT m FROM " + name)

        def conf_set(key: str, created: str, read: str, body: str, order: str) -> dict:
            if order == "conf_then_sql":
                self.s.conf.set(key, created)
                name = self.view(body)
                self.set(key, read)
            else:
                self.set(key, created)
                name = self.view(body)
                self.s.conf.set(key, read)
            return self.read("SELECT m FROM " + name)

        div, hour, add = ANSI_BODIES["div"], ZONE_BODIES["hour"], ANSI_BODIES["add"]
        self.put("replace/div", lambda: replace(ANSI, "true", "false", "true", div))
        self.put("replace/hour", lambda: replace(TZ, "UTC", LA, "UTC", hour))
        self.put("self_join/add", lambda: self_join(ANSI, "true", "false", add))
        self.put("self_join/hour", lambda: self_join(TZ, "UTC", LA, hour))
        self.put("reset/all", lambda: reset(ANSI, "false", "RESET", div))
        self.put("reset/key", lambda: reset(TZ, LA, "RESET " + TZ, hour))
        for order in ("conf_then_sql", "sql_then_conf"):
            self.put(
                "conf_set/" + order + "/div",
                lambda o=order: conf_set(ANSI, "true", "false", div, o),
            )
            self.put(
                "conf_set/" + order + "/hour",
                lambda o=order: conf_set(TZ, "UTC", LA, hour, o),
            )

    def write_cells(self) -> None:
        bodies = {**ANSI_BODIES, **ZONE_BODIES}
        for b, sqltype in WRITE_TYPES.items():
            family = "ansi" if b in ANSI_BODIES else "zone"
            key = ANSI if family == "ansi" else TZ
            for created, read in DIRECTIONS[family]:
                tag = b + "/" + created + ">" + read
                for door in ("ctas", "insert", "writeto"):
                    self.put(
                        "write/" + door + "/" + tag,
                        lambda d=door, k=key, c=created, r=read, bd=bodies[b], t=sqltype: (
                            self.write(d, k, c, r, bd, t)
                        ),
                    )

    def write(self, door: str, key: str, created: str, read: str, body: str, sqltype: str) -> dict:
        self.set(key, created)
        name = self.view(body)
        held = self.s.table(name)
        self.set(key, read)
        table = "lc.ns." + self.name("w")
        if door == "ctas":
            self.q("CREATE TABLE " + table + " USING iceberg AS SELECT m FROM " + name)
        elif door == "insert":
            self.q("CREATE TABLE " + table + " (m " + sqltype + ") USING iceberg")
            self.q("INSERT INTO " + table + " SELECT m FROM " + name)
        else:
            held.writeTo(table).using("iceberg").create()
        self.set(TZ, "UTC")
        return self.read("SELECT m FROM " + table)

    def cache_cells(self) -> None:
        hour, add = ZONE_BODIES["hour"], ANSI_BODIES["add"]

        def cached(body: str, key: str, created: str, read: str, how: str, before: bool) -> dict:
            self.set(key, created)
            name = self.view(body)
            if before:
                self.set(key, read)
            if how == "catalog":
                self.s.catalog.cacheTable(name)
            else:
                self.q(("CACHE LAZY TABLE " if how == "lazy" else "CACHE TABLE ") + name)
            self.set(key, read)
            out = self.read("SELECT m FROM " + name)
            out["cached"] = norm(self.s.catalog.isCached(name))
            self.s.catalog.uncacheTable(name)
            return out

        self.put("cache/sql_then_set", lambda: cached(hour, TZ, "UTC", LA, "eager", False))
        self.put("cache/lazy_then_set", lambda: cached(hour, TZ, "UTC", LA, "lazy", False))
        self.put("cache/catalog_then_set", lambda: cached(hour, TZ, "UTC", LA, "catalog", False))
        self.put("cache/set_then_cache", lambda: cached(hour, TZ, "UTC", LA, "eager", True))
        self.put(
            "cache/ansi_off_cached_on", lambda: cached(add, ANSI, "false", "true", "eager", True)
        )

    def unset_cells(self) -> None:
        for b in ("div", "add"):
            body = ANSI_BODIES[b]

            def unset(body: str = body) -> dict:
                name = self.view(body)
                self.set(ANSI, "false")
                return self.read("SELECT m FROM " + name)

            self.put("unset/" + b, unset)

    def run(self) -> dict[str, dict]:
        self.fixtures()
        if self.mode == "unset":
            self.unset_cells()
            return self.found
        self.baseline()
        self.family_cells()
        self.filter_cells()
        self.nested_cells()
        self.shape_cells()
        self.write_cells()
        self.cache_cells()
        return self.found

    def explain(self) -> dict[str, str]:
        self.fixtures()
        self.baseline()
        name = self.view(FILTER_BODIES["fdiv"][1])
        outer = self.view("SELECT m FROM " + name)
        plans = {}
        for label, form in EXPLAIN_QUERIES.items():
            rows = self.s.sql("EXPLAIN " + form.format(V=name, W=outer)).collect()
            text = "\n".join(str(cell) for row in rows for cell in row)
            for pattern, token in EXPLAIN_NOISE:
                text = pattern.sub(token, text)
            plans[label] = text
        return plans
