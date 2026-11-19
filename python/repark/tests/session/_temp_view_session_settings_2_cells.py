from __future__ import annotations

from typing import Any

import _temp_view_session_settings_1_cells as one

ANSI, TZ, LA, KIRITIMATI = one.ANSI, one.TZ, one.LA, one.KIRITIMATI
BODIES = {"ansi": one.ANSI_BODIES, "zone": one.ZONE_BODIES}
KEYS = {"ansi": ANSI, "zone": TZ}
DOOR = {"s": "sql", "d": "df"}
HELD = {"add": ("ansi", one.ANSI_BODIES["add"]), "hour": ("zone", one.ZONE_BODIES["hour"])}
HELD |= {b: ("zone", one.ZONE_BODIES[b]) for b in ("str", "date", "lit")}
HELD_DIRS = {"on_off": ("true", "false"), "off_on": ("false", "true")}
HELD_DIRS |= {"utc_la": ("UTC", LA), "la_utc": (LA, "UTC")}
DERIVATIONS = (
    "alias",
    "union",
    "selectcol",
    "join",
    "crossjoin",
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
)
HELD_WRITES = [("writeto", b, d) for b in ("hour", "str", "lit") for d in ("utc_la", "la_utc")]
HELD_WRITES += [
    (w, b, d)
    for w in ("parquet", "saveastable", "insertinto")
    for b in ("hour", "lit")
    for d in ("utc_la", "la_utc")
]
HELD_WRITES += [(w, "add", d) for w in ("writeto", "parquet") for d in ("on_off", "off_on")]
NOW_READ = (
    "SELECT (m - hour(localtimestamp()) + 24) % 24 AS m, "
    "(m2 - hour(localtimestamp()) + 24) % 24 AS m2 FROM {V}"
)
EXPLAIN_QUERIES = {
    "read": "SELECT m FROM {V}",
    "filter": "SELECT m FROM {V} WHERE b <> 0",
    "limit": "SELECT m FROM {V} WHERE b <> 0 LIMIT 1",
    "join": "SELECT x.m FROM {V} x JOIN g y ON x.b = y.b",
    "nested": "SELECT m FROM {W}",
    "no_view": "SELECT b, 10 / b AS m FROM g WHERE b <> 0 LIMIT 1",
}


class Cells(one.Cells):
    def frame_view(self, body: str) -> str:
        name = self.name("d")
        self.s.sql(body).createOrReplaceTempView(name)
        return name

    def df_cross(self, key: str, created: str, read: str, body: str, reader: str) -> dict:
        self.set(key, created)
        name = self.frame_view(body)
        self.set(key, read)
        return self.read(reader.format(V=name))

    def family_cells(self) -> None:
        for family, bodies in BODIES.items():
            key = KEYS[family]
            for b, body in bodies.items():
                for created, read in one.DIRECTIONS[family]:
                    tag = family + "/" + b + "/" + created + ">" + read
                    for pos, form in one.POSITIONS.items():
                        self.put(
                            "read/df/" + pos + "/" + tag,
                            lambda k=key, c=created, r=read, bd=body, f=form: self.df_cross(
                                k, c, r, bd, f
                            ),
                        )
                    self.put(
                        "read/vsd/" + tag,
                        lambda k=key, c=created, r=read, bd=body: self.vsd(k, c, r, bd),
                    )
                    if b in one.WRITE_TYPES:
                        self.put(
                            "write/ctas-df/" + tag,
                            lambda k=key, c=created, r=read, bd=body: self.ctas(k, c, r, bd),
                        )

    def vsd(self, key: str, created: str, read: str, body: str) -> dict:
        self.set(key, created)
        inner = self.frame_view(body)
        outer = self.view("SELECT m FROM " + inner)
        self.set(key, read)
        return self.read("SELECT m FROM " + outer)

    def ctas(self, key: str, created: str, read: str, body: str) -> dict:
        self.set(key, created)
        name = self.frame_view(body)
        self.set(key, read)
        table = "lc.ns." + self.name("w")
        self.q("CREATE TABLE " + table + " USING iceberg AS SELECT m FROM " + name)
        self.set(TZ, "UTC")
        return self.read("SELECT m FROM " + table)

    def level(self, door: str, body: str) -> str:
        return self.view(body) if door == "sql" else self.frame_view(body)

    def nested_cells(self) -> None:
        inner_body = "SELECT ts, hour(ts) AS m, current_timezone() AS z FROM tzt"
        outer_body = "SELECT ts, m, z, hour(ts) AS m2, current_timezone() AS z2 FROM {I}"
        top_body = "SELECT m, z, m2, z2, hour(ts) AS m3 FROM {I}"

        def two(inner_door: str, outer_door: str) -> dict:
            inner = self.level(inner_door, inner_body)
            self.set(TZ, KIRITIMATI)
            outer = self.level(outer_door, outer_body.format(I=inner))
            self.set(TZ, LA)
            return self.read("SELECT m, z, m2, z2, current_timezone() AS r FROM " + outer)

        def three(doors: str) -> dict:
            first = self.level(DOOR[doors[0]], inner_body)
            self.set(TZ, KIRITIMATI)
            second = self.level(DOOR[doors[1]], outer_body.format(I=first))
            self.set(TZ, LA)
            third = self.level(DOOR[doors[2]], top_body.format(I=second))
            self.set(TZ, "UTC")
            return self.read("SELECT m, z, m2, z2, m3, current_timezone() AS r FROM " + third)

        def ansi(inner_door: str, outer_door: str, column: str) -> dict:
            self.set(ANSI, "true")
            inner = self.level(inner_door, "SELECT b, 10 / b AS m FROM fx")
            self.set(ANSI, "false")
            outer = self.level(outer_door, "SELECT m, 10 / b AS m2 FROM " + inner)
            self.set(ANSI, "true")
            return self.read("SELECT " + column + " FROM " + outer)

        for inner_door, outer_door in (("sql", "df"), ("df", "sql"), ("df", "df")):
            pair = inner_door + "_" + outer_door
            self.put("nest/zone/" + pair, lambda i=inner_door, o=outer_door: two(i, o))
            for column in ("m", "m2"):
                self.put(
                    "nest/ansi/" + pair + "/" + column,
                    lambda i=inner_door, o=outer_door, c=column: ansi(i, o, c),
                )
        for doors in ("sds", "dsd", "ssd", "dss"):
            self.put("nest/three/" + doors, lambda d=doors: three(d))

    def held(self, body: str, direction: str) -> Any:
        family, text = HELD[body]
        created, read = HELD_DIRS[direction]
        self.set(KEYS[family], created)
        frame = self.s.sql(text)
        self.set(KEYS[family], read)
        return frame

    def derived(self, op: str, frame: Any) -> Any:
        fn = self.functions()
        if op == "alias":
            return frame.alias(self.name("al"))
        if op == "union":
            return frame.union(frame)
        if op == "selectcol":
            return frame.select(fn.col("m"))
        if op == "join":
            left, right = self.name("l"), self.name("r")
            other = self.s.sql("SELECT 1 AS one").alias(right)
            joined = frame.alias(left).join(other, fn.lit(1) == fn.col(right + ".one"))
            return joined.select(left + ".m")
        if op == "crossjoin":
            return frame.crossJoin(self.s.sql("SELECT 1 AS one")).select("m")
        if op == "gagg_lit":
            return frame.select(fn.max("m").alias("m"), fn.lit(1).alias("one"))
        if op == "groupagg":
            return frame.groupBy().agg(fn.max("m").alias("m"))
        if op == "unpivot":
            wide = frame.withColumn("id", fn.lit(1))
            return wide.unpivot(["id"], ["m"], "k", "val").select("val")
        if op == "selectexpr":
            return frame.selectExpr("m")
        if op == "explode":
            return frame.select(fn.explode(fn.array(fn.col("m"))).alias("m"))
        if op == "intersect":
            return frame.intersect(frame)
        if op == "subtract":
            return frame.subtract(frame.filter(fn.col("m").isNull()))
        raise ValueError(op)

    def functions(self) -> Any:
        module = type(self.s).__module__.split(".")[0]
        if module == "pyspark":
            from pyspark.sql import functions

            return functions
        from repark.spark import functions

        return functions

    def derive(self, op: str, body: str, direction: str) -> dict:
        frame = self.held(body, direction)
        if op == "cache":
            cached = frame.cache()
            found = one.answer(cached)
            cached.unpersist()
            return found
        if op in ("tempview_after", "registertemptable"):
            view = self.name("hv")
            if op == "tempview_after":
                frame.createOrReplaceTempView(view)
            else:
                frame.registerTempTable(view)
            return self.read("SELECT m FROM " + view)
        return one.answer(self.derived(op, frame))

    def registered_before(self, body: str, direction: str) -> dict:
        family, text = HELD[body]
        created, read = HELD_DIRS[direction]
        view = self.name("rt")
        self.set(KEYS[family], created)
        self.s.sql(text).registerTempTable(view)
        self.set(KEYS[family], read)
        return self.read("SELECT m FROM " + view)

    def held_write(self, door: str, body: str, direction: str) -> dict:
        frame = self.held(body, direction)
        table = "lc.ns." + self.name("hw")
        if door == "writeto":
            frame.writeTo(table).using("iceberg").create()
        elif door == "saveastable":
            frame.write.format("iceberg").saveAsTable(table)
        elif door == "insertinto":
            self.q("CREATE TABLE " + table + " (m " + one.WRITE_TYPES[body] + ") USING iceberg")
            frame.write.insertInto(table)
        else:
            path = self.root + "/out-" + table.replace(".", "_")
            frame.write.parquet(path)
            self.baseline()
            return one.answer(self.s.read.parquet(path))
        self.baseline()
        return self.read("SELECT m FROM " + table)

    def held_cells(self) -> None:
        for op in DERIVATIONS:
            for body, direction in (("add", "on_off"), ("hour", "utc_la")):
                self.put(
                    "derive/" + op + "/" + body + "/" + direction,
                    lambda o=op, b=body, d=direction: self.derive(o, b, d),
                )
        for body, direction in (("add", "on_off"), ("hour", "utc_la")):
            self.put(
                "door/registertemptable/" + body + "/" + direction,
                lambda b=body, d=direction: self.registered_before(b, d),
            )
        for door, body, direction in HELD_WRITES:
            self.put(
                "held_write/" + door + "/" + body + "/" + direction,
                lambda w=door, b=body, d=direction: self.held_write(w, b, d),
            )

    def filter_cells(self) -> None:
        for shape, body, where, d in one.FILTERS:
            self.put(
                "filter/df/" + shape + "/" + d,
                lambda bd=body, w=where, d=d: self.df_filtered(
                    bd, "SELECT m FROM {V} WHERE " + w, d
                ),
            )
        for shape, body, reader, d in one.READER_FILTERS:
            self.put(
                "filter/df/" + shape + "/" + d,
                lambda bd=body, r=reader, d=d: self.df_filtered(bd, r, d),
            )

    def df_filtered(self, body: str, reader: str, direction: str) -> dict:
        family, text = one.FILTER_BODIES[body]
        created, read = one.FILTER_DIRECTIONS[direction]
        return self.df_cross(KEYS[family], created, read, text, reader)

    def shape_cells(self) -> None:
        div, hour, add = one.ANSI_BODIES["div"], one.ZONE_BODIES["hour"], one.ANSI_BODIES["add"]
        now = "SELECT hour(localtimestamp()) AS m, hour(now()) AS m2"

        def replace(key: str, first: str, second: str, read: str, body: str) -> dict:
            self.set(key, first)
            name = self.frame_view(body)
            self.set(key, second)
            self.s.sql(body).createOrReplaceTempView(name)
            self.set(key, read)
            return self.read("SELECT m FROM " + name)

        def reset(key: str, created: str, statement: str, body: str) -> dict:
            self.set(key, created)
            name = self.frame_view(body)
            self.q(statement)
            return self.read("SELECT m FROM " + name)

        def conf_set(key: str, created: str, read: str, body: str, order: str) -> dict:
            if order == "conf_then_sql":
                self.s.conf.set(key, created)
                name = self.frame_view(body)
                self.set(key, read)
            else:
                self.set(key, created)
                name = self.frame_view(body)
                self.s.conf.set(key, read)
            return self.read("SELECT m FROM " + name)

        def cached(body: str, key: str, created: str, read: str) -> dict:
            self.set(key, created)
            name = self.frame_view(body)
            self.q("CACHE TABLE " + name)
            self.set(key, read)
            found = self.read("SELECT m FROM " + name)
            found["cached"] = one.norm(self.s.catalog.isCached(name))
            self.s.catalog.uncacheTable(name)
            return found

        join = "SELECT x.m, y.m FROM {V} x JOIN {V} y ON true"
        self.put("replace/df/div", lambda: replace(ANSI, "true", "false", "true", div))
        self.put("replace/df/hour", lambda: replace(TZ, "UTC", LA, "UTC", hour))
        self.put("self_join/df/add", lambda: self.df_cross(ANSI, "true", "false", add, join))
        self.put("self_join/df/hour", lambda: self.df_cross(TZ, "UTC", LA, hour, join))
        self.put("reset/df/all", lambda: reset(ANSI, "false", "RESET", div))
        self.put("reset/df/key", lambda: reset(TZ, LA, "RESET " + TZ, hour))
        for order in ("conf_then_sql", "sql_then_conf"):
            self.put(
                "conf_set/df/" + order + "/div",
                lambda o=order: conf_set(ANSI, "true", "false", div, o),
            )
            self.put(
                "conf_set/df/" + order + "/hour",
                lambda o=order: conf_set(TZ, "UTC", LA, hour, o),
            )
        for created, read in (("UTC", LA), (LA, "UTC")):
            self.put(
                "now/df/" + created + ">" + read,
                lambda c=created, r=read: self.df_cross(TZ, c, r, now, NOW_READ),
            )
        self.put("cache/df/table_then_set", lambda: cached(hour, TZ, "UTC", LA))
        self.put("cache/df/ansi_off_cached_on", lambda: cached(add, ANSI, "false", "true"))

    def walker_cells(self) -> None:
        def dropped() -> dict:
            base = self.frame_view("SELECT ts FROM tzt")
            self.set(TZ, LA)
            middle = self.name("d")
            self.s.table(base).createOrReplaceTempView(middle)
            self.set(TZ, "UTC")
            outer = self.view("SELECT hour(ts) AS m FROM " + middle)
            self.s.catalog.dropTempView(base)
            return self.read("SELECT m FROM " + outer)

        def cycle() -> dict:
            self.set(TZ, LA)
            first = self.view("SELECT 1 AS m")
            middle = self.name("d")
            self.s.table(first).createOrReplaceTempView(middle)
            self.set(TZ, "UTC")
            self.q("CREATE OR REPLACE TEMP VIEW " + first + " AS SELECT m FROM " + middle)
            return self.read("SELECT m FROM " + first)

        self.put("walker/dropped_dependency", dropped)
        self.put("walker/cycle", cycle)

    def unset_cells(self) -> None:
        for b in ("div", "add"):

            def unset(body: str = one.ANSI_BODIES[b]) -> dict:
                name = self.frame_view(body)
                self.set(ANSI, "false")
                return self.read("SELECT m FROM " + name)

            self.put("unset/df/" + b, unset)

    def run(self) -> dict[str, dict]:
        self.fixtures()
        if self.mode == "unset":
            self.unset_cells()
            return self.found
        self.baseline()
        self.family_cells()
        self.filter_cells()
        self.nested_cells()
        self.held_cells()
        self.shape_cells()
        self.walker_cells()
        return self.found

    def explain(self) -> dict[str, str]:
        self.fixtures()
        self.baseline()
        name = self.frame_view(one.FILTER_BODIES["fdiv"][1])
        outer = self.name("d")
        self.s.sql("SELECT b, m FROM " + name).createOrReplaceTempView(outer)
        plans = {}
        for label, form in EXPLAIN_QUERIES.items():
            rows = self.s.sql("EXPLAIN " + form.format(V=name, W=outer)).collect()
            text = "\n".join(str(cell) for row in rows for cell in row)
            for pattern, token in one.EXPLAIN_NOISE:
                text = pattern.sub(token, text)
            plans[label] = text
        return plans
