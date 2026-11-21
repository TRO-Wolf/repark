from __future__ import annotations

import contextlib
import io
import json
from collections.abc import Callable
from itertools import count
from pathlib import Path
from typing import Any

import _temp_view_session_settings_1_cells as one

LA, UTC, TK = "America/Los_Angeles", "UTC", "Asia/Tokyo"
ROWS = [(1, "2024-01-01 03:00:00"), (2, "2024-06-01 23:30:00")]
SHAPE = ["n", "CAST(ts AS STRING) AS t", "hour(ts) AS h", "unix_timestamp(ts) AS u"]
READERS = ("json_s", "csv_i")
COLUMNS = "n INT, ts TIMESTAMP, t STRING, h INT, u BIGINT"
VERBS = ("create", "createOrReplace", "append", "overwritePartitions")


class Cells:
    def __init__(self, session: Any, root: str) -> None:
        self.s = session
        self.root = root
        self.serial = count(1)
        self.found: dict[str, dict] = {}

    def conf(self, zone: str) -> None:
        self.s.sql("SET spark.sql.session.timeZone=" + zone).collect()

    def name(self, prefix: str = "cl") -> str:
        return prefix + str(next(self.serial))

    def put(self, cell: str, run: Callable[[], dict]) -> None:
        self.found[cell] = one.attempt(run)
        with contextlib.suppress(Exception):
            self.s.catalog.clearCache()
        self.conf(UTC)

    def fixtures(self) -> None:
        root = Path(self.root)
        (root / "json").mkdir()
        lines = "".join(json.dumps({"n": n, "ts": ts}) + "\n" for n, ts in ROWS)
        (root / "json" / "a.json").write_text(lines)
        (root / "csvh").mkdir()
        (root / "csvh" / "a.csv").write_text("n,ts\n" + "".join(f"{n},{ts}\n" for n, ts in ROWS))
        self.conf(UTC)
        self.s.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()

    def frame(self, reader: str, ts: bool = False) -> Any:
        self.conf(LA)
        read = self.s.read
        if reader == "json_s":
            source = read.schema("n INT, ts TIMESTAMP").json(self.root + "/json")
        else:
            source = read.option("header", "true").option("inferSchema", "true")
            source = source.csv(self.root + "/csvh")
        return source.selectExpr(*SHAPE[:1], *(["ts"] if ts else []), *SHAPE[1:])

    def rows(self, frame: Any) -> list[str]:
        return one.answer(frame)["rows"]

    def ordered(self, frame: Any) -> list[str]:
        return ["|".join(one.norm(x) for x in row) for row in frame.collect()]

    def child_own_unpersist(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        child = df.select("n", "u")
        self.conf(UTC)
        first = self.rows(child)
        df.unpersist()
        self.conf(TK)
        return {"rows": [first, self.rows(child), self.rows(df)]}

    def child_count_unpersist(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        child = df.select("n", "u")
        self.conf(UTC)
        child.count()
        df.unpersist()
        self.conf(TK)
        return {"rows": [self.rows(child), self.rows(df)]}

    def child_own_clear(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        child = df.select("n", "u")
        self.conf(UTC)
        first = self.rows(child)
        self.s.catalog.clearCache()
        self.conf(TK)
        return {"rows": [first, self.rows(child), self.rows(df)]}

    def child_after_fill(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        self.conf(UTC)
        df.count()
        child = df.select("n", "u")
        self.conf(TK)
        first = self.rows(child)
        df.unpersist()
        self.conf(LA)
        return {"rows": [first, self.rows(child), self.rows(df.select("u", "n"))]}

    def recache_own(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        self.conf(UTC)
        first = self.rows(df)
        df.unpersist()
        df.cache()
        self.conf(TK)
        child = df.select("n", "u")
        child.count()
        return {"rows": [first, self.rows(child), self.rows(df)]}

    def explain_child(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        child = df.select("n", "u")
        self.conf(UTC)
        with contextlib.redirect_stdout(io.StringIO()):
            child.explain()
        self.conf(TK)
        return {"rows": [self.rows(child), self.rows(df)]}

    def explain_view_sql(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        view = self.name("ev")
        df.createOrReplaceTempView(view)
        self.conf(UTC)
        self.s.sql("EXPLAIN SELECT * FROM " + view).collect()
        self.conf(TK)
        return {"rows": [self.rows(self.s.sql("SELECT * FROM " + view)), self.rows(df)]}

    def explain_first(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        with contextlib.redirect_stdout(io.StringIO()):
            df.explain()
        self.conf(UTC)
        return {"rows": [self.rows(df)]}

    def take_one(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        self.conf(UTC)
        df.take(1)
        self.conf(TK)
        return {"rows": [self.rows(df.select("n", "u")), self.rows(df)]}

    def limit_zero(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        self.conf(UTC)
        df.limit(0).count()
        self.conf(TK)
        return {"rows": [self.rows(df)]}

    def view_read(self, reader: str, door: str) -> dict:
        df = self.frame(reader).cache()
        view = self.name("cv")
        df.createOrReplaceTempView(view)
        self.conf(UTC)
        read = self.s.table(view) if door == "table" else self.s.sql("SELECT * FROM " + view)
        first = self.rows(read)
        self.conf(TK)
        return {"rows": [first, self.rows(df)]}

    def view_insert(self, reader: str) -> dict:
        table = "lc.db." + self.name("ti")
        self.conf(UTC)
        self.s.sql("CREATE TABLE " + table + " (" + COLUMNS + ") USING iceberg").collect()
        df = self.frame(reader, ts=True).cache()
        view = self.name("iv")
        df.createOrReplaceTempView(view)
        self.conf(UTC)
        self.s.sql("INSERT INTO " + table + " SELECT * FROM " + view).collect()
        return {"rows": [self.rows(self.s.table(table).selectExpr("n", "t", "h", "u"))]}

    def writer(self, reader: str, verb: str, fill: str) -> dict:
        table = "lc.db." + self.name("tw")
        df = self.frame(reader, ts=True).cache()
        if fill == "equal":
            self.conf(UTC)
            df.count()
            self.conf(LA)
        else:
            self.conf(UTC)
        if verb in ("append", "overwritePartitions"):
            self.s.sql("CREATE TABLE " + table + " (" + COLUMNS + ") USING iceberg").collect()
        write = df.writeTo(table)
        getattr(write, verb)()
        self.conf(UTC)
        return {"rows": [self.rows(self.s.table(table).selectExpr("n", "t", "h", "u"))]}

    def own_then_cache(self, reader: str) -> dict:
        df = self.frame(reader)
        self.conf(UTC)
        first = self.rows(df)
        df.cache()
        self.conf(TK)
        second = self.rows(df)
        df.count()
        derived = self.rows(df.select("n", "u"))
        return {"rows": [first, second, derived, self.rows(df)]}

    def child_before(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        child = df.select("n", "u")
        self.conf(UTC)
        first = self.rows(child)
        self.conf(TK)
        return {"rows": [first, self.rows(df)]}

    def siblings(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        a, b = df.select("n", "u"), df.filter("n > 0")
        self.conf(UTC)
        first = self.rows(a)
        self.conf(TK)
        return {"rows": [first, self.rows(b), self.rows(df)]}

    def clear_follows(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        child = df.select("n", "u")
        self.conf(UTC)
        df.count()
        self.s.catalog.clearCache()
        self.conf(TK)
        df.count()
        self.conf(LA)
        return {"rows": [self.rows(df), self.rows(child)]}

    def unpersist_blocking(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        self.conf(UTC)
        first = self.rows(df)
        df.unpersist(blocking=True)
        self.conf(TK)
        return {"rows": [first, self.rows(df), self.rows(df.select("n", "u"))]}

    def unpersist_reread(self, reader: str) -> dict:
        df = self.frame(reader).cache()
        self.conf(UTC)
        df.count()
        df.unpersist()
        self.conf(TK)
        return {"rows": [self.rows(df), self.rows(df.select("n", "u"))]}

    def ordered_first_use(self, reader: str) -> dict:
        df = self.frame(reader).orderBy("u", ascending=False).cache()
        self.conf(UTC)
        first = self.ordered(df.filter("n > 0").select("n", "u"))
        return {"rows": [first, self.ordered(df), self.ordered(df.select("n"))]}

    def run(self) -> dict[str, dict]:
        self.fixtures()
        named = [
            self.child_own_unpersist,
            self.child_count_unpersist,
            self.child_own_clear,
            self.child_after_fill,
            self.recache_own,
            self.explain_child,
            self.explain_view_sql,
            self.explain_first,
            self.take_one,
            self.limit_zero,
            self.view_insert,
            self.own_then_cache,
            self.child_before,
            self.siblings,
            self.clear_follows,
            self.unpersist_blocking,
            self.unpersist_reread,
            self.ordered_first_use,
        ]
        for reader in READERS:
            for cell in named:
                self.put(cell.__name__ + "/" + reader, lambda c=cell, r=reader: c(r))
            for door in ("table", "sql"):
                self.put(
                    "view_read/" + door + "/" + reader,
                    lambda d=door, r=reader: self.view_read(r, d),
                )
            for verb in VERBS:
                for fill in ("differ", "equal"):
                    self.put(
                        "writer/" + verb + "/" + fill + "/" + reader,
                        lambda v=verb, f=fill, r=reader: self.writer(r, v, f),
                    )
        return self.found
