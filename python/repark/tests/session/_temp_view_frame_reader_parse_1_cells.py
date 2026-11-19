from __future__ import annotations

import json
from collections.abc import Callable
from itertools import count
from pathlib import Path
from typing import Any

import _temp_view_session_settings_1_cells as one

LA, UTC, TK = "America/Los_Angeles", "UTC", "Asia/Tokyo"
ROWS = [(1, "2024-01-01 03:00:00"), (2, "2024-06-01 23:30:00")]
ZONES = {"la_utc": (LA, UTC), "utc_la": (UTC, LA)}
ANSIS = {"on_off": ("true", "false"), "off_on": ("false", "true")}
SHAPE = ["n", "CAST(ts AS STRING) AS t", "hour(ts) AS b", "CAST(unix_timestamp(ts) AS STRING) AS u"]
VIEW_READ = "SELECT n, b, unix_timestamp(ts) AS u FROM {V}"
E5_READ = "SELECT n, {C}b, CAST(ts AS STRING) AS t, unix_timestamp(ts) AS u FROM {{V}}"
NESTED = ["n", "CAST(ts AS STRING) AS t", "h1", "h2", "hour(ts) AS h3", "unix_timestamp(ts) AS u"]
SUBQ = (
    "SELECT n, (SELECT max(unix_timestamp(ts)) FROM {V}) AS mu, "
    "(SELECT max(hour(ts)) FROM {V}) AS mh FROM {P} "
    "WHERE n IN (SELECT n FROM {V} WHERE hour(ts) >= 12)"
)
STICKY_SQL = (
    "SELECT n, CAST(ts AS STRING) AS t, hour(ts) AS b, "
    "CAST(unix_timestamp(ts) AS STRING) AS u FROM {V}"
)
STICKY_READERS = ("json_s", "csv_s", "csv_i")
FIRST_ACTIONS: dict[str, Callable[[Any], object]] = {
    "toLocalIterator": lambda frame: list(frame.toLocalIterator()),
    "toArrow": lambda frame: frame.toArrow(),
    "tail": lambda frame: frame.tail(2),
    "foreach": lambda frame: frame.foreach(lambda row: None),
    "foreachPartition": lambda frame: frame.foreachPartition(lambda rows: None),
    "localCheckpoint": lambda frame: frame.localCheckpoint(),
    "checkpoint": lambda frame: frame.checkpoint(),
}


class Cells:
    def __init__(self, session: Any, root: str) -> None:
        self.s = session
        self.root = root
        self.serial = count(1)
        self.found: dict[str, dict] = {}

    def conf(self, zone: str, ansi: str = "true", session: Any = None) -> None:
        target = self.s if session is None else session
        target.sql("SET spark.sql.session.timeZone=" + zone).collect()
        target.sql("SET spark.sql.ansi.enabled=" + ansi).collect()

    def name(self, prefix: str = "rp") -> str:
        return prefix + str(next(self.serial))

    def put(self, cell: str, run: Callable[[], dict]) -> None:
        self.found[cell] = one.attempt(run)
        self.conf(UTC)

    def fixtures(self) -> None:
        root = Path(self.root)
        (root / "json").mkdir()
        lines = "".join(json.dumps({"n": n, "ts": ts}) + "\n" for n, ts in ROWS)
        (root / "json" / "a.json").write_text(lines)
        (root / "csvh").mkdir()
        (root / "csvh" / "a.csv").write_text("n,ts\n" + "".join(f"{n},{ts}\n" for n, ts in ROWS))
        (root / "csv0").mkdir()
        (root / "csv0" / "a.csv").write_text("".join(f"{n},{ts}\n" for n, ts in ROWS))
        self.conf(UTC)
        self.s.sql(
            "SELECT CAST(n AS INT) AS n, CAST(ts AS TIMESTAMP) AS ts FROM VALUES "
            "(1, '2024-01-01 03:00:00'), (2, '2024-06-01 23:30:00') t(n, ts)"
        ).write.mode("overwrite").parquet(self.root + "/pq")

    def reader(self, name: str, session: Any = None) -> Any:
        read, csv = (self.s if session is None else session).read, self.root + "/csvh"
        if name == "json_s":
            return read.schema("n INT, ts TIMESTAMP").json(self.root + "/json")
        if name == "csv_s":
            return read.schema("n INT, ts TIMESTAMP").option("header", "true").csv(csv)
        if name == "csv_i":
            return read.option("header", "true").option("inferSchema", "true").csv(csv)
        return read.parquet(self.root + "/pq")

    def side(self) -> Any:
        return self.s.read.parquet(self.root + "/pq").selectExpr("n AS m", "hour(ts) AS ph")

    def view_cell(self, build: Callable[[str], None], made: str, read: str, query: str) -> dict:
        view = self.name()
        self.conf(made)
        build(view)
        self.conf(read)
        return one.answer(self.s.sql(query.format(V=view)))

    def eb_hour(self, reader: str, view: str) -> None:
        self.reader(reader).selectExpr("n", "ts", "hour(ts) AS b").createOrReplaceTempView(view)

    def sql_view(self, reader: str, view: str) -> None:
        self.reader(reader).createOrReplaceTempView(view + "f")
        body = "SELECT n, ts, hour(ts) AS b FROM " + view + "f"
        self.s.sql("CREATE OR REPLACE TEMP VIEW " + view + " AS " + body)

    def cj(self, reader: str, view: str) -> None:
        frame, other = self.reader(reader), self.side()
        joined = frame.join(other, frame.n == other.m)
        joined.selectExpr("n", "ts", "ph", "hour(ts) AS b").createOrReplaceTempView(view)

    def cross(self, reader: str, view: str) -> None:
        crossed = self.reader(reader).crossJoin(self.side().limit(1))
        crossed.selectExpr("n", "ts", "hour(ts) AS b").createOrReplaceTempView(view)

    def mul(self, reader: str, view: str) -> None:
        self.reader(reader).selectExpr("n", "n * 2000000000 AS b").createOrReplaceTempView(view)

    def zone_cells(self, reader: str) -> None:
        for direction, (made, read) in ZONES.items():
            tag = "/" + direction + "/" + reader
            for door, build in (("eb_hour", self.eb_hour), ("sql_view", self.sql_view)):
                self.put(
                    "Z/" + door + tag,
                    lambda b=build, m=made, x=read: self.view_cell(
                        lambda v: b(reader, v), m, x, VIEW_READ
                    ),
                )
            for door, build, shown in (("cj", self.cj, "ph, "), ("cross", self.cross, "")):
                self.put(
                    "E5/" + door + tag,
                    lambda b=build, m=made, x=read, c=shown: self.view_cell(
                        lambda v: b(reader, v), m, x, E5_READ.format(C=c)
                    ),
                )
            for door in ("direct", "cjdirect", "cache_after", "pathwrite"):
                self.put(
                    "H/" + door + tag, lambda d=door, m=made, x=read: self.held(d, reader, m, x)
                )
            self.put("SQ/differing" + tag, lambda m=made, x=read: self.subq(reader, m, x))
            self.put("SQ/matching" + tag, lambda m=made: self.subq(reader, m, m))

    def ansi_cell(self, reader: str, made: str, read: str) -> dict:
        view = self.name()
        self.conf(UTC, made)
        self.mul(reader, view)
        self.conf(UTC, read)
        return one.answer(self.s.sql("SELECT n, b FROM " + view))

    def ansi_cells(self, reader: str) -> None:
        for direction, (made, read) in ANSIS.items():
            self.put(
                "A/eb_mul/" + direction + "/" + reader,
                lambda m=made, x=read: self.ansi_cell(reader, m, x),
            )

    def held_frame(self, door: str, reader: str) -> Any:
        frame = self.reader(reader)
        if door == "cjdirect":
            other = self.side()
            return frame.join(other, frame.n == other.m).selectExpr(*SHAPE, "ph")
        if door == "pathwrite":
            return frame.selectExpr("n", "ts", "hour(ts) AS b")
        return frame.selectExpr(*SHAPE)

    def held(self, door: str, reader: str, made: str, read: str) -> dict:
        self.conf(made)
        frame = self.held_frame(door, reader)
        self.conf(read)
        if door == "cache_after":
            kept = frame.cache()
            kept.count()
            found = one.answer(kept)
            kept.unpersist()
            return found
        if door == "pathwrite":
            target = self.root + "/" + self.name("out")
            frame.write.mode("overwrite").parquet(target)
            back = self.s.read.parquet(target)
            return one.answer(back.selectExpr("n", "b", "CAST(unix_timestamp(ts) AS STRING) AS u"))
        return one.answer(frame)

    def subq(self, reader: str, made: str, read: str) -> dict:
        view, parquet = self.name(), self.name()
        self.conf(made)
        self.reader(reader).createOrReplaceTempView(view)
        self.reader("pq").createOrReplaceTempView(parquet)
        self.conf(read)
        return one.answer(self.s.sql(SUBQ.format(V=view, P=parquet)))

    def nested(self, reader: str) -> dict:
        inner, top = self.name("na"), self.name("nc")
        self.conf(LA)
        self.reader(reader).selectExpr("n", "ts", "hour(ts) AS h1").createOrReplaceTempView(inner)
        self.conf(UTC)
        over = self.s.table(inner).selectExpr("n", "ts", "h1", "hour(ts) AS h2")
        over.createOrReplaceTempView(top)
        self.conf(TK)
        read = self.s.table(top)
        return one.answer(read.selectExpr(*NESTED))

    def headerless(self) -> dict:
        frame = self.s.read.csv(self.root + "/csv0")
        found = one.answer(frame)
        found["columns"] = list(frame.columns)
        view = self.name()
        frame.createOrReplaceTempView(view)
        found["view"] = one.answer(self.s.sql("SELECT _c0, _c1 FROM " + view + " WHERE _c0 > '0'"))
        return found

    def same_cells(self) -> None:
        for reader in ("json_s", "csv_i"):
            first, second = self.reader(reader), self.reader(reader)
            checks = {
                "two_reads": lambda a=first, b=second: a.sameSemantics(b),
                "two_reads_hash": lambda a=first, b=second: a.semanticHash() == b.semanticHash(),
                "star": lambda a=first: a.sameSemantics(a.select("*")),
                "filters": lambda a=first, b=second: a.filter("n > 0").sameSemantics(
                    b.filter("n > 0")
                ),
                "other_filter": lambda a=first, b=second: a.filter("n > 0").sameSemantics(
                    b.filter("n > 1")
                ),
            }
            for check, run in checks.items():
                self.put("SS/" + reader + "/" + check, lambda r=run: {"value": bool(r())})

    def shaped(self, reader: str, session: Any = None) -> Any:
        return self.reader(reader, session).selectExpr(*SHAPE)

    def twice(self, reader: str, step: str) -> dict:
        self.conf(LA)
        frame = self.shaped(reader)
        frame.collect()
        self.conf(UTC)
        if step == "A3":
            return one.answer(frame.select("*"))
        if step == "A4":
            frame.collect()
            frame.select("*").collect()
        return one.answer(frame)

    def second_set(self, reader: str) -> dict:
        self.conf(LA)
        frame = self.shaped(reader)
        self.conf(UTC)
        frame.collect()
        self.conf(TK)
        return one.answer(frame)

    def sql_twice(self, reader: str) -> dict:
        view = self.name()
        self.conf(LA)
        self.reader(reader).createOrReplaceTempView(view)
        frame = self.s.sql(STICKY_SQL.format(V=view))
        frame.collect()
        self.conf(UTC)
        return one.answer(frame)

    def count_first(self, reader: str) -> dict:
        self.conf(LA)
        frame = self.shaped(reader)
        frame.count()
        self.conf(UTC)
        return one.answer(frame)

    def write_second(self, reader: str) -> dict:
        self.conf(LA)
        frame = self.shaped(reader)
        frame.collect()
        self.conf(UTC)
        target = self.root + "/" + self.name("out")
        frame.write.mode("overwrite").parquet(target)
        return one.answer(self.s.read.parquet(target))

    def other_session(self, reader: str, own: bool) -> dict:
        other = self.s.newSession()
        self.conf(LA)
        self.conf(LA, session=other)
        mine, theirs = self.shaped(reader), self.shaped(reader, other)
        self.conf(TK, session=other)
        mine.collect()
        self.conf(UTC)
        if own:
            return one.answer(mine)
        theirs.collect()
        self.conf(UTC, session=other)
        return one.answer(theirs)

    def first_action(self, action: str) -> dict:
        self.conf(LA)
        frame = self.shaped("json_s")
        FIRST_ACTIONS[action](frame)
        self.conf(UTC)
        return one.answer(frame)

    def checkpoint_second(self, action: str) -> dict:
        self.conf(LA)
        frame = self.shaped("json_s")
        frame.collect()
        self.conf(UTC)
        kept = frame.localCheckpoint() if action == "localCheckpoint" else frame.checkpoint()
        return one.answer(kept)

    def late_pull(self) -> dict:
        self.conf(LA)
        rows = self.shaped("json_s").toLocalIterator()
        self.conf(UTC)
        return {"rows": sorted("|".join(one.norm(x) for x in row) for row in rows)}

    def sticky_cells(self) -> None:
        setter = getattr(getattr(self.s, "sparkContext", None), "setCheckpointDir", None)
        if setter is not None:
            setter(self.root + "/ckpt")
        for reader in STICKY_READERS:
            for step in ("A2", "A3", "A4"):
                self.put(f"F1/{step}/{reader}", lambda r=reader, p=step: self.twice(r, p))
            self.put(f"F1/B2/{reader}", lambda r=reader: self.second_set(r))
            self.put(f"F1/E1/{reader}", lambda r=reader: self.sql_twice(r))
            self.put(f"F1/C1/{reader}", lambda r=reader: self.count_first(r))
            self.put(f"F1/D1/{reader}", lambda r=reader: self.write_second(r))
            for side, own in (("own", True), ("other", False)):
                self.put(
                    f"F1/newsess/{side}/{reader}", lambda r=reader, o=own: self.other_session(r, o)
                )
        for action in FIRST_ACTIONS:
            self.put(f"F1/X/{action}/json_s", lambda a=action: self.first_action(a))
        for action in ("localCheckpoint", "checkpoint"):
            self.put(f"F1/Y/{action}/json_s", lambda a=action: self.checkpoint_second(a))
        self.put("F1/late_pull/json_s", self.late_pull)

    def run(self) -> dict[str, dict]:
        self.fixtures()
        for reader in ("json_s", "csv_i"):
            self.zone_cells(reader)
        for reader in ("json_s", "csv_s"):
            self.ansi_cells(reader)
        for reader in ("json_s", "csv_i"):
            self.put("N/la_utc_tk/" + reader + "/ff", lambda r=reader: self.nested(r))
        self.put("G3/headerless_csv", self.headerless)
        self.same_cells()
        self.sticky_cells()
        return self.found
