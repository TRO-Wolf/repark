import gzip
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import condition, session

engine, parent, out, part = sys.argv[1:5]
s, wh = session(engine, parent)
OUT = Path(out)
OUT.write_text("")
LA, UTC, TK = "America/Los_Angeles", "UTC", "Asia/Tokyo"
ZL = {LA: "la", UTC: "utc", TK: "tk"}
SCHEMA = "n INT, ts TIMESTAMP"
ROWS = [(1, "2024-01-01 03:00:00"), (2, "2024-06-01 23:30:00")]


def put(name: str, text: str) -> Path:
    target = Path(wh) / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    return target


def conf(zone: str, ansi: str = "true") -> None:
    s.sql("SET spark.sql.session.timeZone=" + zone).collect()
    s.sql("SET spark.sql.ansi.enabled=" + ansi).collect()


def emit(record: dict[str, Any]) -> None:
    with OUT.open("a") as handle:
        handle.write(json.dumps(record) + "\n")


put("json/a.json", "".join(json.dumps({"n": n, "ts": ts}) + "\n" for n, ts in ROWS))
put("csvh/a.csv", "n,ts\n" + "".join(f"{n},{ts}\n" for n, ts in ROWS))
put("ml/a.json", json.dumps([{"n": n, "ts": ts} for n, ts in ROWS], indent=2))
Path(wh, "cg").mkdir(exist_ok=True)
with gzip.open(Path(wh, "cg", "a.csv.gz"), "wt") as handle:
    handle.write("n,ts\n" + "".join(f"{n},{ts}\n" for n, ts in ROWS))
put("c2/a.csv", "n,ts\n1,2024-01-01 03:00:00\n")
put("c2/b.csv", "n,ts\n1.5,2024-06-01 23:30:00\n")
conf(UTC)
s.sql(
    "SELECT CAST(n AS INT) AS n, CAST(ts AS TIMESTAMP) AS ts FROM VALUES "
    "(1, '2024-01-01 03:00:00'), (2, '2024-06-01 23:30:00') t(n, ts)"
).write.mode("overwrite").parquet(wh + "/pq")
for zone in (LA, UTC, TK):
    conf(zone)
    s.read.schema(SCHEMA).json(wh + "/json").write.mode("overwrite").parquet(
        wh + "/emu_" + ZL[zone]
    )
conf(UTC)

READERS: dict[str, Callable[[], Any]] = {
    "json_s": lambda: s.read.schema(SCHEMA).json(wh + "/json"),
    "csv_i": lambda: (
        s.read.option("header", "true").option("inferSchema", "true").csv(wh + "/csvh")
    ),
    "pq": lambda: s.read.parquet(wh + "/pq"),
    "json_ml": lambda: s.read.schema(SCHEMA).option("multiLine", "true").json(wh + "/ml"),
    "csv_gz": lambda: s.read.schema(SCHEMA).option("header", "true").csv(wh + "/cg"),
    "csv_gzi": lambda: (
        s.read.option("header", "true").option("inferSchema", "true").csv(wh + "/cg")
    ),
    "csv_2i": lambda: s.read.option("header", "true").option("inferSchema", "true").csv(wh + "/c2"),
}


def reader(name: str, made: str, read: str) -> Any:
    if name == "emu_r":
        return s.read.parquet(wh + "/emu_" + ZL[read])
    if name == "emu_c":
        return s.read.parquet(wh + "/emu_" + ZL[made])
    if name.startswith("emu_"):
        return s.read.parquet(wh + "/" + name)
    return READERS[name]()


def pq_side() -> Any:
    return s.read.parquet(wh + "/pq").selectExpr("n AS m", "hour(ts) AS ph")


def read_out(frame: Any) -> dict[str, Any]:
    cols = ["CAST(`" + name + "` AS STRING) AS `" + name + "`" for name in frame.columns]
    shaped = frame.selectExpr(*cols, "CAST(unix_timestamp(ts) AS STRING) AS u")
    rows = shaped.collect()
    return {
        "names": list(shaped.columns),
        "cols": sorted(
            ([None if value is None else str(value) for value in row] for row in rows),
            key=lambda row: [str(value) for value in row],
        ),
    }


class Cell:
    def __init__(self, tag: str, name: str, door: str, index: int) -> None:
        self.tag, self.name, self.door, self.index = tag, name, door, index
        self.view = "rv" + str(index)
        self.frame: Any = None
        self.error: str | None = None


def build(cell: Cell, made: str, read: str) -> None:
    fr = reader(cell.name, made, read)
    v, k = cell.view, str(cell.index) + " AS k"
    door = cell.door
    if door == "d":
        fr.createOrReplaceTempView(v)
    elif door == "eb_hour":
        fr.selectExpr("n", "ts", "hour(ts) AS b").createOrReplaceTempView(v)
    elif door == "sb_hour":
        fr.createOrReplaceTempView(v + "d")
        s.sql("CREATE OR REPLACE TEMP VIEW " + v + " AS SELECT *, hour(ts) AS b FROM " + v + "d")
    elif door == "ffilter":
        fr.filter("hour(ts) >= 12").createOrReplaceTempView(v)
    elif door == "union_hour":
        other = s.read.parquet(wh + "/pq").selectExpr("n + 10 AS n", "ts", "hour(ts) AS b")
        fr.selectExpr("n", "ts", "hour(ts) AS b").unionByName(other).createOrReplaceTempView(v)
    elif door == "twice_hour":
        cell.frame = fr.selectExpr("n", "ts", "hour(ts) AS b")
        cell.frame.createOrReplaceTempView(v)
    elif door == "cj":
        side = pq_side()
        fr.join(side, fr.n == side.m).createOrReplaceTempView(v)
    elif door == "cjb":
        side = pq_side()
        joined = fr.join(side, fr.n == side.m)
        joined.selectExpr("n", "ts", "ph", "hour(ts) AS b").createOrReplaceTempView(v)
    elif door == "cross":
        crossed = fr.crossJoin(pq_side().limit(1))
        crossed.selectExpr("n", "ts", "hour(ts) AS b").createOrReplaceTempView(v)
    elif door == "sql_join":
        fr.createOrReplaceTempView(v + "r")
        pq_side().createOrReplaceTempView(v + "p")
        s.sql(
            "CREATE OR REPLACE TEMP VIEW " + v + " AS SELECT r.n, r.ts, p.ph, hour(r.ts) AS b "
            "FROM " + v + "r r JOIN " + v + "p p ON r.n = p.m"
        )
    elif door == "cache_mat_hour":
        cached = fr.selectExpr("n", "ts", "hour(ts) AS b", k).cache()
        cached.count()
        cached.createOrReplaceTempView(v)
    elif door == "cache_lazy_hour":
        fr.selectExpr("n", "ts", "hour(ts) AS b", k).cache().createOrReplaceTempView(v)
    elif door == "cachetable_hour":
        fr.selectExpr("n", "ts", "hour(ts) AS b", k).createOrReplaceTempView(v)
        s.sql("CACHE TABLE " + v).collect()
    elif door == "nv":
        cell.frame = fr
    elif door == "nv_hour":
        cell.frame = fr.selectExpr("n", "ts", "hour(ts) AS b")
    else:
        raise ValueError(door)


def finish(cell: Cell) -> None:
    record: dict[str, Any] = {"tag": cell.tag}
    if cell.error is not None:
        record["create_error"] = cell.error
        emit(record)
        return
    try:
        if cell.door == "twice_hour":
            cell.frame.createOrReplaceTempView(cell.view)
        frame = cell.frame if cell.door in ("nv", "nv_hour") else s.table(cell.view)
        record.update(read_out(frame))
    except Exception as error:
        record["error"] = condition(error)
    emit(record)


X_READERS = ["json_s", "csv_i", "pq", "emu_r", "emu_c"]
X_DOORS = ["ffilter", "union_hour", "twice_hour", "cj", "cjb", "cross", "sql_join"]
X_DOORS += ["nv", "nv_hour"]
XC_DOORS = ["cache_mat_hour", "cache_lazy_hour", "cachetable_hour"]
D_CELLS = [
    ("json_ml", "d"),
    ("json_ml", "eb_hour"),
    ("json_ml", "sb_hour"),
    ("csv_gz", "d"),
    ("csv_gz", "eb_hour"),
    ("csv_gzi", "d"),
    ("csv_gzi", "eb_hour"),
    ("csv_2i", "d"),
    ("csv_2i", "eb_hour"),
]
DIRECTIONS = [("la_utc", LA, UTC), ("utc_la", UTC, LA)]


def two_zone(group: str, pairs: list[tuple[str, str]]) -> None:
    index = 0
    for direction, made, read in DIRECTIONS:
        cells = []
        conf(made)
        for name, door in pairs:
            index += 1
            cell = Cell(group + "/" + direction + "/" + name + "/" + door, name, door, index)
            try:
                build(cell, made, read)
            except Exception as error:
                cell.error = condition(error)
            cells.append(cell)
        conf(read)
        for cell in cells:
            finish(cell)


ORDERS = [(LA, UTC, TK), (UTC, LA, TK), (TK, LA, UTC), (LA, TK, UTC), (LA, UTC, LA), (UTC, LA, UTC)]


def nested() -> None:
    index = 0
    for a, b, c in ORDERS:
        order = ZL[a] + "_" + ZL[b] + "_" + ZL[c]
        for name in ["json_s", "csv_i", "pq", "emu_" + ZL[c], "emu_" + ZL[b]]:
            label = {"emu_" + ZL[c]: "emu_live", "emu_" + ZL[b]: "emu_mid"}.get(name, name)
            for shape in ["ff", "fs", "sf"]:
                index += 1
                tag = "N/" + order + "/" + label + "/" + shape
                record: dict[str, Any] = {"tag": tag}
                inner, mid, top = "na" + str(index), "nb" + str(index), "nc" + str(index)
                try:
                    conf(a)
                    reader(name, a, c).selectExpr(
                        "n", "ts", "hour(ts) AS h1"
                    ).createOrReplaceTempView(inner)
                    conf(b)
                    body = "SELECT n, ts, h1, hour(ts) AS h2 FROM " + inner
                    if shape == "ff":
                        s.table(inner).selectExpr(
                            "n", "ts", "h1", "hour(ts) AS h2"
                        ).createOrReplaceTempView(top)
                    elif shape == "fs":
                        s.sql("CREATE OR REPLACE TEMP VIEW " + top + " AS " + body)
                    else:
                        s.sql("CREATE OR REPLACE TEMP VIEW " + mid + " AS " + body)
                        s.table(mid).createOrReplaceTempView(top)
                    conf(c)
                    frame = s.table(top).selectExpr("*", "hour(ts) AS h3")
                    record.update(read_out(frame))
                except Exception as error:
                    record["error"] = condition(error)
                emit(record)
    conf(UTC)


def same_checks(first: Any, second: Any) -> dict[str, Callable[[], Any]]:
    return {
        "two_reads": lambda: first.sameSemantics(second),
        "two_reads_hash": lambda: first.semanticHash() == second.semanticHash(),
        "star": lambda: first.sameSemantics(first.select("*")),
        "filters": lambda: first.filter("n > 0").sameSemantics(second.filter("n > 0")),
        "other_filter": lambda: first.filter("n > 0").sameSemantics(second.filter("n > 1")),
    }


def explain_and_semantics() -> None:
    conf(LA)
    for name in ["json_s", "csv_i", "pq"]:
        frame = READERS[name]()
        frame.createOrReplaceTempView("ev_" + name)
        for spelling in ["EXPLAIN", "EXPLAIN EXTENDED", "EXPLAIN VERBOSE"]:
            tag = "E/" + name + "/" + spelling.replace(" ", "_").lower()
            try:
                rows = s.sql(spelling + " SELECT * FROM ev_" + name + " WHERE n > 0").collect()
                text = "\n".join(" | ".join(str(value) for value in row) for row in rows)
                emit({"tag": tag, "accepted": True, "lines": text.count("\n") + 1, "plan": text})
            except Exception as error:
                emit({"tag": tag, "accepted": False, "error": condition(error)})
        for label, check in same_checks(READERS[name](), READERS[name]()).items():
            try:
                emit({"tag": "SS/" + name + "/" + label, "value": bool(check())})
            except Exception as error:
                emit({"tag": "SS/" + name + "/" + label, "error": condition(error)})
    conf(UTC)


if part == "X":
    two_zone("X", [(name, door) for name in X_READERS for door in X_DOORS])
elif part == "XC":
    two_zone("XC", [(name, door) for name in X_READERS for door in XC_DOORS])
elif part == "D":
    two_zone("D", D_CELLS)
elif part == "N":
    nested()
elif part == "E":
    explain_and_semantics()
s.stop()
