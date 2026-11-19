import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from common import Rec, session

engine, parent, out, part = sys.argv[1:5]
DATA = sys.argv[5] if len(sys.argv) > 5 else "base"
s, wh = session(engine, parent)
r = Rec(s, out)
LA = "America/Los_Angeles"
UTC = "UTC"
ZL = {LA: "la", UTC: "utc"}
SCHEMA = "n INT, ts TIMESTAMP"
LATE = "2024-06-01 23:30:00" if DATA == "midnight" else "2024-01-01 12:00:00"
GOOD_JSON = '{"n": 1, "ts": "2024-01-01 03:00:00"}\n{"n": 2, "ts": "' + LATE + '"}\n'
BAD_JSON = '{"n": 1, "ts": "2024-01-01 03:00:00"}\n{"n": 2147483648, "ts": "not a time"}\n'
GOOD_ROWS = "1,2024-01-01 03:00:00\n2," + LATE + "\n"
BAD_ROWS = "1,2024-01-01 03:00:00\n2147483648,not a time\n"
BODIES = {
    "hour": "hour(ts)",
    "str": "CAST(ts AS STRING)",
    "date": "CAST(ts AS DATE)",
    "fmt": "date_format(ts, 'yyyy-MM-dd HH:mm')",
    "tz": "current_timezone()",
}


def put(name: str, text: str) -> None:
    target = Path(wh) / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)


put("json/a.json", GOOD_JSON)
put("jsond/a.json", GOOD_JSON.splitlines()[0] + "\n")
put("jsond/b.json", GOOD_JSON.splitlines()[1] + "\n")
put("jsonbad/a.json", BAD_JSON)
put("csv/a.csv", GOOD_ROWS)
put("csvh/a.csv", "n,ts\n" + GOOD_ROWS)
put("csvn/a.csv", "n,ts\n1,2024-01-01 03:00:00\nNA," + LATE + "\n3,NA\n")
put("csvbad/a.csv", BAD_ROWS)
put("jsonts/a.json", '{"n": 1, "ts": "2024-01-01 03:00:00"}\n{"n": 2, "ts": "not a time"}\n')
put(
    "jsonov/a.json",
    '{"n": 1, "ts": "2024-01-01 03:00:00"}\n{"n": 2147483648, "ts": "2024-01-01 12:00:00"}\n',
)
put("csvts/a.csv", "1,2024-01-01 03:00:00\n2,not a time\n")
put("csvov/a.csv", "1,2024-01-01 03:00:00\n2147483648,2024-01-01 12:00:00\n")
put("csvbadh/a.csv", "n,ts\n" + BAD_ROWS)
put("smart/a.csv", "n,ts\n" + GOOD_ROWS)
put("smartbad/a.csv", "n,ts\n" + BAD_ROWS)
r.conf(UTC, "true")
s.sql(
    "SELECT CAST(n AS INT) AS n, CAST(ts AS TIMESTAMP) AS ts FROM VALUES "
    "(1, '2024-01-01 03:00:00'), (2, '" + LATE + "') t(n, ts)"
).write.mode("overwrite").parquet(wh + "/pq")
for zone in (LA, UTC):
    r.conf(zone, "true")
    s.read.schema(SCHEMA).json(wh + "/json").write.mode("overwrite").parquet(
        wh + "/emu_" + ZL[zone]
    )
r.conf(UTC, "true")
read_zone = {"value": UTC}


def reader() -> Any:
    return s.read


READERS: dict[str, Callable[[], Any]] = {
    "json_s": lambda: reader().schema(SCHEMA).json(wh + "/json"),
    "json_f": lambda: reader().format("json").schema(SCHEMA).load(wh + "/json"),
    "json_m": lambda: reader().schema(SCHEMA).json(wh + "/jsond"),
    "json_i": lambda: reader().json(wh + "/json"),
    "json_tz": lambda: reader().schema(SCHEMA).option("timeZone", UTC).json(wh + "/json"),
    "csv_s": lambda: reader().schema(SCHEMA).csv(wh + "/csv"),
    "csv_f": lambda: reader().format("csv").schema(SCHEMA).load(wh + "/csv"),
    "csv_i": lambda: (
        reader().option("header", "true").option("inferSchema", "true").csv(wh + "/csvh")
    ),
    "csv_n": lambda: (
        reader()
        .option("header", "true")
        .option("nullValue", "NA")
        .option("inferSchema", "true")
        .csv(wh + "/csvn")
    ),
    "csv_ns": lambda: reader().option("header", "true").option("nullValue", "NA").csv(wh + "/csvn"),
    "csv_0": lambda: reader().option("header", "true").csv(wh + "/csvh"),
    "csv_tf": lambda: (
        reader().schema(SCHEMA).option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(wh + "/csv")
    ),
    "smart": lambda: reader().option("header", "true").smartCsv(wh + "/smart/a.csv"),
    "pq": lambda: reader().parquet(wh + "/pq"),
    "emu": lambda: reader().parquet(wh + "/emu_" + ZL[read_zone["value"]]),
}
BAD_READERS: dict[str, Callable[[], Any]] = {
    "json_s": lambda: reader().schema(SCHEMA).json(wh + "/jsonbad"),
    "json_f": lambda: reader().format("json").schema(SCHEMA).load(wh + "/jsonbad"),
    "json_i": lambda: reader().json(wh + "/jsonbad"),
    "csv_s": lambda: reader().schema(SCHEMA).csv(wh + "/csvbad"),
    "json_sts": lambda: reader().schema(SCHEMA).json(wh + "/jsonts"),
    "json_sov": lambda: reader().schema(SCHEMA).json(wh + "/jsonov"),
    "csv_sts": lambda: reader().schema(SCHEMA).csv(wh + "/csvts"),
    "csv_sov": lambda: reader().schema(SCHEMA).csv(wh + "/csvov"),
    "json_g": lambda: reader().schema(SCHEMA).json(wh + "/json"),
    "csv_g": lambda: (
        reader().option("header", "true").option("inferSchema", "true").csv(wh + "/csvh")
    ),
    "pq": lambda: reader().parquet(wh + "/pq"),
    "csv_i": lambda: (
        reader().option("header", "true").option("inferSchema", "true").csv(wh + "/csvbadh")
    ),
    "smart": lambda: reader().option("header", "true").smartCsv(wh + "/smartbad/a.csv"),
}
if engine == "spark":
    READERS.pop("smart")
    BAD_READERS.pop("smart")
else:
    READERS["csv_fix"] = lambda: s.read_csv(
        wh + "/csv", {"header": "false", "utf8_columns": "column_2"}
    ).selectExpr("CAST(column_1 AS INT) AS n", "CAST(column_2 AS TIMESTAMP) AS ts")


def register(tag: str, name: str, build: Callable[[], Any]) -> bool:
    try:
        build().createOrReplaceTempView(name)
    except Exception as error:
        r.fail(tag + "/create", error)
        return False
    return True


def sql_view(tag: str, name: str, body: str) -> bool:
    try:
        s.sql("CREATE OR REPLACE TEMP VIEW " + name + " AS " + body).collect()
    except Exception as error:
        r.fail(tag + "/create", error)
        return False
    return True


def doors(
    tag: str, key: str, frame: Any, pview: str, bodies: dict[str, str]
) -> dict[str, list[str]]:
    base = "v_" + key
    made: dict[str, list[str]] = {}
    plain = [
        ("d", ["n"], lambda: frame),
        ("f", ["n"], lambda: frame.filter("n > 0")),
        ("e", ["n", "n1"], lambda: frame.selectExpr("n", "n + 1 AS n1", "ts")),
        ("j", ["n", "ph"], lambda: frame.join(s.table(pview), "n")),
    ]
    for kind, cols, build in plain:
        if register(tag + "/" + kind, base + "_" + kind, build):
            made[kind] = cols
    if "d" in made:
        if register(tag + "/o", base + "_o", lambda: s.table(base + "_d").filter("n > 0")):
            made["o"] = ["n"]
        if sql_view(tag + "/sa", base + "_sa", "SELECT * FROM " + base + "_d"):
            made["sa"] = ["n"]
            if register(tag + "/osa", base + "_osa", lambda: s.table(base + "_sa").filter("n > 0")):
                made["osa"] = ["n"]
    for label, body in bodies.items():
        expr = body + " AS b"
        rich = [
            ("eb", lambda expr=expr: frame.selectExpr("n", "ts", expr)),
            ("jb", lambda expr=expr: frame.join(s.table(pview), "n").selectExpr("n", "ts", expr)),
        ]
        if "d" in made:
            rich.append(("ob", lambda expr=expr: s.table(base + "_d").selectExpr("n", "ts", expr)))
        if "sa" in made:
            rich.append(
                ("osb", lambda expr=expr: s.table(base + "_sa").selectExpr("n", "ts", expr))
            )
        for kind, build in rich:
            if register(tag + "/" + kind + "_" + label, base + "_" + kind + "_" + label, build):
                made[kind + "_" + label] = ["n", "b"]
        if "d" in made and sql_view(
            tag + "/sb_" + label,
            base + "_sb_" + label,
            "SELECT *, " + body + " AS b FROM " + base + "_d",
        ):
            made["sb_" + label] = ["n", "b"]
    return made


def build_all(
    tag: str, readers: dict[str, Callable[[], Any]], bodies: dict[str, str]
) -> dict[str, Any]:
    pview = "pv_" + str(abs(hash(tag)) % 100000)
    register(
        tag + "/pview", pview, lambda: s.read.parquet(wh + "/pq").selectExpr("n", "hour(ts) AS ph")
    )
    built = {}
    for key, factory in readers.items():
        try:
            frame = factory()
        except Exception as error:
            r.fail(tag + "/" + key, error)
            continue
        built[key] = doors(tag + "/" + key, key, frame, pview, bodies)
    return built


def read_all(tag: str, built: dict[str, Any]) -> None:
    for key, made in built.items():
        for kind, cols in made.items():
            r.sql(
                tag + "/" + key + "/" + kind,
                "SELECT "
                + ", ".join(cols)
                + ", unix_timestamp(ts) AS u FROM v_"
                + key
                + "_"
                + kind,
            )


def zone_pass(index: int) -> None:
    ansi, made, read = [
        ("true", LA, UTC),
        ("true", UTC, LA),
        ("false", LA, UTC),
        ("false", UTC, LA),
        ("true", LA, LA),
        ("true", UTC, UTC),
        ("false", LA, LA),
        ("false", UTC, UTC),
    ][index]
    tag = "Z/ansi_" + ansi + "/" + ZL[made] + "_" + ZL[read]
    read_zone["value"] = read
    r.conf(made, ansi)
    built = build_all(tag, READERS, BODIES)
    r.conf(read, ansi)
    read_all(tag, built)


def ansi_pass() -> None:
    for made, read in (("true", "false"), ("false", "true"), ("true", "true"), ("false", "false")):
        tag = "A/" + made + "_" + read
        r.conf(UTC, made)
        built = build_all(tag, BAD_READERS, {"hour": "hour(ts)", "mul": "n * 2000000000"})
        r.conf(UTC, read)
        read_all(tag, built)
    r.conf(UTC, "true")


def ttype_pass() -> None:
    picked = {
        key: READERS[key]
        for key in ("json_s", "json_i", "csv_s", "csv_i", "smart", "pq")
        if key in READERS
    }
    ltz, ntz = "TIMESTAMP_LTZ", "TIMESTAMP_NTZ"
    for made, read in ((ltz, ntz), (ntz, ltz), (ltz, ltz), (ntz, ntz)):
        tag = "T/" + made[-3:] + "_" + read[-3:]
        r.conf(LA, "true", made)
        built = build_all(tag, picked, {"hour": "hour(ts)", "str": "CAST(ts AS STRING)"})
        r.conf(LA, "true", read)
        read_all(tag, built)
    r.conf(UTC, "true", ltz)


WRITES: dict[str, Callable[[str, str], None]] = {
    "ctas": lambda view, table: s.sql(
        "CREATE TABLE " + table + " USING iceberg AS SELECT * FROM " + view
    ).collect(),
    "ins": lambda view, table: s.sql("INSERT INTO " + table + " SELECT * FROM " + view).collect(),
    "wto": lambda view, table: s.table(view).writeTo(table).using("iceberg").create(),
    "into": lambda view, table: s.table(view).write.insertInto(table),
}


def write_pass(index: int) -> None:
    ansi, made, read = [
        ("true", LA, UTC),
        ("true", UTC, LA),
        ("false", LA, UTC),
        ("false", UTC, LA),
    ][index]
    tag = "W/ansi_" + ansi + "/" + ZL[made] + "_" + ZL[read]
    read_zone["value"] = read
    picked = {
        key: READERS[key]
        for key in ("json_s", "json_i", "csv_s", "csv_i", "smart", "pq", "emu")
        if key in READERS
    }
    r.conf(made, ansi)
    built = build_all(tag, picked, {"hour": "hour(ts)"})
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.w" + str(index)).collect()
    r.conf(read, ansi)
    for key, made_views in built.items():
        for source in ("d", "eb_hour", "sb_hour"):
            if source not in made_views:
                continue
            view = "v_" + key + "_" + source
            cols = ", ".join(made_views[source])
            for write, action in WRITES.items():
                table = "lc.w" + str(index) + "." + key + "_" + source + "_" + write
                cell = tag + "/" + key + "/" + source + "/" + write
                try:
                    if write in ("ins", "into"):
                        s.sql(
                            "CREATE TABLE "
                            + table
                            + " USING iceberg AS SELECT * FROM "
                            + view
                            + " WHERE false"
                        ).collect()
                    action(view, table)
                except Exception as error:
                    r.emit({"tag": cell, "error": "WRITE " + str(error).split("\n")[0][:140]})
                    continue
                r.conf(UTC, "true")
                r.sql(cell, "SELECT " + cols + ", unix_timestamp(ts) AS u FROM " + table)
                r.conf(read, ansi)


def explain_pass() -> None:
    for key in ("json_s", "csv_s", "csv_i", "pq"):
        r.conf(LA, "true")
        frame = READERS[key]()
        frame.createOrReplaceTempView("x_" + key)
        frame.filter("n > 0").selectExpr("n", "ts", "hour(ts) AS b").createOrReplaceTempView(
            "xh_" + key
        )
        for zone in (LA, UTC):
            r.conf(zone, "true")
            for view in ("x_" + key, "xh_" + key):
                try:
                    plan = s.sql("EXPLAIN SELECT * FROM " + view + " WHERE n > 0").collect()
                    text = "\n".join(" | ".join(str(cell) for cell in row) for row in plan)
                except Exception as error:
                    text = "ERROR " + str(error)[:200]
                r.emit({"tag": "E/" + ZL[zone] + "/" + view, "plan": text})
    r.conf(UTC, "true")


def ddl(tag: str, statement: str) -> bool:
    try:
        s.sql(statement).collect()
    except Exception as error:
        r.fail(tag + "/create", error)
        return False
    return True


def sql_source_pass() -> None:
    statements = [
        (
            "using_json",
            "s_uj",
            "CREATE OR REPLACE TEMPORARY VIEW s_uj USING json OPTIONS (path '" + wh + "/json')",
        ),
        (
            "using_csv",
            "s_uc",
            "CREATE OR REPLACE TEMPORARY VIEW s_uc (n INT, ts TIMESTAMP) USING csv OPTIONS (path '"
            + wh
            + "/csv')",
        ),
        (
            "sql_over_csv",
            "s_sc",
            "CREATE OR REPLACE TEMP VIEW s_sc AS SELECT *, hour(ts) AS b FROM s_uc",
        ),
    ]
    for ansi in ("true", "false"):
        tag = "S/ansi_" + ansi + "/la_utc"
        r.conf(LA, ansi)
        made = [
            (label, name)
            for label, name, statement in statements
            if ddl(tag + "/" + label, statement)
        ]
        r.conf(UTC, ansi)
        r.sql(tag + "/path_json/d", "SELECT n, unix_timestamp(ts) AS u FROM json.`" + wh + "/json`")
        for label, name in made:
            r.sql(tag + "/" + label + "/d", "SELECT * FROM " + name)
    r.conf(UTC, "true")


if part.startswith("Z"):
    zone_pass(int(part[1:]))
elif part.startswith("W"):
    write_pass(int(part[1:]))
elif part == "A":
    ansi_pass()
elif part == "T":
    ttype_pass()
elif part == "S":
    sql_source_pass()
elif part == "E":
    explain_pass()
s.stop()
