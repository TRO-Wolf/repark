import datetime
import os
import sys

import json

from common import COND, Rec, session

engine, whp, out, fix = sys.argv[1:5]
pg = os.environ.get("PG_URL")
s, wh = session(engine, whp)
r = Rec(s, out)
LA = "America/Los_Angeles"
ROWS = "(1, '2024-01-01 03:00:00'), (2, '2024-01-01 12:00:00')"


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as handle:
        handle.write(text)


write(wh + "/json/a.json", '{"n": 1, "ts": "2024-01-01 03:00:00"}\n{"n": 2, "ts": "2024-01-01 12:00:00"}\n')
write(wh + "/csv/a.csv", "1,2024-01-01 03:00:00\n2,2024-01-01 12:00:00\n")
write(wh + "/csvh/a.csv", "n,ts\n1,2024-01-01 03:00:00\n2,2024-01-01 12:00:00\n")
write(wh + "/csvn/a.csv", "n,ts\n1,2024-01-01 03:00:00\nNA,2024-01-01 12:00:00\n3,NA\n")
write(wh + "/text/a.txt", "2024-01-01 03:00:00\n2024-01-01 12:00:00\n")
write(wh + "/textp/p=2024-01-01 03%3A00%3A00/a.txt", "x\n")
write(wh + "/x.xlsx", "")
s.sql(
    "SELECT CAST(n AS INT) AS n, CAST(ts AS TIMESTAMP) AS ts FROM VALUES " + ROWS + " t(n, ts)"
).write.mode("overwrite").parquet(wh + "/pq")
snaps = []
try:
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    s.sql("CREATE TABLE lc.ns.t (n INT, ts TIMESTAMP) USING iceberg").collect()
    for row in ("(1, TIMESTAMP '2024-01-01 03:00:00')", "(2, TIMESTAMP '2024-01-01 12:00:00')"):
        s.sql("INSERT INTO lc.ns.t VALUES " + row).collect()
    got = s.sql("SELECT snapshot_id FROM lc.ns.t.snapshots ORDER BY committed_at").collect()
    snaps = [str(row[0]) for row in got]
except Exception as error:
    r.out.write('{"tag": "setup/iceberg", "error": %r}\n' % " ".join(str(error).split())[:140])
travel = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)).strftime(
    "%Y-%m-%d %H:%M:%S"
)

FRAMES = {
    "json_s": lambda: s.read.schema("n INT, ts TIMESTAMP").json(wh + "/json"),
    "json_i": lambda: s.read.json(wh + "/json"),
    "csv_s": lambda: s.read.schema("n INT, ts TIMESTAMP").csv(wh + "/csv"),
    "csv_i": lambda: s.read.option("header", "true").option("inferSchema", "true").csv(wh + "/csvh"),
    "csv_n": lambda: s.read.option("header", "true")
    .option("nullValue", "NA")
    .option("inferSchema", "true")
    .csv(wh + "/csvn"),
    "csv_ns": lambda: s.read.option("header", "true").option("nullValue", "NA").csv(wh + "/csvn"),
    "text_v": lambda: s.read.text(wh + "/text").selectExpr("length(value) AS n", "value AS ts"),
    "text_p": lambda: s.read.text(wh + "/textp").toDF("n", "ts"),
    "orc": lambda: s.read.orc(fix + "/orc"),
    "orc_s": lambda: s.read.schema("n INT, ts TIMESTAMP").orc(fix + "/orc"),
    "ice_v": lambda: s.read.option("versionAsOf", snaps[0]).table("lc.ns.t"),
    "ice_t": lambda: s.read.option("timestampAsOf", travel).table("lc.ns.t"),
    "ice_i": lambda: s.read.format("iceberg")
    .option("start-snapshot-id", snaps[0])
    .option("end-snapshot-id", snaps[1])
    .load("lc.ns.t"),
    "cdf": lambda: s.createDataFrame(
        [(1, datetime.datetime(2024, 1, 1, 3)), (2, datetime.datetime(2024, 1, 1, 12))],
        "n INT, ts TIMESTAMP",
    ),
    "pq": lambda: s.read.parquet(wh + "/pq"),
    "mixed": lambda: s.read.schema("n INT, ts TIMESTAMP")
    .json(wh + "/json")
    .join(s.table(PVIEW), "n"),
}
if engine != "spark":
    FRAMES["excel"] = lambda: s.read.excel(wh + "/x.xlsx")
    if pg:
        FRAMES["pg"] = lambda: s.read.format("postgres").option("url", pg).option("dbtable", "public.t").load().select("n", "ts")
        FRAMES["pg_tz"] = lambda: s.read.format("postgres").option("url", pg).option("dbtable", "public.t").load().selectExpr("n", "tz AS ts")
FILTER = {"text_p": "n IS NOT NULL"}
EXPR = {"text_p": "concat(n, '1') AS n1"}
PASSES = [("true", LA, "UTC"), ("true", "UTC", LA), ("false", LA, "UTC"), ("false", "UTC", LA)]


def fail(tag, error):
    text = " ".join(str(error).split())
    match = COND.search(text)
    r.out.write(json.dumps({"tag": tag, "error": match.group(1) if match else text[:140]}) + "\n")


def views(tag, key, name, frame):
    made = {}
    keep = ["ts"] if key != "mixed" else ["ts", "ph"]
    extra = [] if key != "mixed" else ["ph"]
    flt = FILTER.get(key, "n > 0")
    steps = [
        ("d", ["n"] + extra, lambda: frame),
        ("f", ["n"] + extra, lambda: frame.filter(flt)),
        ("e", ["n1"] + extra, lambda: frame.selectExpr(EXPR.get(key, "n + 1 AS n1"), *keep)),
        ("h", ["n"] + extra + ["h"], lambda: frame.selectExpr("n", *keep, "hour(ts) AS h")),
        ("o", ["n"] + extra, lambda: s.table(name + "_d").filter(flt)),
        ("oh", ["n", "h2"], lambda: s.table(name + "_d").selectExpr("n", "ts", "hour(ts) AS h2")),
    ]
    for kind, cols, build in steps:
        try:
            build().createOrReplaceTempView(name + "_" + kind)
            made[kind] = cols
        except Exception as error:
            fail(tag + "/" + key + "/create_" + kind, error)
    for kind in list(made):
        cols = made[kind]
        for prefix, body, more in (("sa_", "SELECT * FROM ", []), ("sh_", "SELECT *, hour(ts) AS hs FROM ", ["hs"])):
            try:
                s.sql("CREATE OR REPLACE TEMP VIEW " + name + "_" + prefix + kind + " AS " + body + name + "_" + kind).collect()
                made[prefix + kind] = cols + more
            except Exception as error:
                fail(tag + "/" + key + "/create_" + prefix + kind, error)
    if "sa_d" in made:
        try:
            s.table(name + "_sa_d").filter(flt).createOrReplaceTempView(name + "_osa")
            made["osa"] = ["n"] + extra
        except Exception as error:
            fail(tag + "/" + key + "/create_osa", error)
    return made


for index, (ansi, made_zone, read_zone) in enumerate(PASSES):
    tag = "ansi_" + ansi + "/" + ("la_utc" if made_zone == LA else "utc_la")
    r.conf(made_zone, ansi)
    PVIEW = "pv_" + str(index)
    s.read.parquet(wh + "/pq").selectExpr("n", "hour(ts) AS ph").createOrReplaceTempView(PVIEW)
    built = {}
    for key, factory in FRAMES.items():
        name = key + "_" + str(index)
        try:
            built[key] = (name, views(tag, key, name, factory()))
        except Exception as error:
            fail(tag + "/" + key + "/create", error)
    r.conf(read_zone, ansi)
    for key, (name, made) in built.items():
        for kind, cols in made.items():
            r.sql(
                tag + "/" + key + "/" + kind,
                "SELECT " + ", ".join(cols) + ", unix_timestamp(ts) AS u, CAST(ts AS STRING) AS t FROM " + name + "_" + kind,
            )
r.conf("UTC", "true")
s.stop()
