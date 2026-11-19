import sys
from pathlib import Path
from typing import Any

from common import Rec, session

engine, parent, out = sys.argv[1:4]
s, wh = session(engine, parent)
r = Rec(s, out)
LA = "America/Los_Angeles"
TK = "Asia/Tokyo"
SCHEMA = "n INT, ts TIMESTAMP"
FILES = {
    "good": "1,2024-01-01 03:00:00\n2,2024-06-01 23:30:00\n",
    "date": "1,2024-01-01\n2,2024-06-01\n",
    "epoch": "1,1704078000\n2,1704110400\n",
    "frac": "1,2024-01-01 03:00:00.123456\n2,2024-01-01 12:00:00.5\n",
    "iso": "1,2024-01-01T03:00:00\n2,2024-06-01T23:30:00\n",
    "hdr": "n,ts\n1,2024-01-01 03:00:00\n2,2024-06-01 23:30:00\n",
    "hdrpos": "a,b\n1,2024-01-01 03:00:00\n2,2024-06-01 23:30:00\n",
    "hdrrev": "ts,n\n2024-01-01 03:00:00,1\n2024-06-01 23:30:00,2\n",
    "dst": "1,2024-03-10 02:30:00\n2,2024-11-03 01:30:00\n",
    "mixed": "1,007,2024-01-01 03:00:00,1.005,TRUE,1.50\n2,010,2024-06-01 23:30:00,2.5,false,3\n",
    "mixednots": "1,007,1.005,TRUE,1.50\n2,010,2.5,false,3\n",
    "twots": "1,2024-01-01 03:00:00,2024-06-01 23:30:00\n",
    "emptyts": "1,\n2,2024-01-01 03:00:00\n",
    "nullv": "1,2024-01-01 03:00:00\n2,NA\n",
    "nots": "1,007\n2,010\n",
}
for key, text in FILES.items():
    target = Path(wh, key)
    target.mkdir(parents=True, exist_ok=True)
    (target / "a.csv").write_text(text)
Path(wh, "dir").mkdir(parents=True, exist_ok=True)
Path(wh, "dir", "a.csv").write_text("1,2024-01-01 03:00:00\n")
Path(wh, "dir", "b.csv").write_text("2,2024-06-01 23:30:00\n")
U = "SELECT n, unix_timestamp(ts) AS u FROM xv"
CELLS: list[tuple[str, Any, str]] = [
    ("ntz", lambda: s.read.schema("n INT, ts TIMESTAMP_NTZ").csv(wh + "/good"),
     "SELECT n, CAST(ts AS STRING) AS t, hour(ts) AS h FROM xv"),
    ("date", lambda: s.read.schema(SCHEMA).csv(wh + "/date"), U),
    ("epoch", lambda: s.read.schema(SCHEMA).csv(wh + "/epoch"), U),
    ("frac", lambda: s.read.schema(SCHEMA).csv(wh + "/frac"),
     "SELECT n, unix_timestamp(ts) AS u, CAST(ts AS STRING) AS f FROM xv"),
    ("iso", lambda: s.read.schema(SCHEMA).csv(wh + "/iso"), U),
    ("hdr", lambda: s.read.option("header", "true").schema(SCHEMA).csv(wh + "/hdr"), U),
    ("hdrpos", lambda: s.read.option("header", "true").schema(SCHEMA).csv(wh + "/hdrpos"), U),
    ("hdrrev", lambda: s.read.option("header", "true").schema(SCHEMA).csv(wh + "/hdrrev"), U),
    ("dst", lambda: s.read.schema(SCHEMA).csv(wh + "/dst"), U),
    ("mixed", lambda: s.read.schema(
        "n INT, s STRING, ts TIMESTAMP, d DECIMAL(10,2), b BOOLEAN, x DOUBLE"
    ).csv(wh + "/mixed"),
     "SELECT n, s, unix_timestamp(ts) AS u, CAST(d AS STRING) AS d, b, x FROM xv"),
    ("mixednots", lambda: s.read.schema(
        "n INT, s STRING, d DECIMAL(10,2), b BOOLEAN, x DOUBLE"
    ).csv(wh + "/mixednots"), "SELECT n, s, CAST(d AS STRING) AS d, b, x FROM xv"),
    ("twots", lambda: s.read.schema("n INT, a TIMESTAMP, b TIMESTAMP").csv(wh + "/twots"),
     "SELECT n, unix_timestamp(a) AS ua, unix_timestamp(b) AS ub FROM xv"),
    ("emptyts", lambda: s.read.schema(SCHEMA).csv(wh + "/emptyts"), U),
    ("nullv", lambda: s.read.option("nullValue", "NA").schema(SCHEMA).csv(wh + "/nullv"), U),
    ("dir", lambda: s.read.schema(SCHEMA).csv(wh + "/dir"), U),
    ("nots", lambda: s.read.schema("n INT, s STRING").csv(wh + "/nots"), "SELECT n, s FROM xv"),
    ("struct", lambda: s.read.schema(s.read.schema(SCHEMA).csv(wh + "/good").schema).csv(
        wh + "/good"), U),
    ("load", lambda: s.read.format("csv").option("header", "true").schema(SCHEMA).load(
        wh + "/hdr"), U),
    ("hour", lambda: s.read.schema(SCHEMA).csv(wh + "/good"),
     "SELECT n, hour(ts) AS h, CAST(ts AS STRING) AS t, CAST(ts AS DATE) AS d FROM xv"),
]
for ansi in ("true", "false"):
    for zone in (LA, "UTC", TK):
        for tag, build, query in CELLS:
            r.conf(zone, ansi)
            cell = "X/" + ansi + "/" + zone.split("/")[-1] + "/" + tag
            try:
                build().createOrReplaceTempView("xv")
            except Exception as error:
                r.fail(cell, error)
                continue
            r.sql(cell, query)
r.conf(LA, "true", "TIMESTAMP_NTZ")
try:
    s.read.schema(SCHEMA).csv(wh + "/good").createOrReplaceTempView("xv")
    r.sql("X/ntz_session/LA/good", "SELECT n, CAST(ts AS STRING) AS t, hour(ts) AS h FROM xv")
except Exception as error:
    r.fail("X/ntz_session/LA/good", error)
r.conf("UTC", "true", "TIMESTAMP_LTZ")
for key in ("good", "nots"):
    for zone in (LA, "UTC"):
        r.conf(zone, "true")
        schema = SCHEMA if key == "good" else "n INT, s STRING"
        s.read.schema(schema).csv(wh + "/" + key).filter("n > 0").createOrReplaceTempView("xv")
        try:
            plan = s.sql("EXPLAIN SELECT * FROM xv").collect()
            text = "\n".join(" | ".join(str(cell) for cell in row) for row in plan)
        except Exception as error:
            text = "ERROR " + str(error)[:200]
        r.emit({"tag": "E/" + zone.split("/")[-1] + "/" + key, "plan": text.replace(wh, "<wh>")})
s.stop()
