import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

ENGINE, PARENT, OUT, MODE = sys.argv[1:5]
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
wh = tempfile.mkdtemp(prefix="rg-" + ENGINE + "-", dir=PARENT)
os.chdir(wh)


def put(rel: str, text: str) -> str:
    target = Path(wh, rel)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    return str(target)


if ENGINE == "spark":
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
else:
    import repark._native as native
    from repark import ReparkSession

    print("NATIVE", native.__file__, flush=True)
    builder = ReparkSession.builder.appName("csvts-delta3-residual")
s = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
if ENGINE == "spark":
    s.sparkContext.setLogLevel("ERROR")
out = Path(OUT)
out.write_text("")


def cond(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return "ERR " + (found.group(1) if found else text[:200])


def allstr(frame: Any) -> Any:
    exprs = []
    for name, kind in frame.dtypes:
        exprs.append("CAST(`" + name + "` AS STRING) AS `" + name + "`")
        if kind.startswith("timestamp"):
            exprs.append("unix_timestamp(`" + name + "`) AS `u_" + name + "`")
    return frame.selectExpr(*exprs)


def cell(tag: str, build: Any) -> None:
    try:
        found = allstr(build()).collect()
        rows = sorted(json.dumps([None if v is None else str(v) for v in row]) for row in found)
        got: dict[str, Any] = {"rows": rows}
    except Exception as error:
        got = {"error": cond(error)}
    with out.open("a") as handle:
        handle.write(json.dumps({"tag": tag, **got}) + "\n")


DATA = {
    "x": (
        "1,1e2,2024-03-10 02:30:00,2024-03-10 02:30:00\n",
        "2,1.5E-1,2024-01-01 03:00:00,2024-06-01 12:00:00\n",
    ),
    "c": (
        "1,1.25,2024-01-02 03:04:05,2024-06-01 12:00:00\n",
        "2,2.50,2024-02-03 04:05:06,2023-12-31 23:00:00\n",
    ),
}
SCHEMAS = {
    "x": {
        "full": "n INT, d DECIMAL(10,2), tn TIMESTAMP_NTZ, ts TIMESTAMP",
        "f1": "n INT, d DECIMAL(10,2), x STRING, ts TIMESTAMP",
        "f2": "n INT, x STRING, tn TIMESTAMP_NTZ, ts TIMESTAMP",
        "nots": "n INT, d DECIMAL(10,2), tn TIMESTAMP_NTZ, x STRING",
    },
    "c": {
        "full": "n INT, d DECIMAL(10,2), tn TIMESTAMP_NTZ, ts TIMESTAMP",
        "nots": "n INT, d DECIMAL(10,2), tn TIMESTAMP_NTZ, x STRING",
    },
}
CASES: dict[str, dict[str, str]] = {}
for flavour, (a, b) in DATA.items():
    root = flavour + "/"
    put(root + "gl/a*b.csv", a)
    put(root + "gl/axb.csv", b)
    put(root + "gq/a?.csv", a)
    put(root + "gq/ab.csv", b)
    put(root + "gb/a[1].csv", a)
    put(root + "gb/a1.csv", b)
    put(root + "dd*/part.csv", a)
    put(root + "ddx/part.csv", b)
    put(root + "n5/a*b.csv", a)
    for name in ("a b.csv", "a#b.csv", "a%b.csv", "a%41b.csv"):
        put(root + "n5/" + name, b)
    put(root + "only/a*b.csv", a)
    put(root + "plain/F.csv", a + b)
    put(root + "fdir/part-0.csv", a + b)
    CASES[flavour] = {
        "star_file": str(Path(wh, root + "gl/a*b.csv")),
        "qmark_file": str(Path(wh, root + "gq/a?.csv")),
        "brack_file": str(Path(wh, root + "gb/a[1].csv")),
        "star_dir": str(Path(wh, root + "dd*")),
        "five_star": str(Path(wh, root + "n5/a*b.csv")),
        "star_alone": str(Path(wh, root + "only/a*b.csv")),
        "ctl_file": str(Path(wh, root + "plain/F.csv")),
        "ctl_glob": str(Path(wh, root + "fdir")) + "/*.csv",
    }
for ansi in ("true", "false"):
    for zone in ("UTC", "America/Los_Angeles", "America/New_York"):
        s.sql("SET spark.sql.session.timeZone=" + zone).collect()
        s.sql("SET spark.sql.ansi.enabled=" + ansi).collect()
        pre = ansi + "/" + zone.split("/")[-1].lower() + "/"
        for flavour, cases in CASES.items():
            for name, target in cases.items():
                for key, sc in SCHEMAS[flavour].items():
                    tag = pre + flavour + "/" + name + "/" + key
                    cell(tag, lambda sc=sc, target=target: s.read.schema(sc).csv(target))
print("DONE", flush=True)
if ENGINE == "spark":
    s.stop()
os._exit(0)
