import json
import re
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

ENGINE, PARENT, OUT = sys.argv[1:4]
COND = re.compile(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]")
wh = tempfile.mkdtemp(prefix="d1-" + ENGINE + "-", dir=PARENT)
if ENGINE == "spark":
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
else:
    import repark._native as native
    from repark import ReparkSession

    print("NATIVE", native.__file__, flush=True)
    builder = ReparkSession.builder.appName("csvts-delta1")
s = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
if ENGINE == "spark":
    s.sparkContext.setLogLevel("ERROR")
out = Path(OUT)
out.write_text("")


def cond(error: BaseException) -> str:
    text = " ".join(str(error).split())
    found = COND.search(text)
    return "ERR " + (found.group(1) if found else text[:200])


def emit(tag: str, got: dict[str, Any]) -> None:
    with out.open("a") as handle:
        handle.write(json.dumps({"tag": tag, **got}) + "\n")


def allstr(frame: Any) -> Any:
    exprs = []
    for name, kind in frame.dtypes:
        exprs.append("CAST(`" + name + "` AS STRING) AS `" + name + "`")
        if kind.startswith("timestamp"):
            exprs.append("unix_timestamp(`" + name + "`) AS `u_" + name + "`")
    return frame.selectExpr(*exprs)


def cell(tag: str, build: Callable[[], Any], typed: bool = False) -> None:
    try:
        frame = build()
        got: dict[str, Any] = {}
        if typed:
            got["dtypes"] = [list(pair) for pair in frame.dtypes]
            frame = allstr(frame)
        got["rows"] = sorted(
            json.dumps([None if v is None else str(v) for v in row]) for row in frame.collect()
        )
    except Exception as error:
        got = {"error": cond(error)}
    emit(tag, got)


def put(name: str, text: str, ext: str = "csv") -> str:
    target = Path(wh, name)
    target.mkdir()
    (target / ("a." + ext)).write_text(text)
    return str(target)


def sql(frame: Any, query: str) -> Any:
    frame.createOrReplaceTempView("shape")
    return s.sql(query)


GAP = "2024-03-10 02:30:00"
DEXP = put("dexp", "1,2024-01-01 03:00:00,1e2\n2,2024-01-01 12:00:00,1.5E-1\n")
NBESIDE = put("nbeside", "1," + GAP + ",2024-01-01 03:00:00\n2,2024-01-01 03:00:00," + GAP + "\n")
NEMPTY = put("nempty", "1," + GAP + ",\n2,2024-01-01 03:00:00,\n")
NZ = put("nz", "1," + GAP + ",2024-01-01T03:00:00Z\n2,2024-01-01 03:00:00,2024-06-01T00:00:00Z\n")
NHDR = put("nhdr", "b,n,a\n2024-01-01 03:00:00,1," + GAP + "\n")
NPART = put("npart", "a,zz,b\n" + GAP + ",1,2024-01-01 03:00:00\n")
INTX = put("intx", "1e3,2024-01-01 03:00:00\n2.0,2024-01-01 12:00:00\n1.5,2024-01-01 12:00:00\n")
EPOCH = put("epoch", "1,2024-01-01 03:00:00,1704067200\n")
STRTS = put("strts", "2024-01-01T03:00:00,2024-01-01 03:00:00,2024-01-01 03:00:00.000\n")
LATE = put("late", "".join(str(i) + ",\n" for i in range(1100)) + "1100,abc\n")
LATEH = put("lateh", "n,b\n" + "".join(str(i) + ",\n" for i in range(1100)) + "1100,abc\n")
SPACE = put("space", "a, b,c \n1,2,3\n")
EMPTYH = put("emptyh", "a,,c\n1,2,3\n")
COMMENT = put("comment", "#note\n1,2.5,x\n2,3.5,y\n")
MIXED = put("mixed", "1,2.5,x,true,2024-01-01,2024-01-01 03:00:00,007\n")
MLINE = put("mline", "".join(str(i) + ',"a\nb",2.5\n' for i in range(1100)))
TWO = str(Path(wh, "two"))
Path(TWO).mkdir()
Path(TWO, "a.csv").write_text("n,v\n1,2.5\n")
Path(TWO, "b.csv").write_text("n,v\n2,x\n")
URL = "file://"
for ansi in ("true", "false"):
    for zone in ("America/Los_Angeles", "UTC"):
        s.sql("SET spark.sql.session.timeZone=" + zone).collect()
        s.sql("SET spark.sql.ansi.enabled=" + ansi).collect()
        p = ansi + "/" + zone.split("/")[-1].lower() + "/"
        cell(
            p + "F1/decimal_exponent",
            lambda: sql(
                s.read.schema("n INT, ts TIMESTAMP, d DECIMAL(10,2)").csv(DEXP),
                "SELECT n, unix_timestamp(ts) AS u, CAST(d AS STRING) AS d FROM shape",
            ),
        )
        cell(
            p + "F2/ntz_beside",
            lambda: sql(
                s.read.schema("n INT, a TIMESTAMP_NTZ, b TIMESTAMP").csv(NBESIDE),
                "SELECT n, CAST(a AS STRING) AS a, unix_timestamp(b) AS u FROM shape",
            ),
        )
        cell(
            p + "F2/ntz_empty_ts",
            lambda: sql(
                s.read.schema("n INT, a TIMESTAMP_NTZ, b TIMESTAMP").csv(NEMPTY),
                "SELECT n, CAST(a AS STRING) AS a, unix_timestamp(b) AS u FROM shape",
            ),
        )
        cell(
            p + "F2/ntz_z_ts",
            lambda: sql(
                s.read.schema("n INT, a TIMESTAMP_NTZ, b TIMESTAMP").csv(NZ),
                "SELECT n, CAST(a AS STRING) AS a, unix_timestamp(b) AS u FROM shape",
            ),
        )
        cell(
            p + "F2/ntz_header_by_name",
            lambda: sql(
                s.read.option("header", "true")
                .schema("n INT, a TIMESTAMP_NTZ, b TIMESTAMP")
                .csv(NHDR),
                "SELECT n, CAST(a AS STRING) AS a, unix_timestamp(b) AS u FROM shape",
            ),
        )
        cell(
            p + "F2/ntz_header_partial",
            lambda: sql(
                s.read.option("header", "true")
                .schema("a TIMESTAMP_NTZ, n INT, b TIMESTAMP")
                .csv(NPART),
                "SELECT n, CAST(a AS STRING) AS a, unix_timestamp(b) AS u FROM shape",
            ),
        )
        cell(
            p + "F2/ntz_url",
            lambda: sql(
                s.read.schema("n INT, a TIMESTAMP_NTZ, b TIMESTAMP").csv(URL + NBESIDE + "/a.csv"),
                "SELECT n, CAST(a AS STRING) AS a, unix_timestamp(b) AS u FROM shape",
            ),
        )
        cell(
            p + "N1/int_from_text",
            lambda: sql(
                s.read.schema("n INT, ts TIMESTAMP").csv(INTX),
                "SELECT n, unix_timestamp(ts) AS u FROM shape",
            ),
        )
        cell(
            p + "N1/bigint_from_text",
            lambda: sql(
                s.read.schema("n BIGINT, ts TIMESTAMP").csv(INTX),
                "SELECT n, unix_timestamp(ts) AS u FROM shape",
            ),
        )
        cell(
            p + "N1/epoch_second_ts",
            lambda: sql(
                s.read.schema("n INT, a TIMESTAMP, b TIMESTAMP").csv(EPOCH),
                "SELECT n, unix_timestamp(a) AS ua, unix_timestamp(b) AS ub FROM shape",
            ),
        )
        cell(
            p + "K/string_date_ts",
            lambda: sql(
                s.read.schema("s STRING, d DATE, ts TIMESTAMP").csv(STRTS),
                "SELECT s, CAST(d AS STRING) AS d, unix_timestamp(ts) AS u FROM shape",
            ),
        )
        cell(
            p + "K/string_beside_ts",
            lambda: sql(
                s.read.schema(
                    "a STRING, b STRING, c STRING, d STRING, e STRING, ts TIMESTAMP, g STRING"
                ).csv(MIXED),
                "SELECT a, b, c, d, e, unix_timestamp(ts) AS u, g FROM shape",
            ),
        )
        cell(
            p + "K/decimal_url",
            lambda: sql(
                s.read.schema("n INT, ts TIMESTAMP, d DECIMAL(10,2)").csv(URL + DEXP + "/a.csv"),
                "SELECT n, unix_timestamp(ts) AS u, CAST(d AS STRING) AS d FROM shape",
            ),
        )
        if zone != "UTC":
            continue
        for door, prefix in (("local", ""), ("url", URL)):
            q = p + "I/" + door + "/"
            cell(
                q + "void_late",
                lambda prefix=prefix: s.read.option("inferSchema", "true").csv(
                    prefix + LATE + "/a.csv"
                ),
                True,
            )
            cell(
                q + "void_late_header",
                lambda prefix=prefix: (
                    s.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(prefix + LATEH + "/a.csv")
                ),
                True,
            )
            cell(
                q + "header_space",
                lambda prefix=prefix: (
                    s.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(prefix + SPACE + "/a.csv")
                ),
                True,
            )
            cell(
                q + "header_empty",
                lambda prefix=prefix: (
                    s.read.option("header", "true")
                    .option("inferSchema", "true")
                    .csv(prefix + EMPTYH + "/a.csv")
                ),
                True,
            )
            cell(
                q + "comment",
                lambda prefix=prefix: (
                    s.read.option("comment", "#")
                    .option("inferSchema", "true")
                    .csv(prefix + COMMENT + "/a.csv")
                ),
                True,
            )
            cell(
                q + "mixed",
                lambda prefix=prefix: s.read.option("inferSchema", "true").csv(
                    prefix + MIXED + "/a.csv"
                ),
                True,
            )
            cell(
                q + "multiline",
                lambda prefix=prefix: (
                    s.read.option("multiLine", "true")
                    .option("inferSchema", "true")
                    .csv(prefix + MLINE + "/a.csv")
                    .groupBy()
                    .count()
                ),
                False,
            )
        cell(
            p + "I/local/two_files",
            lambda: s.read.option("header", "true").option("inferSchema", "true").csv(TWO),
            True,
        )
        cell(
            p + "I/local/two_files_noinfer", lambda: s.read.option("header", "true").csv(TWO), True
        )
print("DONE", flush=True)
