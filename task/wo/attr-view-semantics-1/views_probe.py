import json
import os
import sys
import tempfile

ENGINE = sys.argv[1]
OUT_PATH = sys.argv[2]
WORK = tempfile.mkdtemp(prefix="viewsprobe_" + ENGINE + "_")

if ENGINE == "spark":
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    S = (
        SparkSession.builder.master("local[1]")
        .appName("views")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", os.path.join(WORK, "warehouse"))
        .getOrCreate()
    )
    S.sparkContext.setLogLevel("OFF")
else:
    from repark import ReparkSession
    from repark.spark import functions as F

    S = ReparkSession.builder.appName("views").getOrCreate()

ROWS = []


def record_ids(label, frame):
    entry = {"id": label, "kind": "ids"}
    try:
        if ENGINE == "spark":
            entry["outcome"] = "OK"
            entry["ids"] = str(frame._jdf.queryExecution().analyzed().output())
        else:
            from repark import _native

            entry["outcome"] = "OK"
            entry["ids"] = [str(x) for x in _native.attribute_ids(frame._inner)]
    except Exception as e:
        entry["outcome"] = "ERR"
        entry["error_class"] = type(e).__name__
        entry["error_first_line"] = str(e).splitlines()[0][:300] if str(e).splitlines() else ""
        entry["condition"] = getattr(e, "getCondition", lambda: None)()
    ROWS.append(entry)


def record_run(label, fn):
    entry = {"id": label, "kind": "run"}
    try:
        value = fn()
        entry["outcome"] = "OK"
        entry["rows"] = sorted(tuple(r) for r in value.collect())
    except Exception as e:
        entry["outcome"] = "ERR"
        entry["error_class"] = type(e).__name__
        entry["error_first_line"] = str(e).splitlines()[0][:300] if str(e).splitlines() else ""
        entry["condition"] = getattr(e, "getCondition", lambda: None)()
    ROWS.append(entry)


def record_value(label, fn):
    entry = {"id": label, "kind": "run"}
    try:
        entry["outcome"] = "OK"
        entry["rows"] = fn()
    except Exception as e:
        entry["outcome"] = "ERR"
        entry["error_class"] = type(e).__name__
        entry["error_first_line"] = str(e).splitlines()[0][:300] if str(e).splitlines() else ""
        entry["condition"] = getattr(e, "getCondition", lambda: None)()
    ROWS.append(entry)


d = S.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
d.createOrReplaceTempView("tv")
record_ids("d", d)
record_ids("table", S.table("tv"))
record_ids("table2", S.table("tv"))
record_ids("sql-star", S.sql("SELECT * FROM tv"))
record_ids("sql-v", S.sql("SELECT v FROM tv"))
record_ids("sql-vplus0", S.sql("SELECT v + 0 AS v FROM tv"))
record_ids("sql-alias", S.sql("SELECT v AS v FROM tv"))
record_run("V1", lambda: S.table("tv").select(d["v"]))
record_run("V2", lambda: S.sql("SELECT * FROM tv").select(d["v"]))
record_run("V3", lambda: S.sql("SELECT v + 0 AS v FROM tv").select(d["v"]))
record_run("V4", lambda: S.sql("SELECT v AS v FROM tv").select(d["v"]))
t5 = S.table("tv")
record_run("V5", lambda t5=t5: d.join(t5, d["id"] == t5["id"]))
t6a = S.table("tv")
t6b = S.table("tv")
record_run("V6", lambda t6a=t6a, t6b=t6b: t6a.join(t6b, t6a["id"] == t6b["id"]))
t1 = S.table("tv")
t2 = S.table("tv")
record_run("V6b", lambda t1=t1, t2=t2: t1.join(t2, "id").select(t1["v"]))
record_run("V7", lambda: S.sql("SELECT a.v, b.v AS w FROM tv a JOIN tv b ON a.id = b.id"))
d2 = d.withColumn("w", F.lit(1))
d2.createOrReplaceTempView("tv2")
record_ids("d2", d2)
record_ids("table-tv2", S.table("tv2"))
record_run("V8", lambda: S.table("tv2").select(d["v"], d2["w"]))
S.sql("CREATE OR REPLACE TEMP VIEW sv AS SELECT * FROM tv")
record_ids("sv", S.table("sv"))
record_run("V9", lambda: S.table("sv").select(d["v"]))


def v3_write():
    path = os.path.join(WORK, "sv_parquet")
    S.table("sv").write.mode("overwrite").parquet(path)
    return sorted(tuple(r) for r in S.read.parquet(path).collect())


record_value("V3-write", v3_write)
record_run("V3-groupby", lambda: S.table("sv").groupBy("id").count())

try:
    d.createOrReplaceGlobalTempView("gv")
    record_ids("global-gv", S.table("global_temp.gv"))
    record_run("V10", lambda: S.table("global_temp.gv").select(d["v"]))
except Exception as e:
    ROWS.append(
        {
            "id": "V10",
            "kind": "run",
            "outcome": "ERR",
            "error_class": type(e).__name__,
            "error_first_line": str(e).splitlines()[0][:300] if str(e).splitlines() else "",
            "condition": getattr(e, "getCondition", lambda: None)(),
        }
    )
d.createOrReplaceTempView("tv")
record_run("V11", lambda: S.table("tv").select(d["v"]))
e = S.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
e.createOrReplaceTempView("tv")
record_run("V12", lambda: S.table("tv").select(d["v"]))
record_run("V13", lambda: S.table("tv").select(e["v"]))
d.filter(d["id"] > 1).createOrReplaceTempView("tf")
record_run("V14", lambda: S.table("tf").select(d["v"]))
d.groupBy("id").agg(F.sum("v").alias("v")).createOrReplaceTempView("ta")
record_run("V15", lambda: S.table("ta").select(d["v"]))

with open(OUT_PATH, "w") as handle:
    json.dump({"engine": ENGINE, "rows": ROWS}, handle, indent=1, default=str)

if ENGINE == "spark":
    S.stop()
