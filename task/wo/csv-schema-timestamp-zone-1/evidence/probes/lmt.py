import sys
from pathlib import Path

engine, dest, mode = sys.argv[1:4]
if engine == "spark":
    from pyspark.sql import SparkSession

    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
else:
    import repark._native as native
    from repark import ReparkSession

    print("NATIVE", native.__file__)
    s = ReparkSession.builder.appName("lmt").getOrCreate()
s.conf.set("spark.sql.session.timeZone", "America/New_York")
if mode == "write":
    s.sql("SELECT 1 AS id, TIMESTAMP '1850-06-01 10:00:00' AS t").write.mode("overwrite").option(
        "header", "true"
    ).csv(dest)
    for part in sorted(Path(dest).glob("*.csv")):
        print("FILE", part.name, repr(part.read_text()))
QUERY = (
    "SELECT id, CAST(t AS STRING) AS s, unix_timestamp(t) AS u, "
    "date_format(t, 'yyyy-MM-dd HH:mm:ss') AS f FROM v"
)
for zone in ("America/New_York", "UTC"):
    s.conf.set("spark.sql.session.timeZone", zone)
    try:
        frame = s.read.option("header", "true").schema("id INT, t TIMESTAMP").csv(dest)
        frame.createOrReplaceTempView("v")
        print(zone, "TS", frame.dtypes, s.sql(QUERY).collect())
    except Exception as error:
        print(zone, "TS RAISE", " ".join(str(error).split())[:200])
    try:
        text = s.read.option("header", "true").schema("id INT, t STRING").csv(dest).collect()
        print(zone, "TEXT", text)
    except Exception as error:
        print(zone, "TEXT RAISE", " ".join(str(error).split())[:120])
s.stop()
