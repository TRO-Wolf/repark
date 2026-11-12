import json
import sys

engine = sys.argv[1]
if engine == "spark":
    from pyspark.sql import SparkSession

    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
else:
    from repark import ReparkSession

    s = ReparkSession.builder.appName("sz-div").getOrCreate()
stmts = [
    "SELECT 1.0D / CAST('-0.0' AS DOUBLE) AS x",
    "SELECT 1.0D % CAST('-0.0' AS DOUBLE) AS x",
    "SELECT 1.0D / CAST('0.0' AS DOUBLE) AS x",
    "SELECT v / d AS x FROM VALUES (1.0D, CAST('-0.0' AS DOUBLE)), "
    "(1.0D, CAST('0.0' AS DOUBLE)) AS t(v, d)",
    "SELECT nullif(CAST('-0.0' AS DOUBLE), 0.0D) AS x",
]
for ansi in ("false", "true"):
    s.conf.set("spark.sql.ansi.enabled", ansi)
    for text in stmts:
        try:
            rows = [str(r[0]) for r in s.sql(text).collect()]
            out = {"ansi": ansi, "text": text, "rows": rows}
        except BaseException as e:
            message = str(e)[:90].rsplit(" ", 1)[0]
            out = {"ansi": ansi, "text": text, "error": f"{type(e).__name__}: {message}"}
        print(json.dumps(out))
