import json, math, sys
engine = sys.argv[1]
if engine == "spark":
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.master("local[2]").getOrCreate()
else:
    from repark import ReparkSession, functions as F
    spark = ReparkSession.builder.appName("zero-order").getOrCreate()
def sign(v):
    return "-0.0" if v == 0 and math.copysign(1, v) < 0 else repr(v)
out = {}
for name, rows in (("neg-first", [-0.0, 0.0, 0.0]), ("pos-first", [0.0, -0.0, -0.0]), ("pos-first-1", [0.0, -0.0])):
    for parts in (1, 2):
        src = spark.createDataFrame([(1, v) for v in rows], ["g", "x"]).coalesce(1) if parts == 1 else spark.createDataFrame([(1, v) for v in rows], ["g", "x"])
        row = src.groupBy("g").agg(F.collect_set("x").alias("s")).collect()[0]
        out[f"{name}/p{parts}"] = sorted(sign(v) for v in row["s"])
    t = "VALUES " + ", ".join(f"(1, CAST('{v}' AS DOUBLE))" for v in rows)
    out[f"{name}/sql"] = sorted(sign(v) for v in spark.sql(f"SELECT collect_set(x) AS s FROM ({t}) AS v(g, x) GROUP BY g").collect()[0]["s"])
print(json.dumps({"engine": engine, **out}))
