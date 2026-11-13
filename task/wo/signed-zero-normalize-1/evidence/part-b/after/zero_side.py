import json
import math
import sys

engine = sys.argv[1]
if engine == "spark":
    from pyspark.sql import SparkSession, functions

    spark = SparkSession.builder.master("local[2]").getOrCreate()
else:
    from repark import ReparkSession, functions

    spark = ReparkSession.builder.appName("zero-side").getOrCreate()


def sign(v: float | None) -> str | None:
    return (
        None
        if v is None
        else ("nan" if v != v else ("-0.0" if v == 0 and math.copysign(1, v) < 0 else repr(v)))
    )


out = {}
src = spark.createDataFrame([(1, -0.0), (1, 0.0), (1, 0.0)], ["g", "x"])
row = (
    src.groupBy("g")
    .agg(functions.collect_list("x").alias("l"), functions.collect_set("x").alias("s"))
    .collect()[0]
)
out["collect_list"] = sorted(sign(v) for v in row["l"])
out["collect_set"] = sorted(sign(v) for v in row["s"])
multi = spark.createDataFrame([(-0.0, 0.0), (0.0, 0.0), (-0.0, -0.0)], ["a", "b"])
out["count_distinct_ab"] = multi.agg(functions.countDistinct("a", "b").alias("n")).collect()[0]["n"]
dd = spark.createDataFrame(
    [(v,) for v in [1.0, 0.0, 2.0, None, float("nan"), -0.0, 0.0, 1.0]], "c DOUBLE"
)
out["dropdup_double"] = sorted((sign(r["c"]) or "null") for r in dd.dropDuplicates(["c"]).collect())
print(json.dumps({"engine": engine, **out}))
