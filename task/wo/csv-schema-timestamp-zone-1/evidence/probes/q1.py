import sys
import repark._native as native
from repark import ReparkSession
from repark.spark import functions as F

print("NATIVE", native.__file__)
s = ReparkSession.builder.appName("q1").config("spark.sql.session.timeZone", "America/Los_Angeles").getOrCreate()
p = sys.argv[1]
raw = s.read_csv(p, {"header": "false"})
print(raw.dtypes)
cases = {
    "sql_lit": s.sql("SELECT CAST(TIMESTAMP_NTZ '2024-01-01 03:00:00' AS TIMESTAMP) AS ts"),
    "selectexpr": raw.selectExpr("CAST(column_2 AS TIMESTAMP) AS ts"),
    "fcast": raw.select(F.col("column_2").cast("timestamp").alias("ts")),
    "ntz_frame": s.createDataFrame([(1,)], "n INT").selectExpr("CAST(TIMESTAMP_NTZ '2024-01-01 03:00:00' AS TIMESTAMP) AS ts"),
}
for name, frame in cases.items():
    frame.createOrReplaceTempView("x")
    print(name, s.sql("SELECT unix_timestamp(ts) FROM x").collect())
    print("\n".join(str(r[1]) for r in s.sql("EXPLAIN SELECT * FROM x").collect()).splitlines()[:3])
