import sys

from repark import ReparkSession

out = sys.argv[1]
s = ReparkSession.builder.appName("ddu-cost-p16").getOrCreate()
s.read.parquet(f"{out}/big").createOrReplaceTempView("t")
s.sql(
    "SELECT id, CAST(CAST(d10 AS DECIMAL(16,6)) / 101 AS DECIMAL(16,15)) AS d16 FROM t"
).write.mode("overwrite").parquet(f"{out}/p16")
print("ok")
