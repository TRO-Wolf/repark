import sys

from repark import ReparkSession

out = sys.argv[1]
s = ReparkSession.builder.appName("dct-cost-data").getOrCreate()
s.range(10_000_000).createOrReplaceTempView("r")
s.sql(
    "SELECT CAST((id % 1000) - 500 AS DECIMAL(38,0)) AS a, "
    "CAST(CAST((id * 7919) % 100000 - 50000 AS DECIMAL(10,0)) / 100 AS DECIMAL(10,2)) AS c FROM r"
).write.mode("overwrite").parquet(f"{out}/k2")
s.range(10_000).createOrReplaceTempView("r3")
s.sql(
    "SELECT id, CAST(CAST(id % 100 AS DECIMAL(10,0)) * 0.02 AS DECIMAL(10,2)) AS c FROM r3"
).write.mode("overwrite").parquet(f"{out}/k3")
s.range(1_000).createOrReplaceTempView("r5")
s.sql(
    "SELECT id, CAST(CAST(id AS DECIMAL(10,0)) / 4 AS DECIMAL(10,2)) AS c10, "
    "CAST(CAST(id AS DECIMAL(10,0)) / 8 AS DECIMAL(12,4)) AS c12 FROM r5"
).write.mode("overwrite").parquet(f"{out}/k5")
print("ok")
