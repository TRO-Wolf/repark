import sys

from repark import ReparkSession

out = sys.argv[1]
s = ReparkSession.builder.appName("cu-cost-data").getOrCreate()
s.range(10_000_000).createOrReplaceTempView("r")
s.sql(
    "SELECT id, CAST(((id * 7919) % 10000000) - 5000000 AS DECIMAL(10,0)) / 100 AS craw, "
    "((id * 104729) % 2000000000) - 1000000000 AS b, "
    "CAST(((id * 7919) % 20000) - 10000 AS DECIMAL(10,0)) / 100 AS sraw FROM r"
).createOrReplaceTempView("g")
s.sql(
    "SELECT id, CAST(craw AS DECIMAL(10,2)) AS c, CAST(b AS BIGINT) AS b, "
    "CAST(sraw AS DECIMAL(10,2)) AS s FROM g"
).write.mode("overwrite").parquet(f"{out}/c2")
s.sql(
    "SELECT id, CAST(CAST(id AS DECIMAL(12,0)) / 100 - 50000 AS DECIMAL(10,2)) AS c, "
    "CAST(((id * 104729) % 2000000000) - 1000000000 AS BIGINT) AS b, "
    "CAST(CAST((id % 20000) - 10000 AS DECIMAL(10,0)) / 100 AS DECIMAL(10,2)) AS s FROM r"
).write.mode("overwrite").parquet(f"{out}/c3src")
print("ok")
