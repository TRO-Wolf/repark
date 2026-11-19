import sys

import repark._native as native
from repark import ReparkSession

print("native", native.__file__, flush=True)
out = sys.argv[1]
s = ReparkSession.builder.appName("sum-cost-data").getOrCreate()
s.range(10_000_000).selectExpr(
    "id",
    "CAST(id % 200000 AS INT) AS g",
    "CAST(CAST((id * 7919) % 99999999 AS DECIMAL(10,0)) / 100 AS DECIMAL(10,2)) AS d10",
    "CAST(CAST((id * 7919) % 99999999 AS DECIMAL(36,0)) "
    "* CAST('1000000000000000000' AS DECIMAL(36,0)) AS DECIMAL(36,0)) AS d36",
    "CAST(CAST((id * 7919) % 99999999 AS DECIMAL(38,0)) "
    "* CAST('1000000000000000000' AS DECIMAL(38,0)) AS DECIMAL(38,0)) AS d38",
    "CAST((id * 7919) % 99999999 AS DOUBLE) / 100 AS x",
    "(id * 7919) % 99999999 AS b",
).write.mode("overwrite").parquet(f"{out}/k1")
s.range(1_000_000).selectExpr(
    "id",
    "CAST(id % 100 AS INT) AS p",
    "CAST(CAST((id * 7919) % 99999999 AS DECIMAL(10,0)) / 100 AS DECIMAL(10,2)) AS d10",
    "CAST(CAST((id * 7919) % 99999999 AS DECIMAL(38,0)) "
    "* CAST('1000000000000000000' AS DECIMAL(38,0)) AS DECIMAL(38,0)) AS d38",
).write.mode("overwrite").parquet(f"{out}/k2")
print(
    s.read.parquet(f"{out}/k1").selectExpr("count(*)", "max(d38)", "max(d36)", "max(d10)").collect()
)
print(s.read.parquet(f"{out}/k2").selectExpr("count(*)", "max(d38)").collect())
