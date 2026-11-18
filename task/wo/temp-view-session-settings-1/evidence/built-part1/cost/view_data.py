import sys

import repark._native as native
from repark import ReparkSession

print("native", native.__file__, flush=True)
out = sys.argv[1]
s = ReparkSession.builder.appName("view-cost-data").getOrCreate()
s.range(2_000_000).selectExpr(
    "CAST(id % 100000 AS INT) AS a",
    "id AS b",
    "CAST(id % 977 AS STRING) AS s",
).write.mode("overwrite").parquet(f"{out}/k")
s.range(100_000).selectExpr("CAST(id AS INT) AS a", "CAST(id % 13 AS INT) AS w").write.mode(
    "overwrite"
).parquet(f"{out}/k2")
print(s.read.parquet(f"{out}/k").selectExpr("count(*)", "max(a)").collect())
