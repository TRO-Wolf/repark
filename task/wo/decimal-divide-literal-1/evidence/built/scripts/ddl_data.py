import sys

import repark._native as native
from repark import ReparkSession

print("native", native.__file__, flush=True)
out = sys.argv[1]
s = ReparkSession.builder.appName("ddl-cost-data").getOrCreate()
s.range(10_000_000).selectExpr(
    "id",
    "CAST(CAST((id * 7919) % 99999999 AS DECIMAL(10,0)) / 100 AS DECIMAL(10,2)) AS d10",
    "CAST(CAST((id * 7919) % 99999999 AS DECIMAL(38,0)) * CAST('1000000000000000000' AS DECIMAL(38,0)) / 100 AS DECIMAL(38,2)) AS d38",
    "CAST((id * 7919) % 99999 + 1 AS INT) AS i1",
    "CAST((id * 104729) % 997 + 1 AS INT) AS i2",
    "CAST((id * 7919) % 99999999 AS DOUBLE) / 100 AS x1",
    "CAST((id * 104729) % 997 + 1 AS DOUBLE) / 7 AS x2",
).write.mode("overwrite").parquet(f"{out}/k")
print(s.read.parquet(f"{out}/k").selectExpr("count(*)", "max(d38)", "max(d10)", "min(i2)").collect())
