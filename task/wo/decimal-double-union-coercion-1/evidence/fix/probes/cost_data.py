import sys

from repark import ReparkSession

out = sys.argv[1]
s = ReparkSession.builder.appName("ddu-cost-data").getOrCreate()
for name, rows in (("big", 100_000_000), ("mid", 20_000_000)):
    s.range(rows).createOrReplaceTempView("r")
    s.sql(
        "SELECT id, CAST((id % 200000) - 100000 AS DECIMAL(10,0)) / 100 AS d10raw, "
        "CAST(((id * 7919) % 200000) - 100000 AS DOUBLE) / 100 AS v FROM r"
    ).createOrReplaceTempView("g")
    s.sql(
        "SELECT id, CAST(d10raw AS DECIMAL(10,2)) AS d10, "
        "CAST(d10raw AS DECIMAL(38,18)) + CAST(id % 1000 AS DECIMAL(38,18)) / 1000000000000 AS "
        "d38, v FROM g"
    ).write.mode("overwrite").parquet(f"{out}/{name}")
print("ok")
