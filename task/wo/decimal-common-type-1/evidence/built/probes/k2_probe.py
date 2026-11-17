import time

from repark import ReparkSession

s = ReparkSession.builder.appName("k2p").getOrCreate()
s.sql(
    "SELECT 1 AS id, CAST(0.5 AS DECIMAL(10,2)) AS c10, CAST(1 AS DECIMAL(38,0)) AS a38"
).createOrReplaceTempView("one")
for name, term in (
    ("auto", "c10 <> a38 + {i}"),
    ("user", "CAST(c10 AS DECIMAL(38,0)) <> a38 + {i}"),
    ("plain", "CAST(c10 AS DECIMAL(38,0)) <> a38"),
):
    sql = "SELECT id FROM one WHERE " + " AND ".join(term.format(i=i) for i in range(200))
    s.sql(sql).collect()
    t = time.perf_counter()
    for _ in range(10):
        s.sql(sql).collect()
    print(name, round((time.perf_counter() - t) / 10, 4))
