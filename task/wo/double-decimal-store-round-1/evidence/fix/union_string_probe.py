import sys

import repark._native as native
from repark import ReparkSession

UNION = "UNION ALL SELECT"
DEC = "CAST(0 AS DECIMAL(10,2))"
DBL = "CAST(1.255 AS DOUBLE)"
print(native.__file__)
s = ReparkSession.builder.appName("u").config("spark.sql.ansi.enabled", sys.argv[1]).getOrCreate()
s.sql(
    "SELECT * FROM VALUES (1, CAST('1.255' AS DOUBLE)), (2, CAST('2.5' AS DOUBLE)) AS x(id, v)"
).createOrReplaceTempView("f")
for q in [
    f"SELECT id, CAST(v AS STRING) FROM (SELECT id, v FROM f {UNION} -1, {DEC}) ORDER BY id",
    f"SELECT CAST(v AS STRING) FROM (SELECT {DBL} AS v {UNION} {DEC})",
    f"SELECT CAST(v AS STRING) FROM (SELECT {DEC} AS v {UNION} {DBL})",
]:
    try:
        print("OK", [tuple(r) for r in s.sql(q).collect()])
    except Exception as e:
        print("ERR", str(e).splitlines()[0][:200])
