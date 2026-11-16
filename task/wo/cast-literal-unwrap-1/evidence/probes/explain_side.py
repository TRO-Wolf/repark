import sys
import tempfile

engine, wh_parent = sys.argv[1:3]
wh = tempfile.mkdtemp(prefix=f"ex-{engine}-", dir=wh_parent)
if engine == "spark":
    from pyspark.sql import SparkSession as Builder

    b = Builder.builder.master("local[1]").config("spark.ui.enabled", "false")
else:
    import repark._native as native
    from repark import ReparkSession as Builder

    print("native", "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1])
    b = Builder.builder.appName("explain-side")
s = (
    b.config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.warehouse.dir", wh)
    .getOrCreate()
)
s.sql(
    "CREATE TABLE p USING parquet AS SELECT id, "
    "CAST(CASE id WHEN 0 THEN 0.50 WHEN 1 THEN 1.00 WHEN 2 THEN 1.25 ELSE 1.50 END"
    " AS DECIMAL(10,2))"
    " AS c FROM range(4)"
).collect()
statements = (
    "SELECT c FROM p WHERE CAST(c AS INT) = 1 ORDER BY c",
    "EXPLAIN SELECT c FROM p WHERE CAST(c AS INT) = 1",
    "EXPLAIN SELECT c FROM p",
    "EXPLAIN EXTENDED SELECT c FROM p",
    "EXPLAIN SELECT c FROM default.p",
    "EXPLAIN SELECT c FROM spark_catalog.default.p",
)
for sql in statements:
    try:
        rows = s.sql(sql).collect()
        print(sql, "->", [" ".join(str(v).split())[:300] for r in rows for v in r])
    except Exception as error:
        print(sql, "RAISE", type(error).__name__, " ".join(str(error).split())[:240])
