import json
import shutil
import sys

LEG = sys.argv[1]
B = "named_struct('b', 1, 'a', 2)"
PREDICATES = {
    "between": f"s BETWEEN {B} AND {B}",
    "nullif": f"nullif(s, {B}) IS NULL",
    "case_simple": f"CASE s WHEN {B} THEN true ELSE false END",
    "eq_control": f"s = {B}",
}
ROWS = (
    "SELECT 1 AS id, named_struct('a', 1, 'b', 2) AS s UNION ALL SELECT 2, "
    "named_struct('a', 2, 'b', 1)"
)
out = {}
if LEG == "spark":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    )
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW t AS {ROWS}")
    for key, predicate in PREDICATES.items():
        out[key] = sorted(
            row[0] for row in spark.sql(f"SELECT id FROM t WHERE {predicate}").collect()
        )
    out["values_schema"] = spark.sql(
        "SELECT * FROM VALUES (1, named_struct('a', 1, 'b', 2)), (2, named_struct('a', 2, 'b', "
        "1)) AS t(lid, s)"
    ).schema.simpleString()
else:
    from repark import ReparkSession

    warehouse = sys.argv[2]
    shutil.rmtree(warehouse, ignore_errors=True)
    spark = (
        ReparkSession.builder.appName("cmpmemo-dml")
        .config("spark.sql.catalog.hd.type", "hadoop")
        .config("spark.sql.catalog.hd.warehouse", warehouse)
        .getOrCreate()
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.vp")
    for key, predicate in PREDICATES.items():
        table = f"hd.vp.t_{key}"
        spark.sql(f"CREATE TABLE {table} (id INT, s STRUCT<a: INT, b: INT>) USING iceberg")
        spark.sql(f"INSERT INTO {table} {ROWS}")
        try:
            selected = sorted(
                row[0] for row in spark.sql(f"SELECT id FROM {table} WHERE {predicate}").collect()
            )
            spark.sql(f"DELETE FROM {table} WHERE {predicate}")
            left = sorted(row[0] for row in spark.sql(f"SELECT id FROM {table}").collect())
            out[key] = {"selected": selected, "left_after_delete": left}
        except Exception as error:
            out[key] = {"error": " ".join(str(error).split())[:200]}
    out["values_schema"] = spark.sql(
        "SELECT * FROM VALUES (1, named_struct('a', 1, 'b', 2)), (2, named_struct('a', 2, 'b', "
        "1)) AS t(lid, s)"
    ).schema.simpleString()
print("RESULT", LEG, json.dumps(out))
spark.stop()
