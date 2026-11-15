import json
import sys
import tempfile
from pathlib import Path

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"named-{engine}-{ansi}-", dir=wh_parent)
if engine == "spark":
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as F

    b = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.shuffle.partitions", "1")
    )
else:
    from repark import ReparkSession as Builder
    from repark import functions as F

    b = Builder.builder.appName("ddu-named").config("repark.sql.allowCreateFormatVersion3", "true")
s = (
    b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
records = []


def err(e: Exception) -> str:
    java = getattr(e, "java_exception", None)
    head = str(java.toString()) if java is not None else str(e)
    return "ERR " + (head.strip().splitlines() or [""])[0][:200]


def shown(rows: list) -> list:
    return sorted(str(v) for r in rows for v in r)


def record(key: str, fn) -> None:
    try:
        records.append({"key": key, "answer": fn()})
    except Exception as e:
        records.append({"key": key, "answer": err(e)})


def union_frame():
    a = s.sql("SELECT 1 AS id, CAST(0.1 AS DECIMAL(10,2)) AS v")
    b = s.sql("SELECT 2 AS id, CAST(1e-20 AS DOUBLE) AS v")
    return a.union(b)


def stored(table: str) -> dict:
    frame = s.sql(f"SELECT id, v FROM {table} ORDER BY id")
    return {"type": dict(frame.dtypes)["v"], "rows": [[r[0], str(r[1])] for r in frame.collect()]}


def save_as_table():
    union_frame().write.format("iceberg").saveAsTable("lc.ns.sat")
    return stored("lc.ns.sat")


def write_to_append():
    s.sql("CREATE TABLE lc.ns.wta (id INT, v DOUBLE) USING iceberg").collect()
    union_frame().writeTo("lc.ns.wta").append()
    return stored("lc.ns.wta")


def create_or_replace():
    s.sql("CREATE TABLE lc.ns.cor (id INT, v STRING) USING iceberg").collect()
    union_frame().writeTo("lc.ns.cor").using("iceberg").createOrReplace()
    return stored("lc.ns.cor")


def overwrite_where():
    s.sql("CREATE TABLE lc.ns.ow (id INT, v DOUBLE) USING iceberg").collect()
    s.sql("INSERT INTO lc.ns.ow VALUES (1, 5.0), (2, 6.0), (3, 7.0)").collect()
    frame = union_frame()
    if engine == "spark":
        frame.writeTo("lc.ns.ow").overwrite(F.col("id") < 3)
    else:
        frame.write.format("iceberg").mode("overwrite").option("replaceWhere", "id < 3").insertInto("lc.ns.ow")
    return stored("lc.ns.ow")


def v3_delete():
    s.sql(
        "CREATE TABLE lc.ns.v3 (id INT, d DECIMAL(10,2), v DOUBLE) USING iceberg "
        "TBLPROPERTIES ('format-version' = '3')"
    ).collect()
    s.sql(
        "INSERT INTO lc.ns.v3 VALUES (1, CAST(1.00 AS DECIMAL(10,2)), CAST('1.0000000000000004' AS DOUBLE)), "
        "(2, CAST(0 AS DECIMAL(10,2)), CAST('1e-20' AS DOUBLE)), (3, CAST(1.50 AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE))"
    ).collect()
    s.sql("DELETE FROM lc.ns.v3 WHERE d = v").collect()
    return shown(s.sql("SELECT id FROM lc.ns.v3").collect())


def select(sql: str):
    def run():
        frame = s.sql(sql)
        return {"type": frame.dtypes[0][1], "values": shown(frame.collect())}

    return run


record("save_as_table_union", save_as_table)
record("write_to_append_union", write_to_append)
record("create_or_replace_union", create_or_replace)
record("overwrite_where_union", overwrite_where)
record("v3_delete_eq", v3_delete)
s.sql(
    "CREATE OR REPLACE TEMP VIEW lt AS SELECT * FROM VALUES (1, CAST('0.1' AS DOUBLE)), (2, CAST('0.30000000000000004' AS DOUBLE)), "
    "(3, CAST('0.2' AS DOUBLE)) AS t(id, v)"
)
for key, sql in {
    "non_ascii_union": "SELECT 0.1 AS `名前` UNION ALL SELECT CAST(1e-20 AS DOUBLE)",
    "non_ascii_union_name": "SELECT * FROM (SELECT 0.1 AS `名前` UNION ALL SELECT CAST(1e-20 AS DOUBLE)) WHERE `名前` > 0.05",
    "lit_eq": "SELECT id FROM lt WHERE v = 0.1",
    "lit_in": "SELECT id FROM lt WHERE v IN (0.1, 0.3)",
    "lit_between": "SELECT id FROM lt WHERE v BETWEEN 0.1 AND 0.2",
    "lit_case": "SELECT CASE v WHEN 0.1 THEN 'a' WHEN 0.3 THEN 'b' ELSE 'c' END FROM lt",
    "struct_compare_refusal": (
        "SELECT named_struct('a', CAST(0.1 AS DECIMAL(10,2))) = named_struct('a', CAST(1e-20 AS DOUBLE))"
    ),
    "k2_written_cast": "SELECT CAST(CAST('-22.129547339983247' AS DECIMAL(38,18)) AS DOUBLE)",
    "k1_written_cast": "SELECT CAST(CAST('1.005' AS DOUBLE) AS DECIMAL(5,2))",
    "k2_arith": "SELECT CAST('-22.129547339983247' AS DECIMAL(38,18)) + CAST(0 AS DOUBLE)",
}.items():
    record(key, select(sql))
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records))
