import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import drive_cmp as d

spark = d.spark_session(os.environ.get("STRUCT_CMP_WAREHOUSE", "<warehouse-spark>"))
spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.pr").collect()
cases = {
    "brief_shape_v2": "CREATE TABLE hd.pr.a (id INT, s STRUCT<a: INT, b: INT>) USING iceberg "
    "TBLPROPERTIES ('format-version'='2')",
    "brief_shape_v1": "CREATE TABLE hd.pr.b (id INT, s STRUCT<a: INT, b: INT>) USING iceberg",
}
for name, ddl in cases.items():
    spark.sql(
        f"DROP TABLE IF EXISTS hd.pr.{name[-1] if False else ''}x"
    ).collect() if False else None
    tbl = "hd.pr." + ("a" if "v2" in name else "b")
    spark.sql(f"DROP TABLE IF EXISTS {tbl}").collect()
    spark.sql(ddl.replace("hd.pr.a", tbl).replace("hd.pr.b", tbl)).collect()
    spark.sql(
        f"INSERT INTO {tbl} VALUES (1, named_struct('a', 3, 'b', 4)), (2, named_struct('a', 4, "
        "'b', 3))"
    ).collect()
    for pred in [
        "s = named_struct('b', 4, 'a', 3)",
        "s = named_struct('a', 3, 'b', 4)",
        "s = CAST(named_struct('a', 3, 'b', 4) AS STRUCT<a: INT, b: INT>)",
    ]:
        try:
            spark.sql(f"DELETE FROM {tbl} WHERE {pred}").collect()
            print(name, pred, "OK", spark.sql(f"SELECT id FROM {tbl} ORDER BY id").collect())
        except Exception as e:
            print(name, pred, "ERR", type(e).__name__, str(e)[:120])
        spark.sql(
            f"INSERT INTO {tbl} VALUES (1, named_struct('a', 3, 'b', 4)), (2, named_struct('a', "
            "4, 'b', 3))"
        ).collect() if False else None
        spark.sql(f"DROP TABLE IF EXISTS {tbl}").collect()
        spark.sql(ddl.replace("hd.pr.a", tbl).replace("hd.pr.b", tbl)).collect()
        spark.sql(
            f"INSERT INTO {tbl} VALUES (1, named_struct('a', 3, 'b', 4)), (2, named_struct('a', "
            "4, 'b', 3))"
        ).collect()
spark.stop()
