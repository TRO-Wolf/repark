import sys
import tempfile
from pathlib import Path

ENGINE = sys.argv[1]
ROWS = (
    "(1, CAST('0.0' AS DOUBLE))",
    "(2, CAST('-0.0' AS DOUBLE))",
    "(3, CAST('0.0' AS DOUBLE))",
    "(4, CAST('-0.0' AS DOUBLE))",
    "(5, CAST('NaN' AS DOUBLE))",
    "(6, CAST('1.5' AS DOUBLE))",
    "(7, CAST(NULL AS DOUBLE))",
)
root = Path(tempfile.mkdtemp(prefix="szb-pt-", dir=sys.argv[2]))
if ENGINE == "spark":
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", sys.argv[3])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.c", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.c.type", "hadoop")
        .config("spark.sql.catalog.c.warehouse", str(root))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    session.sql("CREATE NAMESPACE IF NOT EXISTS c.ns")
    name = "c.ns.pt"
else:
    from repark import ReparkSession

    session = ReparkSession.builder.appName("pt").getOrCreate()
    session.register_memory_catalog("c", str(root))
    session.sql(f"CREATE NAMESPACE c.ns LOCATION '{root / 'ns'}'")
    name = "c.ns.pt"
session.sql(
    f"CREATE TABLE {name} (id BIGINT, d DOUBLE) USING iceberg PARTITIONED BY (d) "
    "TBLPROPERTIES ('format-version' = '2')"
)
session.sql(f"INSERT INTO {name} VALUES {', '.join(ROWS)}")
for query in (
    f"SELECT CAST(partition.d AS STRING) AS p, record_count FROM {name}.partitions",
    f"SELECT CAST(partition.d AS STRING) AS p, record_count FROM {name}.files",
    f"SELECT id, CAST(d AS STRING) AS d FROM {name} ORDER BY id",
):
    try:
        print(ENGINE, query.split(" FROM ")[1], sorted(str(tuple(r)) for r in session.sql(query).collect()))
    except Exception as error:
        print(ENGINE, query, "REFUSED", str(error)[:200])
