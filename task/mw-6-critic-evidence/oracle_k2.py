"""MW-6 CRITIC oracle, focused: use_caching type coercion + nonexistent table + big-manifest split."""
import shutil, sys, traceback
from pathlib import Path
from pyspark.sql import SparkSession

WAREHOUSE = Path(sys.argv[1]).resolve()
JAR = str(Path(sys.argv[2]).resolve())
if WAREHOUSE.exists():
    shutil.rmtree(WAREHOUSE)
WAREHOUSE.mkdir(parents=True)
spark = (
    SparkSession.builder.appName("mw6-critic-k2")
    .master("local[1]")
    .config("spark.jars", JAR)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ice.type", "hadoop")
    .config("spark.sql.catalog.ice.warehouse", str(WAREHOUSE))
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.ui.enabled", "False")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")

def show(label, sql):
    print(f"\n--- {label}: {sql}")
    try:
        rows = spark.sql(sql).collect()
        for r in rows:
            print("ROW", r.asDict())
        if not rows:
            print("ROW <none>")
    except Exception as exc:
        msg = str(exc)
        print("ERROR", type(exc).__name__, msg[:900].replace("\n", " | "))

def manifests(t):
    rows = spark.sql(f"SELECT content, length FROM ice.db.{t}.manifests").collect()
    print(f"MANIFESTS[{t}] n={len(rows)}", [(r['content'], r['length']) for r in rows])

V2 = "'format-version' = '2'"

print("\n===== M: use_caching type coercion on a FRESH 4-manifest table")
spark.sql(f"CREATE TABLE ice.db.m (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
for i in range(1, 5):
    spark.sql(f"INSERT INTO ice.db.m VALUES ({i}, 'v{i}')")
manifests("m")
show("M1 use_caching => 'yes' (STRING literal)", "CALL ice.system.rewrite_manifests(table => 'db.m', use_caching => 'yes')")
manifests("m")

print("\n===== N: use_caching => 1 (INT literal) on a fresh table")
spark.sql(f"CREATE TABLE ice.db.n (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
for i in range(1, 5):
    spark.sql(f"INSERT INTO ice.db.n VALUES ({i}, 'v{i}')")
show("N1 use_caching => 1", "CALL ice.system.rewrite_manifests(table => 'db.n', use_caching => 1)")
manifests("n")

print("\n===== O: use_caching => 'true' (STRING) on a fresh table")
spark.sql(f"CREATE TABLE ice.db.o (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
for i in range(1, 5):
    spark.sql(f"INSERT INTO ice.db.o VALUES ({i}, 'v{i}')")
show("O1 use_caching => 'true'", "CALL ice.system.rewrite_manifests(table => 'db.o', use_caching => 'true')")
manifests("o")

print("\n===== P: nonexistent table, full message")
try:
    spark.sql("CALL ice.system.rewrite_manifests(table => 'db.nope')").collect()
except Exception as exc:
    print("P ERROR", type(exc).__name__)
    print("P MSG", str(exc)[:1200].replace("\n", " | "))

print("\n===== Q: 12 manifests with target-size 4096 (bigger split)")
spark.sql(
    f"CREATE TABLE ice.db.q (id INT, v STRING) USING iceberg "
    f"TBLPROPERTIES ({V2}, 'commit.manifest.target-size-bytes' = '4096')"
)
for i in range(1, 13):
    spark.sql(f"INSERT INTO ice.db.q VALUES ({i}, 'v{i}')")
manifests("q")
show("Q1 rewrite_manifests", "CALL ice.system.rewrite_manifests(table => 'db.q')")
manifests("q")
print("ROWS", spark.sql("SELECT count(*) c FROM ice.db.q").collect()[0]['c'])

print("\nDONE")
spark.stop()
