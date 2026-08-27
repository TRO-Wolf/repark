"""MW-6 CRITIC round 3: verify the remediation's new registry claims."""
import shutil, sys
from pathlib import Path
from pyspark.sql import SparkSession

WAREHOUSE = Path(sys.argv[1]).resolve(); JAR = str(Path(sys.argv[2]).resolve())
if WAREHOUSE.exists(): shutil.rmtree(WAREHOUSE)
WAREHOUSE.mkdir(parents=True)
spark = (SparkSession.builder.appName("mw6-critic-r2").master("local[1]")
    .config("spark.jars", JAR)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ice.type", "hadoop")
    .config("spark.sql.catalog.ice.warehouse", str(WAREHOUSE))
    .config("spark.sql.shuffle.partitions", "1").config("spark.sql.adaptive.enabled", "false")
    .config("spark.ui.enabled", "False").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
V2 = "'format-version' = '2'"

def show(label, sql):
    print(f"\n--- {label}: {sql}")
    try:
        for r in spark.sql(sql).collect(): print("ROW", r.asDict())
    except Exception as exc:
        print("ERROR", type(exc).__name__, str(exc).splitlines()[0][:300])

def mans(t):
    rows = spark.sql(f"SELECT length FROM ice.db.{t}.manifests").collect()
    tot = sum(r['length'] for r in rows)
    print(f"MANIFESTS[{t}] n={len(rows)} total_bytes={tot}")

# R1: use_caching => 'no' on a fresh 5-manifest table (registry claim).
spark.sql(f"CREATE TABLE ice.db.r1 (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
for i in range(1, 6): spark.sql(f"INSERT INTO ice.db.r1 VALUES ({i}, 'v{i}')")
mans("r1")
show("R1 use_caching => 'no'", "CALL ice.system.rewrite_manifests(table => 'db.r1', use_caching => 'no')")
mans("r1")

# R2: 5 manifests at target 4096 — byte total attributed to Spark in MANIFEST-3.
spark.sql(f"CREATE TABLE ice.db.r2 (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2}, 'commit.manifest.target-size-bytes' = '4096')")
for i in range(1, 6): spark.sql(f"INSERT INTO ice.db.r2 VALUES ({i}, 'v{i}')")
mans("r2")
show("R2 rewrite", "CALL ice.system.rewrite_manifests(table => 'db.r2')")
mans("r2")

# R3: 12 manifests at target 4096.
spark.sql(f"CREATE TABLE ice.db.r3 (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2}, 'commit.manifest.target-size-bytes' = '4096')")
for i in range(1, 13): spark.sql(f"INSERT INTO ice.db.r3 VALUES ({i}, 'v{i}')")
mans("r3")
show("R3 rewrite", "CALL ice.system.rewrite_manifests(table => 'db.r3')")
mans("r3")
print("\nDONE"); spark.stop()
