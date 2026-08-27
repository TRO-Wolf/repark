"""MW-6 CRITIC oracle: novel shapes the Actor's committed tests do not cover."""

import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

WAREHOUSE = Path(sys.argv[1]).resolve()
JAR = str(Path(sys.argv[2]).resolve())

if WAREHOUSE.exists():
    shutil.rmtree(WAREHOUSE)
WAREHOUSE.mkdir(parents=True)

spark = (
    SparkSession.builder.appName("mw6-critic-oracle")
    .master("local[1]")
    .config("spark.jars", JAR)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ice.type", "hadoop")
    .config("spark.sql.catalog.ice.warehouse", str(WAREHOUSE))
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.ui.enabled", "False")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("SPARK", spark.version)
spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")


def show(label, sql):
    print(f"\n--- {label}: {sql}")
    try:
        df = spark.sql(sql)
        rows = df.collect()
        for row in rows:
            print("ROW", row.asDict())
        if not rows:
            print("ROW <none>")
    except Exception as exc:  # noqa: BLE001
        print("ERROR", type(exc).__name__, str(exc).splitlines()[0][:400])


def manifests(table):
    rows = spark.sql(
        f"SELECT partition_spec_id, content, length FROM ice.db.{table}.manifests"
    ).collect()
    print(
        f"MANIFESTS[{table}] n={len(rows)}",
        [(r["partition_spec_id"], r["content"], r["length"]) for r in rows],
    )


def snaps(table):
    rows = spark.sql(f"SELECT snapshot_id, operation FROM ice.db.{table}.snapshots").collect()
    print(f"SNAPSHOTS[{table}] n={len(rows)}", [r["operation"] for r in rows])


def rowcount(table):
    print(f"ROWS[{table}]", spark.sql(f"SELECT count(*) c FROM ice.db.{table}").collect()[0]["c"])


V2 = "'format-version' = '2'"

# ===== A: 2 data manifests + 1 delete manifest — data leg works, delete leg is a no-op.
print("\n===== A: 2 data + 1 delete manifest (control: refusal must NOT fire)")
spark.sql(
    f"CREATE TABLE ice.db.a (id INT, v STRING) USING iceberg "
    f"TBLPROPERTIES ({V2}, 'write.delete.mode' = 'merge-on-read')"
)
spark.sql("INSERT INTO ice.db.a VALUES (1,'a'),(2,'b'),(3,'c')")
spark.sql("DELETE FROM ice.db.a WHERE id = 1")
spark.sql("INSERT INTO ice.db.a VALUES (4,'d')")
manifests("a")
rowcount("a")
show("A1 rewrite_manifests", "CALL ice.system.rewrite_manifests(table => 'db.a')")
manifests("a")
rowcount("a")

# ===== B: 24 appends — a large manifest count.
print("\n===== B: 24 data manifests")
spark.sql(f"CREATE TABLE ice.db.b (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
for i in range(1, 25):
    spark.sql(f"INSERT INTO ice.db.b VALUES ({i}, 'v{i}')")
manifests("b")
show("B1 rewrite_manifests", "CALL ice.system.rewrite_manifests(table => 'db.b')")
manifests("b")
rowcount("b")

# ===== C: after expire_snapshots.
print("\n===== C: rewrite after expire_snapshots")
spark.sql(f"CREATE TABLE ice.db.c (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
for i in range(1, 6):
    spark.sql(f"INSERT INTO ice.db.c VALUES ({i}, 'v{i}')")
snaps("c")
show(
    "C1 expire_snapshots",
    "CALL ice.system.expire_snapshots(table => 'db.c', older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1)",
)
snaps("c")
manifests("c")
show("C2 rewrite_manifests", "CALL ice.system.rewrite_manifests(table => 'db.c')")
manifests("c")
rowcount("c")

# ===== D: schema evolution (ADD COLUMN) then more appends.
print("\n===== D: rewrite after ADD COLUMN schema evolution")
spark.sql(f"CREATE TABLE ice.db.d (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
for i in range(1, 3):
    spark.sql(f"INSERT INTO ice.db.d VALUES ({i}, 'v{i}')")
spark.sql("ALTER TABLE ice.db.d ADD COLUMN extra STRING")
for i in range(3, 6):
    spark.sql(f"INSERT INTO ice.db.d VALUES ({i}, 'v{i}', 'e{i}')")
manifests("d")
show("D1 rewrite_manifests", "CALL ice.system.rewrite_manifests(table => 'db.d')")
manifests("d")
show("D2 rows", "SELECT id, v, extra FROM ice.db.d ORDER BY id")

# ===== E: tiny commit.manifest.target-size-bytes on a 5-manifest table.
print("\n===== E: 5 manifests, commit.manifest.target-size-bytes = 4096")
spark.sql(
    f"CREATE TABLE ice.db.e (id INT, v STRING) USING iceberg "
    f"TBLPROPERTIES ({V2}, 'commit.manifest.target-size-bytes' = '4096')"
)
for i in range(1, 6):
    spark.sql(f"INSERT INTO ice.db.e VALUES ({i}, 'v{i}')")
manifests("e")
show("E1 rewrite_manifests", "CALL ice.system.rewrite_manifests(table => 'db.e')")
manifests("e")
rowcount("e")

# ===== F: ONE manifest bigger than a tiny target — the no-op boundary's other side.
print("\n===== F: one manifest, target below its length (no-op boundary)")
spark.sql(f"CREATE TABLE ice.db.f (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
spark.sql("INSERT INTO ice.db.f VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
manifests("f")
spark.sql("ALTER TABLE ice.db.f SET TBLPROPERTIES ('commit.manifest.target-size-bytes' = '1024')")
show("F1 rewrite_manifests (1 manifest, target 1024)", "CALL ice.system.rewrite_manifests(table => 'db.f')")
manifests("f")
rowcount("f")

# ===== G: positional argument form.
print("\n===== G: positional argument form")
spark.sql(f"CREATE TABLE ice.db.g (id INT, v STRING) USING iceberg TBLPROPERTIES ({V2})")
for i in range(1, 4):
    spark.sql(f"INSERT INTO ice.db.g VALUES ({i}, 'v{i}')")
show("G1 positional table only", "CALL ice.system.rewrite_manifests('db.g')")
manifests("g")

# ===== H: nonexistent table.
print("\n===== H: nonexistent table")
show("H1", "CALL ice.system.rewrite_manifests(table => 'db.does_not_exist')")

# ===== I: call twice with a MERGE between.
print("\n===== I: rewrite, MERGE, rewrite again")
spark.sql(
    f"CREATE TABLE ice.db.i (id INT, v STRING) USING iceberg "
    f"TBLPROPERTIES ({V2}, 'write.merge.mode' = 'merge-on-read')"
)
spark.sql("INSERT INTO ice.db.i VALUES (1,'a'),(2,'b'),(3,'c')")
spark.sql("INSERT INTO ice.db.i VALUES (4,'d')")
manifests("i")
show("I1 first rewrite", "CALL ice.system.rewrite_manifests(table => 'db.i')")
manifests("i")
spark.sql(
    "MERGE INTO ice.db.i AS t USING (SELECT 2 AS id, 'm2' AS v) AS s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET t.v = s.v"
)
manifests("i")
show("I2 second rewrite (after MERGE)", "CALL ice.system.rewrite_manifests(table => 'db.i')")
manifests("i")
show("I3 rows", "SELECT id, v FROM ice.db.i ORDER BY id")

# ===== J: partitioned table, several partitions.
print("\n===== J: partitioned table, 6 appends over 3 partitions")
spark.sql(
    f"CREATE TABLE ice.db.j (id INT, grp STRING) USING iceberg PARTITIONED BY (grp) "
    f"TBLPROPERTIES ({V2})"
)
for i in range(1, 7):
    spark.sql(f"INSERT INTO ice.db.j VALUES ({i}, 'g{i % 3}')")
manifests("j")
show("J1 rewrite_manifests", "CALL ice.system.rewrite_manifests(table => 'db.j')")
manifests("j")
rowcount("j")

# ===== K: 0 data manifests at the current spec but delete manifests present.
print("\n===== K: unknown named argument")
show("K1 bogus arg", "CALL ice.system.rewrite_manifests(table => 'db.g', bogus => 1)")
show("K2 use_caching non-boolean", "CALL ice.system.rewrite_manifests(table => 'db.g', use_caching => 'yes')")
show("K3 too many positional", "CALL ice.system.rewrite_manifests('db.g', true, 0, 9)")

print("\nDONE")
spark.stop()
