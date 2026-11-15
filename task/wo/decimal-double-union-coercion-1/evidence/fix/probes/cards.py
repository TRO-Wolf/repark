import json
import sys
import tempfile
from pathlib import Path

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"cards-{engine}-{ansi}-", dir=wh_parent)
if engine == "spark":
    from pyspark.sql import SparkSession as Builder

    b = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.shuffle.partitions", "1")
    )
else:
    from repark import ReparkSession as Builder

    b = Builder.builder.appName("ddu-cards")
s = (
    b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
MODES = {
    "cow": "'format-version' = '2', 'write.update.mode' = 'copy-on-write'",
    "mor": "'format-version' = '2', 'write.update.mode' = 'merge-on-read'",
}
records = []


def err(e: Exception) -> str:
    java = getattr(e, "java_exception", None)
    head = str(java.toString()) if java is not None else str(e)
    return "ERR " + (head.strip().splitlines() or [""])[0][:160]


def run(key, fn):
    try:
        records.append({"key": key, "answer": fn()})
    except Exception as e:
        records.append({"key": key, "answer": err(e)})


n = 0
for mode, props in MODES.items():
    for pred in ("v IN (w)", "v IN (w, 5.0D)", "v = w", "v IN (0.0D)", "NOT (v NOT IN (w))"):
        n += 1
        t = f"lc.ns.z{n}"

        def update(t=t, props=props, pred=pred):
            s.sql(f"CREATE TABLE {t} (id INT, v DOUBLE, w DOUBLE) USING iceberg TBLPROPERTIES ({props})").collect()
            s.sql(f"INSERT INTO {t} VALUES (1, CAST('-0.0' AS DOUBLE), CAST('0.0' AS DOUBLE)), (2, 0.5D, 0.5D)").collect()
            s.sql(f"UPDATE {t} SET id = id + 100 WHERE {pred}").collect()
            return sorted(int(r[0]) for r in s.sql(f"SELECT id FROM {t}").collect())

        run(f"f3/{mode}/update/{pred}", update)
        n += 1
        t2 = f"lc.ns.zs{n}"

        def select(t=t2, props=props, pred=pred):
            s.sql(f"CREATE TABLE {t} (id INT, v DOUBLE, w DOUBLE) USING iceberg TBLPROPERTIES ({props})").collect()
            s.sql(f"INSERT INTO {t} VALUES (1, CAST('-0.0' AS DOUBLE), CAST('0.0' AS DOUBLE)), (2, 0.5D, 0.5D)").collect()
            return sorted(int(r[0]) for r in s.sql(f"SELECT id FROM {t} WHERE {pred}").collect())

        run(f"f3/{mode}/select/{pred}", select)
for text in ("1.0E-4", "1E2", "1.5e1", "-2.5E-3", "1.0E-20", "1e40"):
    for fn in ("CAST", "TRY_CAST"):
        run(f"f1/{fn}/{text}", lambda fn=fn, text=text: str(s.sql(f"SELECT {fn}('{text}' AS DECIMAL(10,2))").collect()[0][0]))
    n += 1
    t = f"lc.ns.f1_{n}"

    def store(t=t, text=text):
        s.sql(f"CREATE TABLE {t} (id INT, d DECIMAL(10,2)) USING iceberg").collect()
        s.sql(f"INSERT INTO {t} SELECT 1, '{text}'").collect()
        return [str(r[0]) for r in s.sql(f"SELECT d FROM {t}").collect()]

    run(f"f1/store/{text}", store)
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records))
