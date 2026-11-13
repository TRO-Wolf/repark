import sys
import tempfile
from pathlib import Path

from common import error_record, run_frame, write_jsonl
from revision_cells import MOR, PRUNE, PRUNE_SEED, SELECTS, THREE_SEED, WRITES, ZERO_SEED

engine, ansi, wh_parent, out, jar = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
wh = tempfile.mkdtemp(prefix=f"rev-{engine}-{ansi}-", dir=wh_parent)
if engine == "repark":
    import repark._native as native
    from repark import ReparkSession as Builder

    b = Builder.builder.appName("ddrev")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
else:
    import pyspark
    from pyspark.sql import SparkSession as Builder

    b = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.shuffle.partitions", "1")
    )
    prov = {"engine": "spark", "version": pyspark.__version__, "jar": Path(jar).name}
s = (
    b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
records = [dict(key="provenance", ansi=ansi, warehouse=Path(wh).name, **prov)]
for name, sql in SELECTS:
    rec = {"key": f"select/{name}", "sql": sql}
    try:
        rec.update(run_frame(s.sql(sql)))
    except Exception as e:
        rec.update(error_record(e))
    records.append(rec)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns")
for i, (name, schema, seed, sql) in enumerate(WRITES):
    t = f"lc.ns.r{i:02d}"
    rec = {"key": name, "table": t, "target": schema, "sql": sql.format(t=t)}
    try:
        if schema != "rtas":
            cols, _, mode = schema.partition("|")
            props = MOR if mode == "mor" else ""
            s.sql(f"CREATE TABLE {t} ({cols}) USING iceberg{props}").collect()
        if seed == "seed":
            s.sql(f"INSERT INTO {t} VALUES (7, 9), (8, 9)").collect()
        elif seed == "three":
            s.sql(THREE_SEED.format(t=t)).collect()
        elif seed == "zero":
            s.sql(ZERO_SEED.format(t=t)).collect()
        if sql.startswith("DF-"):
            frame = s.sql("SELECT 1 AS id, 0.1 AS v").union(
                s.sql("SELECT 2 AS id, CAST(1e-20 AS DOUBLE) AS v")
            )
            if sql == "DF-INSERTINTO":
                frame.write.insertInto(t)
            else:
                frame.writeTo(t).overwritePartitions()
        else:
            s.sql(sql.format(t=t)).collect()
        rec["write"] = "ok"
    except Exception as e:
        rec["write"] = error_record(e)
    try:
        frame = s.sql(f"SELECT * FROM {t} ORDER BY id")
        rec["readback"] = run_frame(frame)
        rec["rows"] = [[str(x) for x in r] for r in frame.collect()]
    except Exception as e:
        rec["readback"] = error_record(e)
    records.append(rec)
if engine == "repark" and ansi == "true":
    t = "lc.ns.prune"
    s.sql(f"CREATE TABLE {t} (id INT, d DECIMAL(10,2), v DOUBLE) USING iceberg").collect()
    for stmt in PRUNE_SEED:
        s.sql(stmt.format(t=t)).collect()
    for name, sql in PRUNE:
        rec = {"key": f"explain/{name}", "sql": sql.format(t=t)}
        try:
            rows = s.sql("EXPLAIN " + sql.format(t=t)).collect()
            rec["plan"] = [str(r[-1]) for r in rows]
            rec["ids"] = sorted(str(r[0]) for r in s.sql(sql.format(t=t)).collect())
        except Exception as e:
            rec.update(error_record(e))
        records.append(rec)
write_jsonl(out, records)
print(engine, ansi, Path(wh).name, len(records))
