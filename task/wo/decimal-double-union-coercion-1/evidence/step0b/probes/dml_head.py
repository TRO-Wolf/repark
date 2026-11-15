import json
import sys
import tempfile
from pathlib import Path

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"dml-{engine}-{ansi}-", dir=wh_parent)
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
MOR = (
    " TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')"
)
if engine == "native":
    import repark
    import repark._native as native
    from repark._native import PyReparkSession

    session = PyReparkSession.native(config={"spark.sql.ansi.enabled": ansi})
    session.register_memory_catalog("lc", wh)
    repark._ANSI_NATIVE = session
    location = Path(wh) / "ns"
    location.mkdir()
    repark.sql(f"CREATE SCHEMA lc.ns WITH (location = '{location}')").collect()
    sql = repark.sql
    using = ""
    modes = {"cow": ""}
    prov = {"engine": "native", "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1]}
else:
    if engine == "spark":
        import pyspark
        from pyspark.sql import SparkSession as Builder

        b = (
            Builder.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
            .config("spark.sql.extensions", EXT)
            .config("spark.sql.shuffle.partitions", "1")
        )
        prov = {"engine": "spark", "version": pyspark.__version__}
    else:
        import repark._native as native
        from repark import ReparkSession as Builder

        b = Builder.builder.appName("ddu-dml")
        prov = {"engine": "repark", "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1]}
    s = (
        b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", wh)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )
    s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
    sql = s.sql
    using = " USING iceberg"
    modes = {"cow": "", "mor": MOR}
TABLES = {
    "t": (
        "DECIMAL(10,2)",
        "(1, CAST(1.00 AS DECIMAL(10,2)), CAST('1.0000000000000004' AS DOUBLE)), "
        "(2, CAST(1.47 AS DECIMAL(10,2)), CAST('1.4700000000000004' AS DOUBLE)), "
        "(3, CAST(2.23 AS DECIMAL(10,2)), CAST('2.2300000000000004' AS DOUBLE)), "
        "(4, CAST(1.50 AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE))",
    ),
    "u": (
        "DECIMAL(38,18)",
        "(1, CAST('-22.129547339983247' AS DECIMAL(38,18)), CAST('-22.129547339983247' AS DOUBLE)), "
        "(2, CAST(1.5 AS DECIMAL(38,18)), CAST('1.5' AS DOUBLE)), "
        "(3, CAST('1.0000000000000004' AS DECIMAL(38,18)), CAST('1.0000000000000004' AS DOUBLE))",
    ),
    "z": (
        "DECIMAL(10,2)",
        "(1, CAST(0 AS DECIMAL(10,2)), CAST('-0.0' AS DOUBLE)), "
        "(2, CAST(0.50 AS DECIMAL(10,2)), CAST('0.5' AS DOUBLE)), "
        "(3, CAST(0 AS DECIMAL(10,2)), CAST('1e-20' AS DOUBLE))",
    ),
}
SELECTS = {
    "select_eq": "SELECT id FROM {t} WHERE d = v",
    "select_lt": "SELECT id FROM {t} WHERE d < v",
    "select_ns": "SELECT id FROM {t} WHERE d <=> v",
    "select_in": "SELECT id FROM {t} WHERE v IN (d)",
    "select_between": "SELECT id FROM {t} WHERE v BETWEEN d AND d",
}
STATEMENTS = {
    "delete_eq": "DELETE FROM {t} WHERE d = v",
    "update_eq": "UPDATE {t} SET id = id + 100 WHERE d = v",
    "update_in": "UPDATE {t} SET id = id + 100 WHERE v IN (d)",
    "merge_on_dv": (
        "MERGE INTO {t} t USING (SELECT id AS sid, v AS sv FROM {s}) s ON t.d = s.sv "
        "WHEN MATCHED THEN UPDATE SET id = t.id + 100"
    ),
    "merge_on_vd": (
        "MERGE INTO {t} t USING (SELECT id AS sid, d AS sd FROM {s}) s ON t.v = s.sd "
        "WHEN MATCHED THEN UPDATE SET id = t.id + 100"
    ),
    "merge_on_key_and": (
        "MERGE INTO {t} t USING (SELECT id AS sid, v AS sv FROM {s}) s ON t.id = s.sid AND t.d = s.sv "
        "WHEN MATCHED THEN UPDATE SET id = t.id + 100 "
        "WHEN NOT MATCHED THEN INSERT (id, d, v) VALUES (s.sid + 50, 0, 0)"
    ),
    "merge_delete": (
        "MERGE INTO {t} t USING (SELECT id AS sid, v AS sv FROM {s}) s ON t.d = s.sv "
        "WHEN MATCHED THEN DELETE"
    ),
    "merge_by_source": (
        "MERGE INTO {t} t USING (SELECT id AS sid, v AS sv FROM {s}) s ON t.d = s.sv "
        "WHEN NOT MATCHED BY SOURCE THEN DELETE"
    ),
}
FLIP = ("-22.129547339983247", "1.0000000000000004", "2.2300000000000004")
X = "CAST(CAST({} AS STRING) AS DOUBLE)"
SUBS = (
    ("t.d = s.sv", X.format("t.d") + " = s.sv"),
    ("t.v = s.sd", "t.v = " + X.format("s.sd")),
    ("WHERE d = v", "WHERE " + X.format("d") + " = v"),
    ("WHERE d < v", "WHERE " + X.format("d") + " < v"),
    ("WHERE d <=> v", "WHERE " + X.format("d") + " <=> v"),
    ("v IN (d)", "v IN (" + X.format("d") + ")"),
    ("BETWEEN d AND d", "BETWEEN " + X.format("d") + " AND " + X.format("d")),
)
for group in (SELECTS, STATEMENTS):
    for name, text in group.items():
        for a, b in SUBS:
            text = text.replace(a, b)
        group[name] = text
records = [dict(key="provenance", ansi=ansi, warehouse=Path(wh).name, **prov)]


def err(e: Exception) -> dict:
    java = getattr(e, "java_exception", None)
    head = str(java.toString()) if java is not None else str(e)
    lines = head.strip().splitlines()
    return {"error": type(e).__name__, "head": lines[0][:240] if lines else ""}


for v in FLIP:
    d = f"CAST('{v}' AS DOUBLE)"
    k1 = f"CAST({d} AS STRING)"
    shapes = {
        "c1": f"ROUND({d}, 18) = {d}",
        "c3": X.format(f"CAST({k1} AS DECIMAL(38,18))") + f" = {d}",
        "c5": X.format(f"CAST('{v}' AS DECIMAL(38,18))") + f" = {d}",
        "c6": X.format(f"CAST(CAST(ROUND({d}, 2) AS STRING) AS DECIMAL(10,2))") + f" = {d}",
        "c7": X.format(f"CAST(CAST(ROUND({d}, 18) AS STRING) AS DECIMAL(38,18))") + f" = {d}",
    }
    for name, expr in shapes.items():
        rec = {"key": f"flip/{name}/{v}"}
        try:
            rec["answer"] = str(sql(f"SELECT CAST({expr} AS STRING) AS c").collect()[0][0])
        except Exception as e:
            rec.update(err(e))
        records.append(rec)
n = 0
for tname, (dtype, rows) in TABLES.items():
    for mode, props in modes.items():
        cells = [(k, q, True) for k, q in SELECTS.items()] + [
            (k, q, False) for k, q in STATEMENTS.items()
        ]
        for key, stmt, is_select in cells:
            n += 1
            t, src = f"lc.ns.x{n:03d}", f"lc.ns.s{n:03d}"
            rec = {"key": f"{tname}/{mode}/{key}", "sql": stmt.format(t=t, s=src)}
            try:
                sql(f"CREATE TABLE {t} (id INT, d {dtype}, v DOUBLE){using}{props}").collect()
                sql(f"INSERT INTO {t} VALUES {rows}").collect()
                if "{s}" in stmt:
                    sql(f"CREATE TABLE {src} (id INT, d {dtype}, v DOUBLE){using}").collect()
                    sql(f"INSERT INTO {src} VALUES {rows}").collect()
                if is_select:
                    got = sql(stmt.format(t=t, s=src)).collect()
                else:
                    sql(stmt.format(t=t, s=src)).collect()
                    got = sql(f"SELECT id FROM {t}").collect()
                rec["ids"] = sorted(int(r[0]) for r in got)
            except Exception as e:
                rec.update(err(e))
            records.append(rec)
CONTROLS = {
    "merge_by_source_dupkey": (
        "MERGE INTO {t} t USING (SELECT 1 AS sid UNION ALL SELECT 1) s ON t.id = s.sid "
        "WHEN NOT MATCHED BY SOURCE THEN DELETE"
    ),
    "merge_by_source_onekey": (
        "MERGE INTO {t} t USING (SELECT 1 AS sid) s ON t.id = s.sid "
        "WHEN NOT MATCHED BY SOURCE THEN DELETE"
    ),
}
for mode, props in modes.items():
    for key, stmt in CONTROLS.items():
        n += 1
        t = f"lc.ns.x{n:03d}"
        rec = {"key": f"ctl/{mode}/{key}", "sql": stmt.format(t=t)}
        try:
            sql(f"CREATE TABLE {t} (id INT, d DECIMAL(10,2), v DOUBLE){using}{props}").collect()
            sql(f"INSERT INTO {t} VALUES {TABLES['t'][1]}").collect()
            sql(stmt.format(t=t)).collect()
            rec["ids"] = sorted(int(r[0]) for r in sql(f"SELECT id FROM {t}").collect())
        except Exception as e:
            rec.update(err(e))
        records.append(rec)
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records))
