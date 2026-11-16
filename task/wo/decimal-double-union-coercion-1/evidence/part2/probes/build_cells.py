import json
import sys
import tempfile
from pathlib import Path
from typing import Any

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"p2b-{engine}-{ansi}-", dir=wh_parent)
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
MOR = (
    " TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')"
)
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

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
    from repark import functions as sf

    b = Builder.builder.appName("ddu-p2-build")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
s = (
    b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
prov["warehouse"] = Path(wh).name
prov["ansi"] = ansi
records = [{"key": "provenance", **prov}]


def shown(value: object) -> str:
    return "NULL" if value is None else str(value)


def frame_record(key: str, frame: Any) -> dict:
    rows = frame.collect()
    types = [f.dataType.simpleString() for f in frame.schema.fields]
    return {"key": key, "types": types, "rows": [[shown(v) for v in r] for r in rows]}


def run(key: str, build: Any) -> None:
    try:
        records.append(frame_record(key, build()))
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:300]}"})


VALS = "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))"
DEC = "CAST(0.1 AS DECIMAL(10,2))"
TINY = "CAST('1e-20' AS DOUBLE)"
NEG = "CAST('-1e-20' AS DOUBLE)"
SQL = {
    "union/right_deep3": f"SELECT 1 AS id, {DEC} AS c UNION ALL (SELECT 2, {TINY} UNION ALL "
    f"SELECT 3, {NEG})",
    "union/mixed_nest4": f"SELECT 1 AS id, {DEC} AS c UNION ALL SELECT 2, {TINY} UNION "
    f"(SELECT 3, {NEG} UNION ALL SELECT 4, CAST(0.25 AS DECIMAL(10,2)))",
    "union/all5_dec_first": f"SELECT 1 AS id, {DEC} AS c UNION ALL SELECT 2, {TINY} UNION ALL "
    f"SELECT 3, {NEG} UNION ALL SELECT 4, CAST('2e-20' AS DOUBLE) UNION ALL SELECT 5, "
    "CAST(0.25 AS DECIMAL(10,2))",
    "union/all3_cte": f"WITH u AS (SELECT 1 AS id, {DEC} AS c UNION ALL SELECT 2, {TINY} "
    f"UNION ALL SELECT 3, {NEG}) SELECT id, c FROM u",
    "union/all3_view": "SELECT id, c FROM uv3",
    "setop/intersect": f"SELECT {TINY} AS c INTERSECT SELECT c FROM VALUES {VALS} AS t(c)",
    "setop/intersect_all": f"SELECT {TINY} AS c INTERSECT ALL SELECT c FROM VALUES {VALS} AS t(c)",
    "setop/except": f"SELECT c FROM VALUES {VALS} AS t(c) EXCEPT SELECT {TINY} AS c",
    "setop/except_all": f"SELECT c FROM VALUES {VALS} AS t(c) EXCEPT ALL SELECT {TINY} AS c",
    "setop/values_leg_first_intersect": f"SELECT c FROM VALUES {VALS} AS t(c) INTERSECT "
    f"SELECT {TINY}",
    "values/standalone": f"SELECT c FROM VALUES {VALS} AS t(c)",
    "values/two_cols": f"SELECT id, c FROM VALUES (1, {DEC}), (2, {TINY}) AS t(id, c)",
    "values/null_first": f"SELECT c FROM VALUES (NULL), (0.1), ({TINY}) AS t(c)",
    "values/int_dec_double": f"SELECT c FROM VALUES (1), (0.5), ({TINY}) AS t(c)",
    "values/float": "SELECT c FROM VALUES (0.1), (CAST('1e-20' AS FLOAT)) AS t(c)",
    "values/user_cast_both": "SELECT c FROM VALUES (CAST(0.1 AS DECIMAL(30,15))), "
    "(CAST(CAST('1e-20' AS DOUBLE) AS DECIMAL(30,15))) AS t(c)",
    "values/user_cast_dec_beside_dec": "SELECT c FROM VALUES (CAST(0.5 AS DECIMAL(10,2))), "
    "(CAST(CAST('1e-20' AS DOUBLE) AS DECIMAL(10,2))), (1.5) AS t(c)",
    "values/string_beside": f"SELECT c FROM VALUES ('a'), ({DEC}) AS t(c)",
    "values/cte_in": f"WITH v AS (SELECT c FROM VALUES {VALS} AS t(c)) SELECT 1 AS id WHERE "
    f"{TINY} IN (SELECT c FROM v)",
    "values/rcte_anchor": "WITH RECURSIVE r(n, c) AS (SELECT * FROM VALUES (1, "
    "CAST(0.5 AS DECIMAL(10,2))), (2, CAST(1.5 AS DECIMAL(10,2))) AS t(n, c) UNION ALL "
    "SELECT n + 2, c FROM r WHERE n < 3) SELECT n, c FROM r ORDER BY n",
    "values/scalar_min": f"SELECT (SELECT min(c) FROM VALUES {VALS} AS t(c)) AS m",
    "zero/in_zero": f"SELECT 1 AS id WHERE CAST(0.0 AS DOUBLE) IN (SELECT c FROM VALUES {VALS} "
    "AS t(c))",
    "zero/in_2e16": f"SELECT 1 AS id WHERE CAST('2e-16' AS DOUBLE) IN (SELECT c FROM VALUES "
    f"{VALS} AS t(c))",
    "zero/in_1e16": f"SELECT 1 AS id WHERE CAST('1e-16' AS DOUBLE) IN (SELECT c FROM VALUES "
    f"{VALS} AS t(c))",
    "explain/values": f"EXPLAIN SELECT c FROM VALUES {VALS} AS t(c)",
    "explain/extended_values": f"EXPLAIN EXTENDED SELECT c FROM VALUES {VALS} AS t(c)",
    "explain/control": f"EXPLAIN SELECT {DEC} AS c",
}
s.sql(
    f"SELECT 1 AS id, {DEC} AS c UNION ALL SELECT 2, {TINY} UNION ALL SELECT 3, {NEG}"
).createOrReplaceTempView("uv3")
for key, sql in SQL.items():
    if key.startswith("explain/"):
        if engine == "spark":
            continue
        run(key, lambda q=sql: s.sql(q))
        continue
    run(key, lambda q=sql: s.sql(q))

base = s.sql(f"SELECT 1 AS id, {TINY} AS z")
dfv = s.sql(f"SELECT c FROM VALUES {VALS} AS t(c)")
run("df/values_filter_eq", lambda: dfv.filter(sf.col("c") == sf.lit(1e-20)))
run("df/values_filter_str", lambda: dfv.filter("c = CAST('1e-20' AS DOUBLE)"))
run("df/values_join", lambda: base.join(dfv, base.z == dfv.c).select("id"))
run("df/values_union_double", lambda: dfv.union(s.sql(f"SELECT {TINY} AS c")))
run("df/values_select", lambda: dfv.select((sf.col("c") * 2).alias("c2")))

M1_Z = f"z IN (SELECT c FROM VALUES {VALS} AS t(c))"
M1_EX = f"EXISTS (SELECT 1 FROM VALUES {VALS} AS t(c) WHERE t.c = z)"
LIT_ROWS = (
    "SELECT * FROM VALUES (1, CAST('0.1' AS DOUBLE)), (2, CAST('0.30000000000000004' AS DOUBLE)), "
    "(3, CAST('0.2' AS DOUBLE)) AS t(id, v)"
)
LIT = {
    "cmp": "v = 0.1",
    "in": "v IN (0.1, 0.3)",
    "between": "v BETWEEN 0.1 AND 0.2",
    "case": "CASE v WHEN 0.1 THEN 'a' WHEN 0.3 THEN 'b' ELSE 'c' END",
    "cmp_typed": "v = 0.1BD",
    "in_typed": "v IN (0.1BD, 0.3BD)",
}
lit_src = s.sql(LIT_ROWS)
for name, text in LIT.items():
    run(f"lit/spark/{name}", lambda t=text: lit_src.select("id", sf.expr(t).alias("p")))

with Path(out).open("w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")

if engine == "repark":
    import repark
    from repark._native import PyReparkSession

    native = PyReparkSession.native(config={"spark.sql.ansi.enabled": ansi})
    repark._ANSI_NATIVE = native
    nbase = repark.sql(f"SELECT 1 AS id, {TINY} AS z")
    nlit = repark.sql(LIT_ROWS)
    native_cells = {
        "native/fexpr_in_values": lambda: nbase.filter(sf.expr(M1_Z)).select("id"),
        "native/filter_str_in_values": lambda: nbase.filter(M1_Z).select("id"),
        "native/fexpr_exists_values": lambda: nbase.filter(sf.expr(M1_EX)).select("id"),
        "native/fexpr_select_in_values": lambda: nbase.select(sf.expr(M1_Z).alias("p")),
        "native/fexpr_scalar_values": lambda: nbase.select(
            sf.expr(f"(SELECT min(c) FROM VALUES {VALS} AS t(c))").alias("m")
        ),
        "native/explain_fexpr": lambda: nbase.filter(sf.expr(M1_Z)).select("id"),
    }
    for key, build in native_cells.items():
        if key == "native/explain_fexpr":
            try:
                build().explain()
                records.append({"key": key, "rows": [["planned"]], "types": ["string"]})
            except Exception as error:
                records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:300]}"})
            continue
        run(key, build)
    for name, text in LIT.items():
        run(f"lit/native/{name}", lambda t=text: nlit.select("id", sf.expr(t).alias("p")))

DML = {
    "dml/delete_in_values_v": "DELETE FROM {t} WHERE v IN (SELECT c FROM VALUES "
    + VALS
    + " AS x(c))",
    "dml/update_in_values_v": "UPDATE {t} SET w = 9 WHERE v IN (SELECT c FROM VALUES "
    + VALS
    + " AS x(c))",
    "dml/delete_in_table": "DELETE FROM {t} WHERE id IN (SELECT id FROM lc.ns.src)",
    "dml/delete_exists_table": "DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM lc.ns.src s WHERE "
    "s.id = {t}.id)",
    "dml/merge_union_src": "MERGE INTO {t} t USING (SELECT 1 AS id, 0.1 AS c UNION ALL SELECT 2, "
    f"{TINY} UNION ALL SELECT 3, {NEG}) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.c",
    "dml/merge_union_right_deep": "MERGE INTO {t} t USING (SELECT 1 AS id, 0.1 AS c UNION ALL "
    f"(SELECT 2, {TINY} UNION ALL SELECT 3, {NEG})) s ON t.id = s.id WHEN MATCHED THEN UPDATE "
    "SET t.v = s.c",
    "dml/merge_values_src": "MERGE INTO {t} t USING (SELECT * FROM VALUES (1, 0.1), "
    f"(2, {TINY}), (3, {NEG}) AS s0(id, c)) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = "
    "s.c",
    "dml/merge_values_insert": "MERGE INTO {t} t USING (SELECT * FROM VALUES (7, 0.1), "
    f"(8, {TINY}) AS s0(id, c)) s ON t.id = s.id "
    "WHEN NOT MATCHED THEN INSERT (id, d, v, w) VALUES (s.id, 0.0, s.c, 0)",
    "dml/merge_values_on": "MERGE INTO {t} t USING (SELECT * FROM VALUES (0.1), "
    f"({TINY}) AS s0(c)) s ON t.v = s.c WHEN MATCHED THEN DELETE",
    "dml/merge_values_view": "MERGE INTO {t} t USING sv s ON t.id = s.id WHEN MATCHED THEN UPDATE "
    "SET t.v = s.c",
}
s.sql(
    f"SELECT * FROM VALUES (1, 0.1), (2, {TINY}), (3, {NEG}) AS s0(id, c)"
).createOrReplaceTempView("sv")
s.sql("CREATE TABLE lc.ns.src (id BIGINT) USING iceberg").collect()
s.sql("INSERT INTO lc.ns.src VALUES (2), (3)").collect()
seq = 0
for mode, props in (("cow", ""), ("mor", MOR)):
    for key, template in DML.items():
        seq += 1
        t = f"lc.ns.t{seq}"
        s.sql(
            f"CREATE TABLE {t} (id BIGINT, d DECIMAL(10,2), v DOUBLE, w BIGINT) USING "
            f"iceberg{props}"
        ).collect()
        s.sql(
            f"INSERT INTO {t} VALUES (1, 0.10, CAST(0.1 AS DOUBLE), 1), "
            f"(2, 0.00, {TINY}, 2), (3, 0.00, CAST(0.0 AS DOUBLE), 3), "
            f"(4, 0.10, CAST('0.10000000000000002' AS DOUBLE), 4)"
        ).collect()
        rec_key = f"{key}/{mode}"
        try:
            s.sql(template.format(t=t)).collect()
            records.append(frame_record(rec_key, s.sql(f"SELECT id, d, v, w FROM {t} ORDER BY id")))
        except Exception as error:
            records.append({"key": rec_key, "error": f"{type(error).__name__}: {str(error)[:200]}"})

WRITES = {
    "write/insert_dec38": ("DECIMAL(38,18)", f"INSERT INTO {{t}} VALUES (0.1), ({TINY})"),
    "write/insert_string": ("STRING", f"INSERT INTO {{t}} VALUES (0.1), ({TINY})"),
    "write/insert_double": ("DOUBLE", f"INSERT INTO {{t}} VALUES (0.1), ({TINY})"),
    "write/insert_dec10": (
        "DECIMAL(10,2)",
        "INSERT INTO {t} VALUES (1.255), (CAST(1.255 AS DOUBLE))",
    ),
    "write/insert_select_values": (
        "STRING",
        f"INSERT INTO {{t}} SELECT c FROM VALUES (0.1), ({TINY}) AS v(c)",
    ),
    "write/overwrite_values": ("DOUBLE", f"INSERT OVERWRITE {{t}} VALUES (0.1), ({TINY})"),
    "write/overwrite_empty_values": (
        "DOUBLE",
        f"INSERT OVERWRITE {{t}} SELECT c FROM VALUES (0.1), ({TINY}) AS v(c) WHERE c > 1",
    ),
    "write/user_cast_into_dec": (
        "DECIMAL(10,2)",
        "INSERT INTO {t} VALUES (CAST(CAST(1.255 AS DOUBLE) AS DECIMAL(10,2))), (1.5)",
    ),
}
for key, (column_type, template) in WRITES.items():
    seq += 1
    t = f"lc.ns.w{seq}"
    s.sql(f"CREATE TABLE {t} (c {column_type}) USING iceberg").collect()
    if key == "write/overwrite_empty_values":
        s.sql(f"INSERT INTO {t} VALUES (CAST(7 AS DOUBLE))").collect()
    try:
        s.sql(template.format(t=t)).collect()
        records.append(frame_record(key, s.sql(f"SELECT c FROM {t} ORDER BY c")))
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:200]}"})
seq += 1
run(
    "write/ctas_values",
    lambda: (
        s.sql(
            f"CREATE TABLE lc.ns.w{seq} USING iceberg AS SELECT c FROM VALUES {VALS} AS v(c)"
        ).collect(),
        s.sql(f"SELECT c FROM lc.ns.w{seq} ORDER BY c"),
    )[1],
)

with Path(out).open("w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")
print("wrote", out, len(records))
