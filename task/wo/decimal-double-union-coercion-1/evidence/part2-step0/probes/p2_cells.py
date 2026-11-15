import json
import sys
import tempfile
from pathlib import Path

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"p2-{engine}-{ansi}-", dir=wh_parent)
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

    b = Builder.builder.appName("ddu-p2")
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


def frame_record(key: str, frame: object) -> dict:
    rows = frame.collect()
    types = [f.dataType.simpleString() for f in frame.schema.fields]
    return {"key": key, "types": types, "rows": [[shown(v) for v in r] for r in rows]}


def run(key: str, build: object) -> None:
    try:
        records.append(frame_record(key, build()))
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:300]}"})


TI_ROWS = [
    (1, "0.00", "0"),
    (2, "0.50", "0.5"),
    (3, "1.25", "1.25"),
    (4, "4.50", "0.500000000000000001"),
    (5, "-0.50", "-0.5"),
    (6, None, None),
    (7, "99999999.99", "99999999999999999999.999999999999999999"),
    (8, "0.10", "0.1"),
    (9, "0.01", "0.000000000000000001"),
    (10, "2.75", "0.100000000000000006"),
]


def ti_cell(text: str | None, kind: str) -> str:
    return f"CAST(NULL AS {kind})" if text is None else f"CAST('{text}' AS {kind})"


s.sql(
    "SELECT * FROM VALUES "
    + ", ".join(
        f"({i}, {ti_cell(a, 'DECIMAL(10,2)')}, {ti_cell(c, 'DECIMAL(38,18)')}, "
        f"CAST({i} AS DOUBLE) / 4)"
        for i, a, c in TI_ROWS
    )
    + " AS x(id, d10, d38, v)"
).createOrReplaceTempView("ti")
POOL = [
    "CAST(0.5 AS DOUBLE)",
    "CAST(1.25 AS DOUBLE)",
    "CAST('-0.0' AS DOUBLE)",
    "CAST(4.5 AS DOUBLE)",
    "CAST('0.505' AS DOUBLE)",
    "CAST('NaN' AS DOUBLE)",
    "CAST(-0.5 AS DOUBLE)",
    "CAST(0.1 AS DOUBLE)",
    "CAST(NULL AS DOUBLE)",
    "CAST('1e-18' AS DOUBLE)",
    "CAST('Infinity' AS DOUBLE)",
    "CAST(2.75 AS DOUBLE)",
    "CAST('1e30' AS DOUBLE)",
    "CAST('99999999.99' AS DOUBLE)",
    "CAST('0.0' AS DOUBLE)",
    "CAST('-Infinity' AS DOUBLE)",
    "CAST(0.01 AS DOUBLE)",
    "CAST('1e20' AS DOUBLE)",
]
NO_SPECIAL = [p for p in POOL if not any(x in p for x in ("NaN", "Inf", "NULL", "0.0'"))]
for column in ("d10", "d38"):
    for n in (1, 2, 3, 4, 5, 6, 8, 12, 20):
        for start in (0, 3, 7, 11):
            items = [POOL[(start + i) % len(POOL)] for i in range(n)]
            for neg in ("", "NOT "):
                key = f"in/{column}/{neg.strip().lower() or 'in'}/n{n}/s{start}"
                sql = f"SELECT id, {column} {neg}IN ({', '.join(items)}) AS p FROM ti ORDER BY id"
                run(key, lambda q=sql: s.sql(q))
        plain = [NO_SPECIAL[i % len(NO_SPECIAL)] for i in range(n)]
        for neg in ("", "NOT "):
            key = f"in/{column}/{neg.strip().lower() or 'in'}/n{n}/plain"
            sql = f"SELECT id, {column} {neg}IN ({', '.join(plain)}) AS p FROM ti ORDER BY id"
            run(key, lambda q=sql: s.sql(q))
    specials = {
        "mixed_dec_int": "CAST(0.5 AS DOUBLE), 1.25, 2, CAST(4.5 AS DOUBLE), 0.1",
        "mixed_dec_int_small": "CAST(0.5 AS DOUBLE), 1.25, 2",
        "with_column": "v, CAST(0.5 AS DOUBLE), CAST(1.25 AS DOUBLE), CAST(4.5 AS DOUBLE)",
        "null_only": "CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), "
        "CAST(NULL AS DOUBLE)",
        "nan_inf_only": "CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), "
        "CAST('-Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)",
        "zeros4": "CAST('-0.0' AS DOUBLE), CAST('0.0' AS DOUBLE), CAST(7.5 AS DOUBLE), "
        "CAST(8.5 AS DOUBLE)",
        "negzero4": "CAST('-0.0' AS DOUBLE), CAST(6.5 AS DOUBLE), CAST(7.5 AS DOUBLE), "
        "CAST(8.5 AS DOUBLE)",
        "negzero12": ", ".join(
            ["CAST('-0.0' AS DOUBLE)"] + [f"CAST({i}.5 AS DOUBLE)" for i in range(11)]
        ),
        "dup4": "CAST(0.5 AS DOUBLE), CAST(0.5 AS DOUBLE), CAST(0.5 AS DOUBLE), "
        "CAST(0.5 AS DOUBLE)",
        "float4": "CAST(0.5 AS FLOAT), CAST(1.25 AS FLOAT), CAST(0.1 AS FLOAT), CAST(4.5 AS FLOAT)",
        "exp_lit4": "5e-1, 1.25e0, 45e-1, 1e-1",
    }
    for name, items in specials.items():
        for neg in ("", "NOT "):
            key = f"in/{column}/{neg.strip().lower() or 'in'}/{name}"
            sql = f"SELECT id, {column} {neg}IN ({items}) AS p FROM ti ORDER BY id"
            run(key, lambda q=sql: s.sql(q))
for column in ("d10", "d38"):
    for n in (3, 4):
        items = ", ".join(NO_SPECIAL[:n])
        sql = f"EXPLAIN SELECT id FROM ti WHERE {column} IN ({items})"
        if engine == "repark":
            run(f"plan/{column}/in{n}", lambda q=sql: s.sql(q))

DML_SQL = {
    "dml_delete_in_values_v": "DELETE FROM {t} WHERE v IN (SELECT c FROM VALUES "
    "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE)) AS x(c))",
    "dml_delete_in_values_d": "DELETE FROM {t} WHERE d IN (SELECT c FROM VALUES "
    "(CAST(0.1 AS DECIMAL(10,2))), (CAST('0.10000000000000002' AS DOUBLE)) AS x(c))",
    "dml_update_in_values_v": "UPDATE {t} SET w = 9 WHERE v IN (SELECT c FROM VALUES "
    "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE)) AS x(c))",
    "dml_delete_in_bare_values_v": "DELETE FROM {t} WHERE v IN (VALUES "
    "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE)))",
    "dml_delete_exists_values_v": "DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM VALUES "
    "(CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE)) AS x(c) WHERE x.c = {t}.v)",
    "merge_values_src": "MERGE INTO {t} t USING (SELECT * FROM VALUES (1, 0.1), "
    "(2, CAST('1e-20' AS DOUBLE)), (3, CAST('-1e-20' AS DOUBLE)) AS s0(id, c)) s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET t.v = s.c",
    "merge_values_derived": "MERGE INTO {t} t USING (VALUES (1, 0.1), (2, CAST('1e-20' AS "
    "DOUBLE)), "
    "(3, CAST('-1e-20' AS DOUBLE))) AS s(id, c) ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v "
    "= s.c",
    "merge_union_src": "MERGE INTO {t} t USING (SELECT 1 AS id, 0.1 AS c UNION ALL SELECT 2, "
    "CAST('1e-20' AS DOUBLE) UNION ALL SELECT 3, CAST('-1e-20' AS DOUBLE)) s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET t.v = s.c",
    "merge_union_view": "MERGE INTO {t} t USING su s ON t.id = s.id WHEN MATCHED THEN UPDATE "
    "SET t.v = s.c",
    "merge_values_view": "MERGE INTO {t} t USING sv s ON t.id = s.id WHEN MATCHED THEN UPDATE "
    "SET t.v = s.c",
    "merge_values_insert": "MERGE INTO {t} t USING (SELECT * FROM VALUES (7, 0.1), "
    "(8, CAST('1e-20' AS DOUBLE)) AS s0(id, c)) s ON t.id = s.id "
    "WHEN NOT MATCHED THEN INSERT (id, d, v, w) VALUES (s.id, 0.0, s.c, 0)",
    "merge_values_on": "MERGE INTO {t} t USING (SELECT * FROM VALUES (0.1), (CAST('1e-20' AS "
    "DOUBLE)) "
    "AS s0(c)) s ON t.v = s.c WHEN MATCHED THEN DELETE",
}
s.sql(
    "SELECT 1 AS id, 0.1 AS c UNION ALL SELECT 2, CAST('1e-20' AS DOUBLE) UNION ALL SELECT 3, "
    "CAST('-1e-20' AS DOUBLE)"
).createOrReplaceTempView("su")
s.sql(
    "SELECT * FROM VALUES (1, 0.1), (2, CAST('1e-20' AS DOUBLE)), (3, CAST('-1e-20' AS DOUBLE)) "
    "AS s0(id, c)"
).createOrReplaceTempView("sv")
seq = 0
for mode, props in (("cow", ""), ("mor", MOR)):
    for key, template in DML_SQL.items():
        seq += 1
        t = f"lc.ns.t{seq}"
        s.sql(
            f"CREATE TABLE {t} (id BIGINT, d DECIMAL(10,2), v DOUBLE, w BIGINT) USING "
            f"iceberg{props}"
        ).collect()
        s.sql(
            f"INSERT INTO {t} VALUES (1, 0.10, CAST(0.1 AS DOUBLE), 1), "
            f"(2, 0.00, CAST('1e-20' AS DOUBLE), 2), (3, 0.00, CAST(0.0 AS DOUBLE), 3), "
            f"(4, 0.10, CAST('0.10000000000000002' AS DOUBLE), 4)"
        ).collect()
        rec_key = f"dml/{mode}/{key}"
        try:
            s.sql(template.format(t=t)).collect()
            records.append(frame_record(rec_key, s.sql(f"SELECT id, d, v, w FROM {t} ORDER BY id")))
        except Exception as error:
            records.append({"key": rec_key, "error": f"{type(error).__name__}: {str(error)[:300]}"})

frag_src = s.sql(
    "SELECT 1 AS id, CAST('0.1000000000000001' AS DOUBLE) AS q, CAST('1e-20' AS DOUBLE) AS z"
)
M1_IN = (
    "q IN (SELECT * FROM (VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))) AS t(c))"
)
M1_UNION = (
    "q IN (SELECT c FROM (SELECT CAST(0.1 AS DECIMAL(10,2)) AS c UNION ALL "
    "SELECT CAST('1e-20' AS DOUBLE)))"
)
M1_Z = "z IN (SELECT c FROM VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE)) AS t(c))"
M1_SCALAR = (
    "(SELECT min(c) FROM VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE)) AS t(c))"
)
M1_EXISTS = (
    "EXISTS (SELECT 1 FROM VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE)) AS t(c) "
    "WHERE t.c = z)"
)
FRAG = {
    "frag/fexpr_filter_in_values": lambda: frag_src.filter(sf.expr(M1_IN)).select("id"),
    "frag/fexpr_filter_in_union": lambda: frag_src.filter(sf.expr(M1_UNION)).select("id"),
    "frag/fexpr_filter_z_in_values": lambda: frag_src.filter(sf.expr(M1_Z)).select("id"),
    "frag/fexpr_filter_exists_values": lambda: frag_src.filter(sf.expr(M1_EXISTS)).select("id"),
    "frag/fexpr_select_scalar_values": lambda: frag_src.select(sf.expr(M1_SCALAR).alias("c")),
    "frag/fexpr_select_in_values": lambda: frag_src.select(sf.expr(M1_IN).alias("c")),
    "frag/filter_str_in_values": lambda: frag_src.filter(M1_IN).select("id"),
    "frag/filter_str_z_in_values": lambda: frag_src.filter(M1_Z).select("id"),
    "frag/filter_str_in_union": lambda: frag_src.filter(M1_UNION).select("id"),
    "frag/selectexpr_scalar_values": lambda: frag_src.selectExpr(M1_SCALAR + " AS c"),
    "frag/where_sql_in_values": lambda: s.sql(
        "SELECT id FROM (SELECT 1 AS id, CAST('0.1000000000000001' AS DOUBLE) AS q) WHERE " + M1_IN
    ),
}
for key, build in FRAG.items():
    run(key, build)

RANGE_VALUES = (
    "FROM VALUES (DATE '2024-01-01', CAST(0.1 AS DECIMAL(10,2))), "
    "(DATE '2024-01-02', CAST('1e-20' AS DOUBLE)), (DATE '2024-01-04', CAST('-0.0' AS DOUBLE)) "
    "AS t(dt, c)"
)
RANGE = {
    "range/bare_days": "SELECT dt, c, count(*) OVER (ORDER BY dt RANGE BETWEEN 1 PRECEDING AND "
    "CURRENT ROW) AS n " + RANGE_VALUES + " ORDER BY dt",
    "range/interval_days": "SELECT dt, c, count(*) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 1 DAY "
    "PRECEDING AND CURRENT ROW) AS n " + RANGE_VALUES + " ORDER BY dt",
    "range/bare_days_sum_c": "SELECT dt, sum(c) OVER (ORDER BY dt RANGE BETWEEN 1 PRECEDING AND "
    "CURRENT ROW) AS n " + RANGE_VALUES + " ORDER BY dt",
    "range/no_frame_control": "SELECT dt, c " + RANGE_VALUES + " ORDER BY dt",
}
for key, sql in RANGE.items():
    run(key, lambda q=sql: s.sql(q))

TRIP = {
    "trip/values_user_cast_both": "SELECT c FROM VALUES (CAST(0.1 AS DECIMAL(30,15))), "
    "(CAST(CAST('1e-20' AS DOUBLE) AS DECIMAL(30,15))) AS t(c)",
    "trip/values_all_double": "SELECT c FROM VALUES (CAST(0.1 AS DOUBLE)), (CAST('1e-20' AS "
    "DOUBLE)) AS t(c)",
    "trip/values_all_decimal": "SELECT c FROM VALUES (0.1), (CAST(2 AS DECIMAL(10,2))) AS t(c)",
    "trip/values_single_row": "SELECT c, e FROM VALUES (0.1, CAST('1e-20' AS DOUBLE)) AS t(c, e)",
    "trip/values_int_double": "SELECT c FROM VALUES (1), (CAST('1e-20' AS DOUBLE)) AS t(c)",
    "trip/values_dec_float": "SELECT c FROM VALUES (0.1), (CAST('1e-20' AS FLOAT)) AS t(c)",
    "trip/values_null_double_dec": "SELECT c FROM VALUES (NULL), (0.1), (CAST('1e-20' AS "
    "DOUBLE)) AS t(c)",
    "trip/cte_values": "WITH x AS (SELECT * FROM VALUES (0.1), (CAST('1e-20' AS DOUBLE)) AS t(c)) "
    "SELECT c FROM x",
    "trip/in_subquery_values": "SELECT CAST('1e-20' AS DOUBLE) IN (SELECT c FROM VALUES (0.1), "
    "(CAST('1e-20' AS DOUBLE)) AS t(c)) AS c",
    "trip/values_10k_one_double": "SELECT count(*), count(DISTINCT b) FROM VALUES "
    + ", ".join(f"({i}, 0.5)" for i in range(999))
    + ", (999, CAST('1e-20' AS DOUBLE)) AS x(a, b)",
}
for key, sql in TRIP.items():
    run(key, lambda q=sql: s.sql(q))

B_DEC = "SELECT 1 AS id, 0.1 AS c"
B_DBL = [
    "SELECT 2, CAST('1e-20' AS DOUBLE)",
    "SELECT 3, CAST('-1e-20' AS DOUBLE)",
    "SELECT 4, CAST('2e-20' AS DOUBLE)",
]
B_DEC2 = "SELECT 5, CAST(0.25 AS DECIMAL(10,2))"
UNIONS = {
    "union/all3_dec_first": " UNION ALL ".join([B_DEC, *B_DBL[:2]]),
    "union/all3_dec_last": " UNION ALL ".join([*B_DBL[:2], B_DEC]),
    "union/all3_dec_middle": " UNION ALL ".join([B_DBL[0], B_DEC, B_DBL[1]]),
    "union/all4_dec_first": " UNION ALL ".join([B_DEC, *B_DBL]),
    "union/distinct3_dec_first": " UNION ".join([B_DEC, *B_DBL[:2]]),
    "union/all3_two_dec_first": " UNION ALL ".join([B_DEC, B_DEC2, B_DBL[0]]),
    "union/all3_paren_right": B_DEC + " UNION ALL (" + " UNION ALL ".join(B_DBL[:2]) + ")",
    "union/all2_control": " UNION ALL ".join([B_DEC, B_DBL[0]]),
    "union/all3_in_subquery": "SELECT count(*) FROM ("
    + " UNION ALL ".join([B_DEC, *B_DBL[:2]])
    + ") WHERE c <> 0",
}
for key, sql in UNIONS.items():
    run(key, lambda q=sql: s.sql(q + (" ORDER BY 1" if not q.startswith("SELECT count") else "")))
frag_u3 = "SELECT c FROM (" + " UNION ALL ".join([B_DEC, *B_DBL[:2]]) + ")"
run(
    "union/fexpr_scalar_min_all3",
    lambda: frag_src.select(sf.expr(f"(SELECT min(c) FROM ({frag_u3}))").alias("c")),
)

INS = {
    "ins/values_all_double_into_dec": (
        "DECIMAL(10,2)",
        "(CAST(1.255 AS DOUBLE)), (CAST(2.5 AS DOUBLE))",
    ),
    "ins/values_mixed_into_dec": ("DECIMAL(10,2)", "(1.255), (CAST(1.255 AS DOUBLE))"),
    "ins/values_mixed_into_double": ("DOUBLE", "(0.1), (CAST('1e-20' AS DOUBLE))"),
    "ins/values_mixed_into_dec38": ("DECIMAL(38,18)", "(0.1), (CAST('1e-20' AS DOUBLE))"),
}
for seq, (key, (kind, rows)) in enumerate(INS.items(), start=1):
    t = f"lc.ns.i{seq}"
    try:
        s.sql(f"CREATE TABLE {t} (c {kind}) USING iceberg").collect()
        s.sql(f"INSERT INTO {t} VALUES {rows}").collect()
        records.append(frame_record(key, s.sql(f"SELECT c FROM {t} ORDER BY c")))
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:300]}"})

with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r) + "\n")
print(engine, ansi, len(records))
