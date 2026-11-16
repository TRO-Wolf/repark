import json
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"delta-{engine}-{ansi}-", dir=wh_parent)
EXT = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
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

    b = Builder.builder.appName("ddu-delta")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
    print("native", native.__file__)
s = (
    b.config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lc.type", "hadoop")
    .config("spark.sql.catalog.lc.warehouse", wh)
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
prov["ansi"] = ansi
records = [{"key": "provenance", **prov}]

V = "VALUES (CAST(0.1 AS DECIMAL(10,2))), (CAST('1e-20' AS DOUBLE))"
Z = "CAST('1e-20' AS DOUBLE)"
PROBES = {
    "tiny": "CAST('1e-20' AS DOUBLE)",
    "zero": "CAST(0.0 AS DOUBLE)",
    "negzero": "CAST('-0.0' AS DOUBLE)",
    "dec01": "CAST(0.1 AS DOUBLE)",
}
PREDICATES = {
    "in": "{z} IN (SELECT c FROM " + V + " AS t(c))",
    "notin": "{z} NOT IN (SELECT c FROM " + V + " AS t(c))",
    "exists": "EXISTS (SELECT 1 FROM " + V + " AS t(c) WHERE t.c = {z})",
    "notexists": "NOT EXISTS (SELECT 1 FROM " + V + " AS t(c) WHERE t.c = {z})",
    "scalar": "{z} = (SELECT min(c) FROM " + V + " AS t(c))",
}


def shown(value: object) -> str:
    return "NULL" if value is None else str(value)


def frame_record(key: str, frame: Any) -> dict:
    rows = frame.collect()
    types = [f.dataType.simpleString() for f in frame.schema.fields]
    return {"key": key, "types": types, "rows": [[shown(v) for v in r] for r in rows]}


def run(key: str, build: Callable[[], Any]) -> None:
    try:
        records.append(frame_record(key, build()))
    except Exception as error:
        records.append({"key": key, "error": f"{type(error).__name__}: {str(error)[:400]}"})


def sql(key: str, text: str) -> None:
    run(key, lambda: s.sql(text))


def fragment_seats(prefix: str, session_sql: Callable[[str], Any], functions: Any) -> None:
    for pname, ptext in PREDICATES.items():
        for zname, ztext in PROBES.items():
            text = ptext.format(z=ztext)
            key = f"{prefix}/{pname}/{zname}"
            run(
                f"{key}/fexpr_filter",
                lambda t=text: session_sql("SELECT 1 AS id").filter(functions.expr(t)),
            )
            run(
                f"{key}/fexpr_select",
                lambda t=text: session_sql("SELECT 1 AS id").select(functions.expr(t).alias("p")),
            )
            run(
                f"{key}/fexpr_withcolumn",
                lambda t=text: session_sql("SELECT 1 AS id").withColumn("p", functions.expr(t)),
            )
            run(
                f"{key}/selectexpr",
                lambda t=text: session_sql("SELECT 1 AS id").selectExpr(t + " AS p"),
            )
            run(f"{key}/filter_str", lambda t=text: session_sql("SELECT 1 AS id").filter(t))


fragment_seats("f3", s.sql, sf)
sql("f3/pin/sql_in", f"SELECT 1 AS id WHERE {Z} IN (SELECT c FROM {V} AS t(c))")
sql("f3/pin/sql_exists", f"SELECT 1 AS id WHERE EXISTS (SELECT 1 FROM {V} AS t(c) WHERE t.c = {Z})")
run(
    "f3/pin/fexpr_column_probe",
    lambda: (
        s.sql(f"SELECT 1 AS id, {Z} AS z")
        .filter(sf.expr(f"z IN (SELECT c FROM {V} AS t(c))"))
        .select("id")
    ),
)
run(
    "f3/pin/fexpr_scalar_min",
    lambda: s.sql("SELECT 1 AS id").select(sf.expr(f"(SELECT min(c) FROM {V} AS t(c))").alias("m")),
)
run(
    "f3/pin/fexpr_two_values",
    lambda: s.sql("SELECT 1 AS id").filter(
        sf.expr(
            f"{Z} IN (SELECT c FROM {V} AS t(c)) AND CAST(0.1 AS DOUBLE) IN (SELECT c FROM {V} "
            f"AS u(c))"
        )
    ),
)
run(
    "f3/pin/fexpr_plain_values",
    lambda: s.sql("SELECT 1 AS id").filter(
        sf.expr(f"{Z} IN (SELECT c FROM VALUES (0.1), (0.2) AS t(c))")
    ),
)
run(
    "f3/pin/fexpr_all_double_values",
    lambda: s.sql("SELECT 1 AS id").filter(
        sf.expr(f"{Z} IN (SELECT c FROM VALUES (CAST(0.1 AS DOUBLE)), ({Z}) AS t(c))")
    ),
)

LAT = {
    "comma": f"SELECT t.c, l.x FROM {V} AS t(c), LATERAL (SELECT t.c + 1 AS x) l ORDER BY t.c",
    "join_lateral": f"SELECT t.c, l.x FROM {V} AS t(c) JOIN LATERAL (SELECT t.c + 1 AS x) l "
    f"ORDER BY t.c",
    "join_lateral_on": f"SELECT t.c, l.x FROM {V} AS t(c) JOIN LATERAL (SELECT t.c + 1 AS x) l "
    f"ON true ORDER BY t.c",
    "cross_join_lateral": f"SELECT t.c, l.x FROM {V} AS t(c) CROSS JOIN LATERAL (SELECT t.c * "
    f"2 AS x) l ORDER BY t.c",
    "left_join_lateral": f"SELECT t.c, l.x FROM {V} AS t(c) LEFT JOIN LATERAL (SELECT t.c + 1 "
    f"AS x) l ON true ORDER BY t.c",
    "lateral_where": f"SELECT t.c, l.y FROM {V} AS t(c), LATERAL (SELECT u.y FROM VALUES "
    f"(0.05), (0.5) AS u(y) WHERE u.y > t.c) l ORDER BY t.c, l.y",
    "lateral_eq": f"SELECT t.c, l.y FROM {V} AS t(c), LATERAL (SELECT u.y FROM VALUES ({Z}), "
    f"(CAST(0.0 AS DOUBLE)) AS u(y) WHERE u.y = t.c) l ORDER BY t.c",
    "lateral_two_cols": f"SELECT t.id, t.c, l.x FROM VALUES (1, CAST(0.1 AS DECIMAL(10,2))), "
    f"(2, {Z}) AS t(id, c), LATERAL (SELECT t.c + t.id AS x) l ORDER BY t.id",
    "lateral_cte": f"WITH v AS (SELECT c FROM {V} AS t(c)) SELECT v.c, l.x FROM v, LATERAL "
    f"(SELECT v.c + 1 AS x) l ORDER BY v.c",
    "lateral_in_union_branch": f"SELECT t.c, l.x FROM {V} AS t(c), LATERAL (SELECT t.c + 1 AS "
    f"x) l UNION ALL SELECT CAST(0.5 AS DOUBLE), CAST(1.5 AS DOUBLE) ORDER BY 1",
    "lateral_union_inside": f"SELECT t.c, l.x FROM {V} AS t(c), LATERAL (SELECT t.c AS x UNION "
    f"ALL SELECT t.c * 2) l ORDER BY t.c, l.x",
    "lateral_under_union_first": f"SELECT 0.5 AS c, 1.5 AS x UNION ALL SELECT t.c, l.x FROM "
    f"{V} AS t(c), LATERAL (SELECT t.c + 1 AS x) l ORDER BY 1",
    "lateral_agg": f"SELECT t.c, l.n FROM {V} AS t(c), LATERAL (SELECT count(*) AS n FROM "
    f"VALUES (1), (2) AS u(y) WHERE u.y > t.c) l ORDER BY t.c",
    "scalar_select": f"SELECT t.c, (SELECT t.c + 1) AS m FROM {V} AS t(c) ORDER BY t.c",
    "scalar_select_agg": f"SELECT t.c, (SELECT max(u.y) FROM VALUES (0.05), (0.5) AS u(y) "
    f"WHERE u.y > t.c) AS m FROM {V} AS t(c) ORDER BY t.c",
    "scalar_select_count": f"SELECT t.c, (SELECT count(*) FROM VALUES ({Z}), (0.1) AS u(y) "
    f"WHERE u.y = t.c) AS m FROM {V} AS t(c) ORDER BY t.c",
    "exists_where": f"SELECT t.c FROM {V} AS t(c) WHERE EXISTS (SELECT 1 FROM VALUES ({Z}) AS "
    f"u(y) WHERE u.y = t.c) ORDER BY t.c",
    "not_exists_where": f"SELECT t.c FROM {V} AS t(c) WHERE NOT EXISTS (SELECT 1 FROM VALUES "
    f"({Z}) AS u(y) WHERE u.y = t.c) ORDER BY t.c",
    "in_where_correlated": f"SELECT t.c FROM {V} AS t(c) WHERE t.c IN (SELECT u.y FROM VALUES "
    f"({Z}), (CAST(0.1 AS DOUBLE)) AS u(y) WHERE u.y >= t.c) ORDER BY t.c",
    "in_where_outer_only": f"SELECT t.c FROM {V} AS t(c) WHERE 1 IN (SELECT 1 FROM VALUES (1) "
    f"AS u(y) WHERE t.c < 0.05) ORDER BY t.c",
    "scalar_where": f"SELECT t.c FROM {V} AS t(c) WHERE (SELECT count(*) FROM VALUES ({Z}) AS "
    f"u(y) WHERE u.y = t.c) = 1 ORDER BY t.c",
    "exists_in_having": f"SELECT t.c, count(*) AS n FROM {V} AS t(c) GROUP BY t.c HAVING "
    f"EXISTS (SELECT 1 FROM VALUES ({Z}) AS u(y) WHERE u.y = t.c) ORDER BY t.c",
    "nested_two_levels": f"SELECT t.c FROM {V} AS t(c) WHERE EXISTS (SELECT 1 FROM VALUES (1) "
    f"AS u(y) WHERE EXISTS (SELECT 1 FROM VALUES ({Z}) AS w(z) WHERE w.z = t.c)) ORDER BY t.c",
    "inner_values_outer_ref": f"SELECT o.z FROM (SELECT {Z} AS z) o WHERE EXISTS (SELECT 1 "
    f"FROM {V} AS t(c) WHERE t.c = o.z)",
    "lateral_inner_values": f"SELECT o.z, l.c FROM (SELECT {Z} AS z) o, LATERAL (SELECT t.c "
    f"FROM {V} AS t(c) WHERE t.c = o.z) l",
}
for key, text in LAT.items():
    sql(f"f2/{key}", text)

STRAY = {
    "plain": "SELECT __repark_values_cell(1)",
    "double": f"SELECT __repark_values_cell({Z})",
    "decimal": "SELECT __repark_values_cell(0.1)",
    "upper": "SELECT __REPARK_VALUES_CELL(1)",
    "mixed_case": "SELECT __Repark_Values_Cell(1)",
    "backquoted": "SELECT `__repark_values_cell`(1)",
    "nested": "SELECT __repark_values_cell(__repark_values_cell(0.1))",
    "nested_fn": "SELECT ABS(__repark_values_cell(-1.5)) AS a",
    "zero_arg": "SELECT __repark_values_cell()",
    "two_arg": "SELECT __repark_values_cell(1, 2)",
    "alias": "SELECT __repark_values_cell(CAST(0.1 AS DECIMAL(10,2))) AS d",
    "order_by": "SELECT 1 AS x ORDER BY __repark_values_cell(1)",
    "order_alias": "SELECT __repark_values_cell(1) AS x ORDER BY x",
    "where_no_values": "SELECT 1 AS id WHERE __repark_values_cell(1.5) > 1.0",
    "having_no_values": "SELECT count(*) AS n FROM (SELECT 1 AS a) GROUP BY a HAVING "
    "__repark_values_cell(count(*)) > 0",
    "group_by": "SELECT __repark_values_cell(a) AS g FROM (SELECT 1 AS a) GROUP BY "
    "__repark_values_cell(a)",
    "in_cell_mixed": f"SELECT c FROM VALUES (__repark_values_cell(0.1)), ({Z}) AS t(c)",
    "in_cell_mixed_dec": f"SELECT c FROM (VALUES (__repark_values_cell(CAST(0.1 AS "
    f"DECIMAL(10,2)))), ({Z})) AS t(c)",
    "in_cell_plain": "SELECT c FROM (VALUES (__repark_values_cell(1.0)), (2.0)) AS t(c)",
    "in_cell_nested": f"SELECT c FROM VALUES (ABS(__repark_values_cell(0.1))), ({Z}) AS t(c)",
    "in_cell_upper": f"SELECT c FROM VALUES (__REPARK_VALUES_CELL(0.1)), ({Z}) AS t(c)",
    "in_single_row_values": "SELECT c FROM VALUES (__repark_values_cell(0.1)) AS t(c)",
    "proj_with_values": f"SELECT __repark_values_cell(1) AS x, c FROM {V} AS t(c) ORDER BY c",
    "proj_with_plain_values": "SELECT __repark_values_cell(1) AS x, c FROM VALUES (0.1), (0.2) "
    "AS t(c) ORDER BY c",
    "where_with_values": f"SELECT c FROM {V} AS t(c) WHERE c > __repark_values_cell(0)",
    "having_with_values": f"SELECT c FROM {V} AS t(c) GROUP BY c HAVING "
    f"__repark_values_cell(COUNT(*)) > 0",
    "in_pred_with_values": f"SELECT c FROM {V} AS t(c) WHERE c IN (__repark_values_cell(0.1)) "
    f"ORDER BY c",
    "subquery": f"SELECT 1 AS id WHERE {Z} IN (SELECT __repark_values_cell(c) FROM {V} AS t(c))",
    "cte": "WITH w AS (SELECT __repark_values_cell(1) AS x) SELECT x FROM w",
    "insert_values": "INSERT INTO lc.ns.stray_t VALUES (__repark_values_cell(0.1)), (0.2)",
    "explain": "EXPLAIN SELECT __repark_values_cell(1)",
}
s.sql("CREATE NAMESPACE IF NOT EXISTS lc.ns").collect()
s.sql("CREATE TABLE IF NOT EXISTS lc.ns.stray_t (c DOUBLE) USING iceberg").collect()
for key, text in STRAY.items():
    sql(f"f1/{key}", text)
run(
    "f1/fexpr_filter",
    lambda: s.sql("SELECT 1 AS id").filter(sf.expr("__repark_values_cell(1) = 1")),
)
run(
    "f1/fexpr_select",
    lambda: s.sql("SELECT 1 AS id").select(sf.expr("__repark_values_cell(1)").alias("x")),
)
run(
    "f1/fexpr_in_cell",
    lambda: s.sql("SELECT 1 AS id").filter(
        sf.expr(f"{Z} IN (SELECT c FROM VALUES (__repark_values_cell(0.1)), ({Z}) AS t(c))")
    ),
)
run("f1/selectexpr", lambda: s.sql("SELECT 1 AS id").selectExpr("__repark_values_cell(id) AS x"))
run("f1/filter_str", lambda: s.sql("SELECT 1 AS id").filter("__repark_values_cell(id) = 1"))
run(
    "f1/withcolumn",
    lambda: s.sql("SELECT 1 AS id").withColumn("x", sf.expr("__REPARK_VALUES_CELL(id)")),
)
records.append(
    {
        "key": "f1/insert_values_readback",
        **(
            {
                "rows": [
                    [shown(v) for v in r]
                    for r in s.sql("SELECT c FROM lc.ns.stray_t ORDER BY c").collect()
                ]
            }
        ),
    }
)

if engine != "spark":
    import repark
    from repark._native import PyReparkSession

    nsession = PyReparkSession.native(config={"spark.sql.ansi.enabled": ansi})
    repark._ANSI_NATIVE = nsession
    fragment_seats("nat/f3", repark.sql, sf)
    for key, text in LAT.items():
        run(f"nat/f2/{key}", lambda t=text: repark.sql(t))
    for key, text in STRAY.items():
        if key in ("insert_values",):
            continue
        run(f"nat/f1/{key}", lambda t=text: repark.sql(t))
    run(
        "nat/f1/fexpr_filter",
        lambda: repark.sql("SELECT 1 AS id").filter(sf.expr("__repark_values_cell(1) = 1")),
    )
    run(
        "nat/f1/fexpr_select",
        lambda: repark.sql("SELECT 1 AS id").select(sf.expr("__repark_values_cell(1)").alias("x")),
    )
    run(
        "nat/f1/fexpr_in_cell",
        lambda: repark.sql("SELECT 1 AS id").filter(
            sf.expr(f"{Z} IN (SELECT c FROM VALUES (__repark_values_cell(0.1)), ({Z}) AS t(c))")
        ),
    )
    run(
        "nat/f1/filter_str",
        lambda: repark.sql("SELECT 1 AS id").filter("__repark_values_cell(id) = 1"),
    )
    run(
        "nat/f1/selectexpr",
        lambda: repark.sql("SELECT 1 AS id").selectExpr("__repark_values_cell(id) AS x"),
    )

with Path(out).open("w") as fh:
    for record in records:
        fh.write(json.dumps(record, default=str) + "\n")
print("wrote", out, len(records))
