import json
import sys
import tempfile
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import cells

engine, ansi, wh_parent, out = sys.argv[1:5]
wh = tempfile.mkdtemp(prefix=f"sh-{engine}-{ansi}-", dir=wh_parent)
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

    builder = Builder.builder.master("local[1]").config("spark.ui.enabled", "false")
    prov = {"engine": "spark", "version": pyspark.__version__}
else:
    import repark._native as native
    from repark import ReparkSession as Builder
    from repark import functions as sf

    print("native", native.__file__, flush=True)
    builder = Builder.builder.appName("cast-unwrap-shapes")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
s = (
    builder.config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.warehouse.dir", wh)
    .getOrCreate()
)
records: list = [dict(key="provenance", ansi=ansi, **prov)]
KINDS = {k.name: k for k in cells.KINDS}


def err(e: Exception) -> str:
    cls = None
    for attr in ("getCondition", "getErrorClass"):
        fn = getattr(e, attr, None)
        if fn is not None:
            try:
                cls = fn()
            except Exception:
                cls = None
            if cls:
                break
    return f"ERR:{type(e).__name__}:{cls}:{' '.join(str(e).split())[:160]}"


def answer(run: Callable[[], object]) -> object:
    try:
        return run()
    except Exception as e:
        return err(e)


def ids(frame: Any) -> list:
    return sorted(int(r[0]) for r in frame.collect())


def record(key: str, text: str, run: Callable[[], object]) -> None:
    records.append({"key": key, "sql": text, "result": answer(run)})


def sql_ids(key: str, sql: str) -> None:
    record(key, sql, lambda: ids(s.sql(sql)))


for name in ("dec_int", "dec_dec0", "bigint_int", "dec_tinyint_overflow", "int_smallint"):
    s.sql(cells.select_sql(KINDS[name])).createOrReplaceTempView(f"v_{name}")
dec_int = KINDS["dec_int"]
items = ", ".join(f"CAST({v} AS {dec_int.col_type})" for v in dec_int.values)
s.sql(
    "SELECT CAST(id AS INT) AS id, element_at(array("
    + items
    + "), CAST(id AS INT)) AS `café` FROM range(1, "
    + str(len(dec_int.values) + 1)
    + ")"
).createOrReplaceTempView("v_cafe")

V = "SELECT id FROM v_dec_int WHERE "
sql_ids("s02/nested_int_bigint", V + "CAST(CAST(c AS INT) AS BIGINT) = 1")
sql_ids(
    "s02/nested_dec0_dec20",
    "SELECT id FROM v_dec_dec0 WHERE CAST(CAST(c AS DECIMAL(10,0)) AS DECIMAL(20,0)) = 1",
)
sql_ids("s03/try_cast_int", V + "TRY_CAST(c AS INT) = 1")
sql_ids("s03/try_cast_dec0", "SELECT id FROM v_dec_dec0 WHERE TRY_CAST(c AS DECIMAL(10,0)) = 1")
sql_ids("s03/try_cast_bigint_int", "SELECT id FROM v_bigint_int WHERE TRY_CAST(c AS INT) = 1")
sql_ids("s04/colon_int", V + "c::int = 1")
sql_ids("s04/colon_dec0", "SELECT id FROM v_dec_dec0 WHERE c::decimal(10,0) = 1")
sql_ids(
    "s17/subquery", "SELECT id FROM (SELECT id, CAST(c AS INT) AS k FROM v_dec_int) WHERE k = 1"
)
sql_ids("s23/non_ascii", "SELECT id FROM v_cafe WHERE CAST(`café` AS INT) = 1")
sql_ids("spelled/is_distinct_c0", V + "CAST(c AS INT) IS DISTINCT FROM 1")
sql_ids("spelled/is_not_distinct_c0", V + "CAST(c AS INT) IS NOT DISTINCT FROM 1")
sql_ids("spelled/is_distinct_c1", V + "CAST(c AS INT) IS DISTINCT FROM 0")
sql_ids("spelled/is_not_distinct_c1", V + "CAST(c AS INT) IS NOT DISTINCT FROM 0")
sql_ids("spelled/not_between", V + "CAST(c AS INT) NOT BETWEEN 1 AND 2")
sql_ids("neg_half/dec0_eq_minus1", "SELECT id FROM v_dec_dec0 WHERE CAST(c AS DECIMAL(10,0)) = -1")
sql_ids("neg_half/dec0_lt_minus1", "SELECT id FROM v_dec_dec0 WHERE CAST(c AS DECIMAL(10,0)) < -1")
frame = s.table("v_dec_int")
record(
    "s21/with_column_filter_str",
    'withColumn("k", col("c").cast("int")).filter("k = 1")',
    lambda: ids(frame.withColumn("k", sf.col("c").cast("int")).filter("k = 1").select("id")),
)
record(
    "s22/with_column_filter_col",
    'withColumn("k", col("c").cast("int")).filter(col("k") == 1)',
    lambda: ids(
        frame.withColumn("k", sf.col("c").cast("int")).filter(sf.col("k") == 1).select("id")
    ),
)
TOP = (
    ("v_dec_tinyint_overflow", "TINYINT", "127", "-128"),
    ("v_int_smallint", "SMALLINT", "32767", "-32768"),
    ("v_bigint_int", "INT", "2147483647", "-2147483648"),
)
for view, target, top, bottom in TOP:
    cast = f"CAST(c AS {target})"
    for label, pred in (
        ("gt_top", f"{cast} > {top}"),
        ("top_lt", f"{top} < {cast}"),
        ("ge_top", f"{cast} >= {top}"),
        ("le_top", f"{cast} <= {top}"),
        ("lt_bottom", f"{cast} < {bottom}"),
        ("ge_bottom", f"{cast} >= {bottom}"),
        ("eq_top_plus", f"{cast} = {int(top) + 1}"),
    ):
        sql_ids(f"q4/{target.lower()}/{label}", f"SELECT id FROM {view} WHERE {pred}")
record(
    "s16/view_over_view",
    "CREATE OR REPLACE TEMP VIEW w16 AS SELECT id, CAST(c AS INT) AS k FROM v_dec_int; "
    "SELECT id FROM w16 WHERE k = 1",
    lambda: (
        s.sql(
            "CREATE OR REPLACE TEMP VIEW w16 AS SELECT id, CAST(c AS INT) AS k FROM v_dec_int"
        ).collect(),
        ids(s.sql("SELECT id FROM w16 WHERE k = 1")),
    )[1],
)
record(
    "s16/view_over_view_plain",
    "CREATE TEMP VIEW w16b AS SELECT id, c FROM v_dec_int; "
    "SELECT id FROM w16b WHERE CAST(c AS INT) = 1",
    lambda: (
        s.sql("CREATE TEMP VIEW w16b AS SELECT id, c FROM v_dec_int").collect(),
        ids(s.sql("SELECT id FROM w16b WHERE CAST(c AS INT) = 1")),
    )[1],
)
record(
    "s16/view_over_base_table",
    "CREATE TEMP VIEW w16c AS SELECT id, CAST(c AS INT) AS k FROM p16; "
    "SELECT id FROM w16c WHERE k = 1",
    lambda: (
        s.table("v_dec_int").write.parquet(str(Path(wh) / "p16")),
        s.read.parquet(str(Path(wh) / "p16")).createOrReplaceTempView("p16"),
        s.sql("CREATE TEMP VIEW w16c AS SELECT id, CAST(c AS INT) AS k FROM p16").collect(),
        ids(s.sql("SELECT id FROM w16c WHERE k = 1")),
    )[3],
)


def view_over_cdf() -> list:
    rows = [(1, Decimal("1.25")), (2, Decimal("2.50")), (3, Decimal("1.00"))]
    s.createDataFrame(rows, "id INT, c DECIMAL(10,2)").createOrReplaceTempView("cdf16")
    s.sql("CREATE OR REPLACE TEMP VIEW w16d AS SELECT id, CAST(c AS INT) AS k FROM cdf16").collect()
    return ids(s.sql("SELECT id FROM w16d WHERE k = 1"))


record(
    "s16/view_over_create_dataframe",
    "createDataFrame view, then a SQL temp view over it",
    view_over_cdf,
)

sql_ids("s05/int_fn", V + "int(c) = 1")
sql_ids("s06/bigint_fn", V + "bigint(c) = 1")
record(
    "s26/explain_parquet_view",
    "EXPLAIN SELECT id FROM p16 WHERE CAST(c AS INT) = 1",
    lambda: [
        " ".join(str(r[-1]).split())[:200]
        for r in s.sql("EXPLAIN SELECT id FROM p16 WHERE CAST(c AS INT) = 1").collect()
    ],
)
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records))
