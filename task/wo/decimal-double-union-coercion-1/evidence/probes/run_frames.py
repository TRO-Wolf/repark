import sys
from decimal import Decimal

import cells
from common import error_record, run_frame, write_jsonl

engine, ansi, out = sys.argv[1], sys.argv[2], sys.argv[3]
if engine == "spark":
    import pyspark
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

    b = (
        Builder.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
    )
    prov = {"engine": "spark", "version": pyspark.__version__}
else:
    import repark._native as native
    from repark import ReparkSession as Builder
    from repark import functions as sf

    b = Builder.builder.appName("ddf")
    prov = {
        "engine": "repark",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
s = (
    b.config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
records = [dict(key="provenance", ansi=ansi, **prov)]
src = s.createDataFrame(cells.src_rows(), cells.SRC_SCHEMA)
src.createOrReplaceTempView("src")
LEFTS = ["dec_0.1", "dec_18_9", "dec_col", "int", "null"]
RIGHTS = ["dbl_1e-20", "dbl_1e30", "dbl_neg0", "dbl_nan", "dbl_col", "flt_1e-20"]
OPS = {
    "union": lambda a, b: a.union(b),
    "unionByName": lambda a, b: a.unionByName(b),
    "intersect": lambda a, b: a.intersect(b),
    "subtract": lambda a, b: a.subtract(b),
    "intersectAll": lambda a, b: a.intersectAll(b),
    "exceptAll": lambda a, b: a.exceptAll(b),
}
cases = []
for op, fn in OPS.items():
    for ln in LEFTS:
        for rn in RIGHTS:
            lq = f"SELECT {cells.LEFT[ln]} AS c FROM src"
            rq = f"SELECT {cells.RIGHT[rn]} AS c FROM src"
            cases.append(
                (f"df_{op}/{ln}/{rn}", lambda fn=fn, lq=lq, rq=rq: fn(s.sql(lq), s.sql(rq)))
            )
COLS = {
    "coalesce_dd_dc": lambda: src.select(sf.coalesce(sf.col("dd"), sf.col("dc")).alias("c")),
    "coalesce_dbig_dc": lambda: src.select(sf.coalesce(sf.col("dbig"), sf.col("dc")).alias("c")),
    "coalesce_dd_litdec": lambda: src.select(
        sf.coalesce(sf.col("dd"), sf.lit(Decimal("0.1"))).alias("c")
    ),
    "greatest_dc_dbig": lambda: src.select(sf.greatest(sf.col("dc"), sf.col("dbig")).alias("c")),
    "least_dc_dd": lambda: src.select(sf.least(sf.col("dc"), sf.col("dd")).alias("c")),
    "when_dc_dd": lambda: src.select(
        sf.when(sf.col("id") == 2, sf.col("dc")).otherwise(sf.col("dd")).alias("c")
    ),
    "when_dc_dbig": lambda: src.select(
        sf.when(sf.col("id") == 2, sf.col("dc")).otherwise(sf.col("dbig")).alias("c")
    ),
    "array_dc_dd": lambda: src.select(sf.array(sf.col("dc"), sf.col("dd")).alias("c")),
    "map_dc_dd": lambda: src.select(
        sf.create_map(sf.lit("a"), sf.col("dc"), sf.lit("b"), sf.col("dd")).alias("c")
    ),
    "nvl_dd_dc": lambda: src.select(sf.nvl(sf.col("dd"), sf.col("dc")).alias("c")),
    "ifnull_dd_dc": lambda: src.select(sf.ifnull(sf.col("dd"), sf.col("dc")).alias("c")),
    "nvl2_id_dd_dc": lambda: src.select(
        sf.nvl2(sf.col("id"), sf.col("dd"), sf.col("dc")).alias("c")
    ),
    "nullif_dd_zero": lambda: src.select(
        sf.nullif(sf.col("dd"), sf.lit(Decimal("0.0"))).alias("c")
    ),
    "isin_dd_litdec": lambda: src.select(sf.col("dd").isin(Decimal("0.0")).alias("c")),
    "isin_dc_dd": lambda: src.select(sf.col("dc").isin(sf.col("dd")).alias("c")),
    "fexpr_coalesce": lambda: src.select(sf.expr("coalesce(dd, 0.1)").alias("c")),
    "fexpr_greatest_big": lambda: src.select(sf.expr("greatest(dbig, 0.1)").alias("c")),
    "fexpr_case": lambda: src.select(sf.expr("CASE WHEN id = 2 THEN 0.1 ELSE dd END").alias("c")),
    "fexpr_array": lambda: src.select(sf.expr("array(0.1, dd)").alias("c")),
    "selectexpr_if": lambda: src.selectExpr("IF(id = 2, 0.1, dd) AS c"),
    "filter_dd_eq_zero": lambda: src.filter("dd = 0.0").selectExpr("count(*) AS c"),
    "filter_zero_in_dd": lambda: src.filter("0.0 IN (dd)").selectExpr("count(*) AS c"),
    "withcolumn_union_view": lambda: (
        s.sql("SELECT 0.5 AS c").union(s.sql("SELECT dd AS c FROM src")).filter("c > 0").select("c")
    ),
}
for name, fn in COLS.items():
    cases.append((f"dfcol/{name}", fn))
for key, fn in cases:
    rec = {"key": key}
    try:
        rec.update(run_frame(fn()))
    except Exception as e:
        rec.update(error_record(e))
    records.append(rec)
write_jsonl(out, records)
print(engine, ansi, len(records))
