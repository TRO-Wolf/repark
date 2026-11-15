import sys

sys.path.insert(0, sys.argv[4])
import cells
from common import error_record, run_frame, write_jsonl

engine, ansi, out = sys.argv[1], sys.argv[2], sys.argv[3]
if engine == "spark":
    from pyspark.sql import SparkSession as Builder
    from pyspark.sql import functions as sf

    b = Builder.builder.master("local[1]").config("spark.ui.enabled", "false")
else:
    from repark import ReparkSession as Builder
    from repark import functions as sf

    b = Builder.builder.appName("ddu-fexpr")
s = (
    b.config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.ansi.enabled", ansi)
    .getOrCreate()
)
src = s.createDataFrame(cells.src_rows(), cells.SRC_SCHEMA)
src.createOrReplaceTempView("src")
other = s.createDataFrame([(1, 1e-20)], "k INT, w DOUBLE")
CASES = {
    "fexpr_coalesce_cols": lambda: src.select(sf.expr("coalesce(dd, dc)").alias("c")),
    "fexpr_greatest_cols": lambda: src.select(sf.expr("greatest(dc, dbig)").alias("c")),
    "fexpr_case_cols": lambda: src.select(
        sf.expr("CASE WHEN id = 2 THEN dc ELSE dd END").alias("c")
    ),
    "fexpr_eq_cols": lambda: src.select(sf.expr("dc = dd").alias("c")),
    "fexpr_cast_dec_lit": lambda: src.select(
        sf.expr("coalesce(dd, CAST(0.1 AS DECIMAL(10,2)))").alias("c")
    ),
    "fexpr_dec_lit_vs_dbl": lambda: src.select(
        sf.expr("CAST(0.0 AS DECIMAL(10,2)) = dd").alias("c")
    ),
    "selectexpr_in_cols": lambda: src.selectExpr("dd IN (dc) AS c"),
    "filter_cols_eq": lambda: src.filter("CAST(dc AS DECIMAL(10,2)) - 1.25 = dd").selectExpr(
        "count(*) AS c"
    ),
    "col_eq_cols": lambda: src.select(
        (sf.col("dc") - sf.lit(1.25).cast("decimal(10,2)") == sf.col("dd")).alias("c")
    ),
    "col_between_cols": lambda: src.select(
        sf.col("dd").between(sf.col("dc") - 1.25, sf.col("dc")).alias("c")
    ),
    "df_join_dec_dbl": lambda: (
        src.select((sf.col("dc") - sf.lit(1.25).cast("decimal(10,2)")).alias("k0"))
        .join(other, sf.col("k0") == sf.col("w"))
        .selectExpr("count(*) AS c")
    ),
    "df_when_col_dec": lambda: src.select(
        sf.when(sf.col("id") == 1, sf.col("dd")).otherwise(sf.col("dc")).alias("c")
    ),
}
records = [{"key": "provenance", "engine": engine, "ansi": ansi}]
for key, fn in CASES.items():
    rec = {"key": f"dfx/{key}"}
    try:
        rec.update(run_frame(fn()))
    except Exception as e:
        rec.update(error_record(e))
    records.append(rec)
write_jsonl(out, records)
print(engine, ansi, len(records))
