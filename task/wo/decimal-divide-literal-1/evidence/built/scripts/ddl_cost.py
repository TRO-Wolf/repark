import json
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession
from repark import functions as sf

case, label, data, out = sys.argv[1:5]
s = ReparkSession.builder.appName("ddl-cost").getOrCreate()
k = s.read.parquet(f"{data}/k")
k.createOrReplaceTempView("k")
AGG = "SELECT count(*), sum(q) FROM (SELECT {e} AS q FROM k)"
SQL = {
    "s1_sql_d10_div3": AGG.format(e="d10 / 3"),
    "s1_sql_d38_div3": AGG.format(e="d38 / 3"),
    "s1_sql_d10_mod3": AGG.format(e="mod(d10, 3)"),
    "s1_sql_d10_pmod3": AGG.format(e="pmod(d10, 3)"),
    "s3_sql_int_div_int": AGG.format(e="i1 / i2"),
    "s3_sql_double_div_double": AGG.format(e="x1 / x2"),
}
d = sf.col("d10")
FRAME = {
    "s2_df_d10_div3": lambda: d / 3,
    "s2_df_d10_mod3": lambda: d % 3,
    "s2_df_d10_pmod": lambda: sf.pmod(d, sf.lit(3)),
    "s2_df_d10_expr_mod": lambda: sf.expr("mod(d10, 3)"),
    "s2_df_d10_expr_div3": lambda: sf.expr("d10 / 3"),
    "s3_df_int_div_int": lambda: sf.col("i1") / sf.col("i2"),
    "s3_df_double_div_double": lambda: sf.col("x1") / sf.col("x2"),
}
REPS = 4


def run() -> list:
    if case in SQL:
        return s.sql(SQL[case]).collect()
    return k.select(FRAME[case]().alias("q")).agg(sf.count(sf.lit(1)), sf.sum("q")).collect()


got = run()
start = time.perf_counter()
for _ in range(REPS):
    got = run()
seconds = time.perf_counter() - start
record = {
    "case": case,
    "label": label,
    "seconds": seconds,
    "reps": REPS,
    "answer": str([list(r) for r in got])[:300],
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as fh:
    fh.write(json.dumps(record) + "\n")
