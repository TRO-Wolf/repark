import json
import sys
import time
from pathlib import Path

import repark._native as native
from repark import ReparkSession

case, label, data, out = sys.argv[1:5]
ansi = "false" if case.endswith("_off") else "true"
s = ReparkSession.builder.appName("sum-cost").config("spark.sql.ansi.enabled", ansi).getOrCreate()
s.read.parquet(f"{data}/k1").createOrReplaceTempView("k1")
s.read.parquet(f"{data}/k2").createOrReplaceTempView("k2")
GROUPED = "SELECT count(*), sum(m) FROM (SELECT g, {f}({c}) AS m FROM k1 GROUP BY g)"
WINDOW = (
    "SELECT count(*), sum(m) FROM (SELECT sum({c}) OVER (PARTITION BY p ORDER BY id "
    "ROWS BETWEEN {frame} AND CURRENT ROW) AS m FROM k2)"
)
PLANS = {
    "k1_sum_d10_on": (GROUPED.format(f="sum", c="d10"), 4),
    "k1_sum_d10_off": (GROUPED.format(f="sum", c="d10"), 4),
    "k1_sum_d38_on": (GROUPED.format(f="sum", c="d38"), 4),
    "k1_sum_d38_off": (GROUPED.format(f="sum", c="d38"), 4),
    "k2_rows3_d10": (WINDOW.format(c="d10", frame="3 PRECEDING"), 4),
    "k2_rows3_d38": (WINDOW.format(c="d38", frame="3 PRECEDING"), 4),
    "k2_running_d10": (WINDOW.format(c="d10", frame="UNBOUNDED PRECEDING"), 4),
    "k2_running_d38": (WINDOW.format(c="d38", frame="UNBOUNDED PRECEDING"), 4),
    "k2_rows1000_d10": (WINDOW.format(c="d10", frame="1000 PRECEDING"), 4),
    "k2_rows1000_d38": (WINDOW.format(c="d38", frame="1000 PRECEDING"), 4),
    "k3_sum_d10": ("SELECT sum(d10) FROM k1", 10),
    "k3_sum_d36": ("SELECT sum(d36) FROM k1", 10),
    "k3_sum_d38": ("SELECT sum(d38) FROM k1", 10),
    "k4_sum_b_grouped": (GROUPED.format(f="sum", c="b"), 4),
    "k4_sum_x_grouped": (GROUPED.format(f="sum", c="x"), 4),
    "k4_sum_b": ("SELECT sum(b) FROM k1", 10),
    "k4_sum_x": ("SELECT sum(x) FROM k1", 10),
    "k4_sum_linear": ("SELECT sum(b + 1), sum(b + 2) FROM k1", 10),
    "k5_try_sum_d10_grouped": (GROUPED.format(f="try_sum", c="d10"), 4),
}
sql, reps = PLANS[case]
got = s.sql(sql).collect()
start = time.perf_counter()
for _ in range(reps):
    got = s.sql(sql).collect()
seconds = time.perf_counter() - start
record = {
    "case": case,
    "label": label,
    "seconds": seconds,
    "reps": reps,
    "ansi": ansi,
    "answer": str([list(r) for r in got])[:300],
    "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
}
with Path(out).open("a") as fh:
    fh.write(json.dumps(record) + "\n")
