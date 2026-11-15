import json
import sys
from pathlib import Path

import repark._native as native
from repark import ReparkSession

out = sys.argv[1]

s = ReparkSession.builder.appName("ddu-zero").config("spark.sql.ansi.enabled", "true").getOrCreate()
ZSRC = "(SELECT CAST(0 AS DECIMAL(10,2)) AS d, CAST('-0.0' AS DOUBLE) AS v)"
CELLS = {
    "zero_eq_today": f"SELECT v = d AS c FROM {ZSRC}",
    "zero_eq_widened": f"SELECT CAST(d AS DOUBLE) = v AS c FROM {ZSRC}",
    "zero_eq_widened_inline": (
        "SELECT CAST(CAST(0 AS DECIMAL(10,2)) AS DOUBLE) = CAST('-0.0' AS DOUBLE) AS c"
    ),
    "zero_eq_double_consts": "SELECT CAST('0.0' AS DOUBLE) = CAST('-0.0' AS DOUBLE) AS c",
}
records = [
    {"key": "provenance", "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1]}
]
for key, q in CELLS.items():
    rec = {"key": key, "sql": q}
    try:
        rec["answer"] = str(s.sql(q).collect()[0][0])
        rows = s.sql("EXPLAIN VERBOSE " + q).collect()
        steps = []
        last = None
        for r in rows:
            kind, plan = str(r[0]), str(r[1])
            if "physical" in kind.lower():
                continue
            if plan != last and "same as above" not in plan.lower():
                steps.append({"after": kind, "plan": plan.splitlines()[0][:300]})
                last = plan
        rec["steps"] = steps
    except Exception as e:
        rec["error"] = str(e).strip().splitlines()[0][:240]
    records.append(rec)
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r) + "\n")
print(len(records))
