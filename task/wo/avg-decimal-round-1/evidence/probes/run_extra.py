import json
import sys
import tempfile
from pathlib import Path

import cells
import lib

engine, ansi, wh_parent, jar, out = sys.argv[1:6]
wh = tempfile.mkdtemp(prefix=f"extra-{engine}-{ansi}-", dir=wh_parent)
s, sf, _, prov = lib.session(engine, ansi, wh, jar, {})
names, rows = cells.avg_rows()
types = cells.avg_types()
raw = s.createDataFrame(rows, ", ".join(f"{n} STRING" for n in names))
raw.selectExpr(*[f"CAST({n} AS {types[n]}) AS {n}" for n in names]).createOrReplaceTempView("t")
dn, dr = cells.div_rows()
dtypes = cells.div_types()
rawv = s.createDataFrame(dr, ", ".join(f"{n} STRING" for n in dn))
rawv.selectExpr(*[f"CAST({n} AS {dtypes[n]}) AS {n}" for n in dn]).createOrReplaceTempView("v")
SQL = {
    "avg_distinct_with_count": (
        "SELECT g, avg(DISTINCT c10_2) AS m, count(c10_2) AS n FROM t WHERE g IN (2, 13) GROUP BY g"
    ),
    "avg_distinct_window": (
        "SELECT g, avg(DISTINCT c10_2) OVER (PARTITION BY g) AS m FROM t WHERE g IN (2, 13)"
    ),
    "mul_tinyint_literal": "SELECT id, d10_2 * 3Y AS q FROM v WHERE id <= 3",
    "add_tinyint_literal": "SELECT id, d10_2 + 3Y AS q FROM v WHERE id <= 3",
    "mul_smallint_literal": "SELECT id, d10_2 * 3S AS q FROM v WHERE id <= 3",
    "mul_bigint_literal": "SELECT id, d10_2 * 3L AS q FROM v WHERE id <= 3",
    "try_add_dec_int": "SELECT id, try_add(d10_2, 1) AS q FROM v WHERE id <= 3",
    "try_multiply_dec_int": "SELECT id, try_multiply(d10_2, 3) AS q FROM v WHERE id <= 3",
    "try_divide_dec_dec": (
        "SELECT id, try_divide(d10_2, CAST(3 AS DECIMAL(1,0))) AS q FROM v WHERE id <= 3"
    ),
}
records = [dict(key="provenance", ansi=ansi, **prov)]
for key, sql in SQL.items():
    records.append(
        {
            "key": f"extra/{key}",
            "sql": sql,
            "result": lib.attempt(lambda q=sql: lib.answer_of(s.sql(q))),
        }
    )
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print(engine, ansi, len(records), flush=True)
