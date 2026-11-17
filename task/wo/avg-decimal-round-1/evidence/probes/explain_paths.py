import sys
import tempfile
from pathlib import Path

import cells
import lib

import repark

wh_parent, out = sys.argv[1:3]
wh = tempfile.mkdtemp(prefix="explain-", dir=wh_parent)
s, sf, _, prov = lib.session("repark", "true", wh, "unused.jar", {})
names, rows = cells.avg_rows()
types = cells.avg_types()
raw = s.createDataFrame(rows, ", ".join(f"{n} STRING" for n in names))
raw.selectExpr(*[f"CAST({n} AS {types[n]}) AS {n}" for n in names]).createOrReplaceTempView("t")
dn, dr = cells.div_rows()
dtypes = cells.div_types()
rawv = s.createDataFrame(dr, ", ".join(f"{n} STRING" for n in dn))
rawv.selectExpr(*[f"CAST({n} AS {dtypes[n]}) AS {n}" for n in dn]).createOrReplaceTempView("v")
SQL = {
    "avg_distinct": "SELECT g, avg(DISTINCT c10_2) AS m FROM t GROUP BY g",
    "avg_grouped": "SELECT g, avg(c10_2) AS m FROM t GROUP BY g",
    "avg_rows3": (
        "SELECT g, avg(c10_2) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 2 PRECEDING AND "
        "CURRENT ROW) AS m FROM t"
    ),
    "div_lit": "SELECT d10_2 / 3 AS q FROM v",
    "mul_lit": "SELECT d10_2 * 3 AS q FROM v",
    "mod_lit": "SELECT d10_2 % 3 AS q FROM v",
}
lines = [f"provenance: {prov}"]
for key, sql in SQL.items():
    res = lib.attempt(lambda q=sql: s.sql(f"EXPLAIN {q}").collect())
    lines.append(f"== spark door {key}: {sql}")
    if isinstance(res, str):
        lines.append(res)
    else:
        for r in res:
            lines.append(" | ".join(str(x) for x in r))
lines.append("== DataFrame col / 3 schema")
frame = s.table("v").select((sf.col("d10_2") / 3).alias("q"))
lines.append(lib.attempt(lambda: str(frame.schema)))


def view_sql(cols: list, data: list, tys: dict) -> str:
    body = ", ".join(
        "(" + ", ".join("NULL" if v is None else f"'{v}'" for v in r) + ")" for r in data
    )
    items = ", ".join(f"CAST(column{n + 1} AS {tys[c]}) AS {c}" for n, c in enumerate(cols))
    return f"SELECT {items} FROM (VALUES {body})"


lines.append(
    lib.attempt(
        lambda: repark.sql(f"CREATE VIEW nt AS {view_sql(names, rows, types)}").collect() and "ok"
    )
)
lines.append(
    lib.attempt(
        lambda: repark.sql(f"CREATE VIEW nv AS {view_sql(dn, dr, dtypes)}").collect() and "ok"
    )
)
for key, sql in (
    (
        "avg_distinct_with_count",
        "SELECT g, avg(DISTINCT c10_2) AS m, count(c10_2) AS n FROM t GROUP BY g",
    ),
    ("avg_distinct_window", "SELECT g, avg(DISTINCT c10_2) OVER (PARTITION BY g) AS m FROM t"),
):
    lines.append(f"== spark door answer {key}: {sql}")
    lines.append(str(lib.attempt(lambda q=sql: lib.answer_of(s.sql(q)))))
for key in ("avg_distinct", "div_lit"):
    sql = SQL[key].replace(" FROM t", " FROM nt").replace(" FROM v", " FROM nv")
    lines.append(f"== native door {key}: {sql}")
    res = lib.attempt(lambda q=sql: repark.sql(f"EXPLAIN {q}").collect())
    if isinstance(res, str):
        lines.append(res)
    else:
        for r in res:
            lines.append(" | ".join(str(x) for x in r))
text = "\n".join(str(x) for x in lines)
text = text.replace(wh, "<warehouse>")
Path(out).write_text(text + "\n")
print(len(lines))
