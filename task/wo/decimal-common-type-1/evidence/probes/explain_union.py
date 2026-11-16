import sys
from pathlib import Path

import cells
import run_cells_lib as lib

out = sys.argv[1]
s, _, prov = lib.session("repark", "false", sys.argv[2], "")
s.range(8).selectExpr(*cells.setup_exprs()).filter("id <= 6").createOrReplaceTempView("t")
lines = [f"provenance: {prov}"]
for label, sql in (
    ("sql_flat", "SELECT c AS v FROM t UNION ALL SELECT d FROM t UNION ALL SELECT a FROM t"),
    (
        "sql_paren_right",
        "SELECT a AS v FROM t UNION ALL (SELECT c FROM t UNION ALL SELECT d FROM t)",
    ),
):
    rows = s.sql("EXPLAIN VERBOSE " + sql).collect()
    for r in rows:
        if str(r[0]).startswith(("initial_logical_plan", "logical_plan after type_coercion")):
            lines.append(f"== {label} / {r[0]}")
            lines.extend(str(r[1]).splitlines())
keep = [
    x
    for x in lines
    if x.startswith(("provenance", "== ")) or x.strip().startswith(("Union", "Projection: t."))
]
keep = [
    x
    for x in keep
    if "after type_coercion" not in x and (not x.strip().startswith("Projection") or len(x) < 60)
]
Path(out).write_text("\n".join(keep) + "\n")
print(len(lines))
