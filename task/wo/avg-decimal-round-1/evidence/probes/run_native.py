import json
import sys
from pathlib import Path

import cells
import lib

import repark
import repark._native as native

out = sys.argv[1]
print("native", native.__file__, flush=True)
records = [
    {
        "key": "provenance",
        "engine": "native",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
]


def answer(sql: str) -> object:
    return lib.attempt(lambda: lib.answer_of(repark.sql(sql)))


def view_sql(names: list, rows: list, types: dict) -> str:
    body = ", ".join(
        "(" + ", ".join("NULL" if v is None else f"'{v}'" for v in r) + ")" for r in rows
    )
    items = ", ".join(
        f"CAST(column{n + 1} AS {types[name]}) AS {name}" for n, name in enumerate(names)
    )
    return f"SELECT {items} FROM (VALUES {body})"


an, ar = cells.avg_rows()
dn, dr = cells.div_rows()
records.append(
    {"key": "setup/t", "result": answer(f"CREATE VIEW t AS {view_sql(an, ar, cells.avg_types())}")}
)
records.append(
    {"key": "setup/v", "result": answer(f"CREATE VIEW v AS {view_sql(dn, dr, cells.div_types())}")}
)
AVG_SAMPLE = {"c10_0", "c10_2", "c28_2", "c34_4", "c38_0", "c38_18"}
AVG_POS = {
    "ungrouped",
    "grouped",
    "win_rows3",
    "win_running",
    "distinct",
    "mean",
    "having_eq",
    "try_avg",
}
for key, fam, pos, col, sql in cells.avg_sql_cells("t"):
    if fam == "avg" and (col not in AVG_SAMPLE or pos not in AVG_POS):
        continue
    records.append(
        {
            "key": key,
            "family": fam,
            "door": "sql",
            "pos": pos,
            "col": col,
            "sql": sql,
            "result": answer(sql),
        }
    )
DIV_SAMPLE = {"d10_2", "d28_2", "d38_2", "d10_0"}
DIV_LITERALS = {"i3", "l3", "neg3", "i100", "big3e9"}
for key, fam, pos, col, sql in cells.div_sql_cells("v"):
    if col not in DIV_SAMPLE and col != "lit":
        continue
    if pos in ("lit_right", "lit_left") and key.rsplit("/", 1)[1] not in DIV_LITERALS:
        continue
    records.append(
        {
            "key": key,
            "family": fam,
            "door": "sql",
            "pos": pos,
            "col": col,
            "sql": sql,
            "result": answer(sql),
        }
    )
records.append(
    {
        "key": "conf/set/apl",
        "result": answer("SET spark.sql.decimalOperations.allowPrecisionLoss = false"),
    }
)
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print("native", len(records), flush=True)
