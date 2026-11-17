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


sn, sr = cells.s_rows()
kn, kr = cells.k_rows()
records.append(
    {"key": "setup/s", "result": answer(f"CREATE VIEW s AS {view_sql(sn, sr, cells.s_types())}")}
)
records.append(
    {"key": "setup/k", "result": answer(f"CREATE VIEW k AS {view_sql(kn, kr, cells.k_types())}")}
)
records.append(
    {
        "key": "setup/sv_sum",
        "result": answer("CREATE VIEW sv_sum AS SELECT g, sum(c38_0) AS m FROM s GROUP BY g"),
    }
)
records.append({"key": "data/s", "result": answer("SELECT * FROM s")})
POS = {
    "ungrouped",
    "grouped",
    "grouped_all",
    "empty_ungrouped",
    "win_rows2",
    "win_part",
    "win_running",
    "distinct",
    "try_grouped",
    "try_ungrouped",
    "plus1",
    "as_string",
    "having",
    "linear",
    "beside",
    "cte",
    "scalar_sub",
    "temp_view",
}
COLS = {"c10_2", "c38_0", "c38_38"}
for key, fam, pos, col, g, table, sql in cells.sum_sql_cells():
    keep = (table == "s" and pos in POS and col in COLS) or table == "k"
    if not keep:
        continue
    records.append(
        {
            "key": key,
            "family": fam,
            "door": "native",
            "pos": pos,
            "col": col,
            "group": g,
            "result": answer(sql),
        }
    )
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print("native", len(records), flush=True)
