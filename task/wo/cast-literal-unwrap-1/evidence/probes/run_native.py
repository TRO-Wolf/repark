import json
import sys
import time
from collections.abc import Callable
from pathlib import Path

import cells

import repark
import repark._native as native

out, pq_root = sys.argv[1:3]
print("native", native.__file__, flush=True)
records = [
    {
        "key": "provenance",
        "engine": "native",
        "native": "<scratchpad>/" + native.__file__.split("/scratchpad/", 1)[-1],
    }
]


def err(e: Exception) -> str:
    return f"ERR:{type(e).__name__}:None:{' '.join(str(e).split())[:200]}"


def answer(run: Callable[[], object]) -> object:
    try:
        return run()
    except Exception as e:
        return err(e)


def ids(sql: str) -> Callable[[], object]:
    return lambda: sorted(int(r[0]) for r in repark.sql(sql).collect())


def view_sql(kind: cells.Kind) -> str:
    items = ", ".join(f"CAST({v} AS {kind.col_type})" for v in kind.values)
    rows = ", ".join(f"({i + 1})" for i in range(len(kind.values)))
    return (
        "SELECT CAST(column1 AS INT) AS id, "
        f"array_element(make_array({items}), CAST(column1 AS INT)) AS c "
        f"FROM (VALUES {rows})"
    )


start = time.time()
for kind in cells.KINDS:
    view = f"nv_{kind.name}"
    setup = answer(lambda k=kind, v=view: repark.sql(f"CREATE VIEW {v} AS {view_sql(k)}").collect())
    if isinstance(setup, str):
        records.append(
            {"key": f"{kind.name}/native_view/setup", "kind": kind.name, "result": setup}
        )
        continue
    path = str(Path(pq_root) / f"pq_{kind.name}") + "/"
    tables = {"native_view": view}
    made = answer(
        lambda k=kind, p=path: repark.sql(
            f"CREATE EXTERNAL TABLE np_{k.name} STORED AS PARQUET LOCATION '{p}'"
        ).collect()
    )
    if isinstance(made, str):
        records.append({"key": f"{kind.name}/native_parquet/setup", "result": made})
    else:
        tables["native_parquet"] = f"np_{kind.name}"
    for storage, table in tables.items():
        dense = storage == "native_view"
        for ckey, pred, _const in cells.where_cells(kind, dense):
            sql = f"SELECT id FROM {table} WHERE {pred}"
            records.append(
                {
                    "key": f"{kind.name}/{storage}/where/{ckey}",
                    "kind": kind.name,
                    "storage": storage,
                    "door": "native",
                    "pos": "where",
                    "sql": sql,
                    "result": answer(ids(sql)),
                }
            )
        for pkey, sql in cells.position_cells(kind, table, table):
            records.append(
                {
                    "key": f"{kind.name}/{storage}/{pkey}",
                    "kind": kind.name,
                    "storage": storage,
                    "door": "native",
                    "pos": pkey.split("/")[0],
                    "sql": sql,
                    "result": answer(
                        lambda q=sql: sorted(
                            ":".join("null" if x is None else str(x).lower() for x in r)
                            for r in repark.sql(q).collect()
                        )
                    ),
                }
            )
    print(kind.name, len(records), round(time.time() - start, 1), flush=True)
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print("native", len(records), round(time.time() - start, 1))
