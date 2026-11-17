import json
import sys
import time
from decimal import Decimal
from pathlib import Path

import cells

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


def err(e: Exception) -> str:
    return f"ERR:{type(e).__name__}:None:{' '.join(str(e).split())[:240]}"


def norm(v: object) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return str(v).lower()
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, dict):
        return (
            "{"
            + ",".join(
                f"{norm(k)}->{norm(x)}" for k, x in sorted(v.items(), key=lambda i: str(i[0]))
            )
            + "}"
        )
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(norm(x) for x in v) + "]"
    return str(v)


def answer(sql: str) -> object:
    try:
        frame = repark.sql(sql)
        rows = frame.collect()
        types = [f.dataType.simpleString() for f in frame.schema.fields]
        return {"rows": sorted("|".join(norm(x) for x in r) for r in rows), "types": types}
    except Exception as e:
        return err(e)


def record(key: str, group: str, pos: str, pair: str, sql: str) -> None:
    records.append(
        {
            "key": key,
            "group": group,
            "door": "native",
            "pos": pos,
            "pair": pair,
            "sql": sql,
            "result": answer(sql),
        }
    )


def view_sql(lo: int, hi: int) -> str:
    rows = []
    for i in range(lo, hi):
        rows.append(
            "("
            + ", ".join("NULL" if v[i] is None else repr(v[i]) for _, _, v in cells.COLUMNS)
            + ")"
        )
    items = ", ".join(
        f"CAST(column{n + 1} AS {ty}) AS {name}" for n, (name, ty, _) in enumerate(cells.COLUMNS)
    )
    return f"SELECT {items} FROM (VALUES {', '.join(rows)})"


start = time.time()
for name, lo, hi in (("t", 0, 6), ("tx", 6, 8)):
    res = answer(f"CREATE VIEW {name} AS {view_sql(lo, hi)}")
    records.append({"key": f"setup/{name}", "result": res})
records.append({"key": "data/t", "result": answer("SELECT * FROM t")})
records.append({"key": "data/tx", "result": answer("SELECT * FROM tx")})
for t in ("t", "tx"):
    for key, family, pos, tmpl in cells.pair_cells():
        if t == "tx" and pos in cells.T_ONLY:
            continue
        record(f"pair/{key}/{t}/{pos}", "pair:" + family, pos, key, tmpl.replace("{t}", t))
    for ck, pred in cells.CONSTS:
        record(f"const/{ck}/{t}/select", "const", "select", ck, f"SELECT id, {pred} AS r FROM {t}")
        record(f"const/{ck}/{t}/where", "const", "where", ck, f"SELECT id FROM {t} WHERE {pred}")
    for group, table in (
        ("three", cells.THREE),
        ("agg", cells.AGGS),
        ("literal", cells.LITERALS),
        ("special", cells.SPECIAL),
    ):
        for ck, tmpl in table.items():
            record(f"{group}/{ck}/{t}", group, ck, ck, tmpl.replace("{t}", t))
for ck, tmpl in cells.VALUES.items():
    record(f"values/{ck}", "values", ck, ck, tmpl.replace("{t}", "t"))
records.append({"key": "elapsed", "seconds": round(time.time() - start, 1)})
with Path(out).open("w") as fh:
    for r in records:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
print("native", len(records), round(time.time() - start, 1))
