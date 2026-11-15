import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

NEW, OLD, OUT = (Path(p) for p in sys.argv[1:4])
OUT.mkdir(parents=True, exist_ok=True)
OK = ("equal", "EQUAL", "BOTH-RAISE", "both-raise")
NESTED = (
    "select/struct_",
    "select/array_union",
    "select/array_case",
    "select/map_union",
    "select/map_case",
)
Q8 = ("merge_on_zero_today", "merge_on_zero_widened")


def lines(path: Path) -> list:
    return [json.loads(x) for x in path.read_text().splitlines()]


def part_of(section: str, key: str) -> str:
    name = key.split("/")
    head = name[0]
    if section.startswith("native") or section.endswith("-native"):
        return "native (Q2)"
    if section.endswith("-control"):
        return "control root (not this unit)"
    if any(key.startswith(n) for n in NESTED):
        return "card DECIMAL-DOUBLE-NESTED-1"
    if "rcte" in key:
        return "excluded: recursive CTE (RECURSIVE-CTE-JOIN-ERROR-1)"
    if head == "ctl_add":
        return "card DECIMAL-FLOAT-ARITH-1"
    if key in Q8:
        return "Q8 PRUNED-WRONG (main's answer kept)"
    if head in ("values", "values_rev") or "values" in key:
        return "part 2 (seat V)"
    return "part 1"


def ok(v: str) -> bool:
    return v in OK


old = {(r["section"], r["key"], r["ansi"]): r for r in lines(OLD / "cells.jsonl")}
rows = []
counts = defaultdict(Counter)
moved = []
for r in lines(NEW / "cells.jsonl"):
    k = (r["section"], r["key"], r["ansi"])
    o = old.get(k)
    r["main_6f9f"] = o["main"] if o else None
    r["main_6f9f_verdict"] = o["main_verdict"] if o else None
    r["part"] = part_of(r["section"], r["key"])
    rows.append(r)
    m = ok(r["main_verdict"])
    c = counts[(r["section"], r["ansi"])]
    c["cells"] += 1
    c["main wrong"] += not m
    c["ff34 wrong"] += not ok(r["ff34_verdict"])
    if o is not None:
        c["6f9f wrong"] += not ok(o["main_verdict"])
        if o["main"] != r["main"] or ok(o["main_verdict"]) != m:
            moved.append(r)
            c["moved since 6f9f"] += 1
            c["fixed since 6f9f"] += m and not ok(o["main_verdict"])
            c["broken since 6f9f"] += (not m) and ok(o["main_verdict"])
    if not m:
        counts[("wrong on main by part", r["ansi"])][r["part"]] += 1
        counts[("wrong on main by part and section", r["ansi"] + "|" + r["part"])][
            r["section"]
        ] += 1
with (OUT / "cells.jsonl").open("w") as fh:
    for r in rows:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
with (OUT / "moved-since-6f9f.jsonl").open("w") as fh:
    for r in moved:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
(OUT / "summary.json").write_text(
    json.dumps({"|".join(k): dict(v) for k, v in sorted(counts.items())}, indent=1, sort_keys=True)
    + "\n"
)
print("cells", len(rows), "moved since 6f9f", len(moved))
