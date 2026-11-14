import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

SPARK, MAIN, FF34, WIDE, OUT = (Path(p) for p in sys.argv[1:6])
OUT.mkdir(parents=True, exist_ok=True)


def lines(path: Path) -> list:
    return [json.loads(x) for x in path.read_text().splitlines()]


def coerce_map(path: Path) -> dict:
    return {(r["shape"], r["value"]): r["answer"] for r in lines(path)}


def dml_map(path: Path) -> dict:
    out = {}
    for r in lines(path):
        if r["key"] == "provenance":
            continue
        out[r["key"]] = ("ERR", r.get("head", "")[:60]) if "error" in r else r.get("ids", r.get("answer"))
    return out


def same(a: object, b: object) -> bool:
    if isinstance(a, tuple) and isinstance(b, tuple):
        return True
    return a == b


report = {"faithful": {}, "coerce": {}, "family": {}, "dml": {}}
held = []
sp_w = coerce_map(WIDE / "coerce-spark-ansi-true.jsonl")
sp_t = coerce_map(SPARK / "coerce-spark-ansi-true.jsonl")
report["faithful"]["coerce_spark_true"] = {
    "cells": len(sp_t),
    "widened_differs": sum(1 for k, v in sp_t.items() if sp_w.get(k) != v),
}
for mode in ("true", "false"):
    sp = coerce_map(SPARK / f"coerce-spark-ansi-{mode}.jsonl")
    mn = coerce_map(MAIN / f"coerce-repark-ansi-{mode}.jsonl")
    ff = coerce_map(FF34 / f"coerce-repark-ansi-{mode}.jsonl")
    hd = coerce_map(WIDE / f"coerce-ff34-ansi-{mode}.jsonl")
    by = defaultdict(Counter)
    for k, s in sp.items():
        fam = k[0].split("_")[0] + ("/d38_18" if k[0].endswith("d38_18") else "/other")
        m, f, h = mn.get(k) == s, ff.get(k) == s, hd.get(k) == s
        c = by[fam]
        c["cells"] += 1
        c["head wrong"] += not h
        c["head wrong, main right"] += (not h) and m
        c["head wrong, ff34 right"] += (not h) and f
        c["head right, both wrong"] += h and not m and not f
        if not h and (m or f):
            held.append({"set": "coerce", "ansi": mode, "shape": k[0], "value": k[1], "spark": s,
                         "main": mn.get(k), "ff34": ff.get(k), "head_emulated": hd.get(k)})
    total = Counter()
    for c in by.values():
        total.update(c)
    by["all/all"] = total
    report["coerce"][mode] = {k: dict(v) for k, v in sorted(by.items())}
sp_fw = json.loads((WIDE / "family-spark-ansi-true.json").read_text())
sp_ft = json.loads((SPARK / "family-spark-ansi-true.json").read_text())
report["faithful"]["family_spark_true"] = {
    p: len(set(sp_fw[p]) ^ set(sp_ft[p])) for p in ("eq", "lt", "ns", "in")
}
for mode in ("true", "false"):
    sp = json.loads((SPARK / f"family-spark-ansi-{mode}.json").read_text())
    mn = json.loads((MAIN / f"family-repark-ansi-{mode}.json").read_text())
    ff = json.loads((FF34 / f"family-repark-ansi-{mode}.json").read_text())
    hd = json.loads((WIDE / f"family-ff34-ansi-{mode}.json").read_text())
    for p in ("eq", "lt", "ns", "in"):
        s = set(sp[p])
        wm, wf, wh = s ^ set(mn[p]), s ^ set(ff[p]), s ^ set(hd[p])
        report["family"][f"{mode}|{p}"] = {
            "cells": sp["rows"],
            "head wrong": len(wh),
            "head wrong, main right": len(wh - wm),
            "head wrong, ff34 right": len(wh - wf),
        }
sp_dw = dml_map(WIDE / "dml-spark-ansi-true.jsonl")
sp_dt = dml_map(SPARK / "dml-spark-ansi-true.jsonl")
report["faithful"]["dml_spark_true"] = {
    "cells": len(sp_dt),
    "widened_differs": [k for k, v in sp_dt.items() if not same(sp_dw.get(k), v)],
}
for mode in ("true", "false"):
    sp = dml_map(SPARK / f"dml-spark-ansi-{mode}.jsonl")
    for door in ("repark", "native"):
        mn = dml_map(MAIN / f"dml-{door}-ansi-{mode}.jsonl")
        ff = dml_map(FF34 / f"dml-{door}-ansi-{mode}.jsonl")
        hd = dml_map(WIDE / f"dml-{door}-ff34-ansi-{mode}.jsonl")
        c = Counter()
        for k, s in sp.items():
            if k not in mn:
                continue
            m, f, h = same(mn[k], s), same(ff[k], s), same(hd[k], s)
            c["cells"] += 1
            c["head wrong"] += not h
            c["head wrong, main right"] += (not h) and m
            c["head wrong, ff34 right"] += (not h) and f
            c["head right, both wrong"] += h and not m and not f
            if not h and (m or f):
                held.append({"set": f"dml-{door}", "ansi": mode, "key": k, "spark": s, "main": mn[k],
                             "ff34": ff[k], "head_emulated": hd[k]})
        report["dml"][f"{mode}|{door}"] = dict(c)
report["decimal_to_double"] = {}
d2d_sp = coerce_map(WIDE / "d2d-spark-ansi-true.jsonl")
for build in ("main", "ff34"):
    got = coerce_map(WIDE / f"d2d-{build}-ansi-true.jsonl")
    wrong = Counter(k[0] for k, v in d2d_sp.items() if got.get(k) != v)
    cells = Counter(k[0] for k in d2d_sp)
    report["decimal_to_double"][build] = {s: f"{wrong.get(s, 0)} of {n}" for s, n in sorted(cells.items())}
    for k, v in d2d_sp.items():
        if got.get(k) != v and len(held) < 100000:
            held.append({"set": "decimal-to-double", "build": build, "shape": k[0], "value": k[1], "spark": v, "repark": got.get(k)})
(OUT / "widened-summary.json").write_text(json.dumps(report, indent=1, sort_keys=True, default=str) + "\n")
with (OUT / "widened-held-wrong.jsonl").open("w") as fh:
    for row in held:
        fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")
print(json.dumps(report["faithful"], default=str))
print("held cells wrong on the emulated head:", len(held))
