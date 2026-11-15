import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

SPARK, MAIN, FF34, OLDWIDE, HEAD, OUT = (Path(p) for p in sys.argv[1:7])
OUT.mkdir(parents=True, exist_ok=True)
VARIANTS = ("seat_e", "k1", "k1k2")
HEAD_FILES = {
    "seat_e": "coerce-seat_e-main-ansi-{mode}.jsonl",
    "k1": "coerce-k1r-k1-main-ansi-{mode}.jsonl",
    "k1k2": "coerce-k1r-k1k2-main-ansi-{mode}.jsonl",
}
MODES = ("true", "false")


def lines(path: Path) -> list:
    return [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else []


def cmap(path: Path) -> dict:
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


def family_of(shape: str) -> str:
    return shape.split("_")[0] + ("/d38_18" if shape.endswith("d38_18") else "/other")


report = {"faithful": {}, "kernels": {}, "coerce": {}, "family": {}, "dml": {}}
held = []
for v in VARIANTS:
    for mode in MODES:
        sp_v = cmap(HEAD / f"coerce-{v}-spark-ansi-{mode}.jsonl")
        sp_o = cmap(SPARK / f"coerce-spark-ansi-{mode}.jsonl")
        report["faithful"][f"coerce|{v}|{mode}"] = {
            "cells": len(sp_o),
            "spark_form_differs": sum(1 for k, a in sp_o.items() if sp_v.get(k) != a),
        }
for mode in MODES:
    sp = cmap(HEAD / f"kernels-spark-ansi-{mode}.jsonl")
    mn = cmap(HEAD / f"kernels-main-ansi-{mode}.jsonl")
    mr = cmap(HEAD / f"kernels-k1r-main-ansi-{mode}.jsonl")
    shapes = sorted({k[0].replace("_direct_", "|").replace("_text_", "|") for k in sp})
    for sh in shapes:
        kern, dec = sh.split("|")
        direct = {k[1]: a for k, a in sp.items() if k[0] == f"{kern}_direct_{dec}"}
        sp_text = {k[1]: a for k, a in sp.items() if k[0] == f"{kern}_text_{dec}"}
        mn_text = {k[1]: a for k, a in mn.items() if k[0] == f"{kern}_text_{dec}"}
        mn_direct = {k[1]: a for k, a in mn.items() if k[0] == f"{kern}_direct_{dec}"}
        report["kernels"][f"{mode}|{kern}|{dec}"] = {
            "values": len(direct),
            "spark text route differs from spark": sum(1 for t, a in direct.items() if sp_text.get(t) != a),
            "main text route differs from spark": sum(1 for t, a in direct.items() if mn_text.get(t) != a),
            "main direct cast differs from spark": sum(1 for t, a in direct.items() if mn_direct.get(t) != a),
        }
        if kern in ("k1", "k1f"):
            route = {k[1]: a for k, a in mr.items() if k[0] == f"{kern}_route_{dec}"}
            report["kernels"][f"{mode}|{kern}|{dec}"]["main python half-up route differs from spark"] = sum(
                1 for t, a in direct.items() if route.get(t) != a
            )
for mode in MODES:
    sp = cmap(SPARK / f"coerce-spark-ansi-{mode}.jsonl")
    mn = cmap(MAIN / f"coerce-repark-ansi-{mode}.jsonl")
    ff = cmap(FF34 / f"coerce-repark-ansi-{mode}.jsonl")
    old = cmap(OLDWIDE / f"coerce-ff34-ansi-{mode}.jsonl")
    heads = {v: cmap(HEAD / HEAD_FILES[v].format(mode=mode)) for v in VARIANTS}
    by = defaultdict(Counter)
    for k, s in sp.items():
        m, f, o = mn.get(k) == s, ff.get(k) == s, old.get(k) == s
        for fam in (family_of(k[0]), "all/all"):
            c = by[fam]
            c["cells"] += 1
            c["main wrong"] += not m
            c["old emulated head wrong"] += not o
            c["old emulated head held (right on main or ff34)"] += (not o) and (m or f)
            for v, hd in heads.items():
                h = hd.get(k) == s
                c[f"{v} wrong"] += not h
                c[f"{v} held vs main"] += (not h) and m
                c[f"{v} held vs main or ff34"] += (not h) and (m or f)
                c[f"{v} fixed vs main"] += h and not m
        for v, hd in heads.items():
            if v == "k1k2" and hd.get(k) != s and not (mode == "false" and k[0].endswith("d5_0")):
                held.append({"set": "coerce", "variant": v, "ansi": mode, "shape": k[0], "value": k[1],
                             "spark": s, "main": mn.get(k), "ff34": ff.get(k), "head": hd.get(k)})
    report["coerce"][mode] = {k: dict(c) for k, c in sorted(by.items())}
for mode in MODES:
    sp = json.loads((SPARK / f"family-spark-ansi-{mode}.json").read_text())
    mn = json.loads((MAIN / f"family-repark-ansi-{mode}.json").read_text())
    ff = json.loads((FF34 / f"family-repark-ansi-{mode}.json").read_text())
    for v in ("seat_e", "k1k2"):
        path = HEAD / f"family-{v}-main-ansi-{mode}.json"
        if not path.exists():
            continue
        hd = json.loads(path.read_text())
        for p in ("eq", "lt", "ns", "in"):
            s = set(sp[p])
            wm, wf, wh = s ^ set(mn[p]), s ^ set(ff[p]), s ^ set(hd[p])
            report["family"][f"{mode}|{v}|{p}"] = {
                "cells": sp["rows"],
                "main wrong": len(wm),
                "head wrong": len(wh),
                "held vs main": len(wh - wm),
                "held vs main or ff34": len(wh - (wm & wf)),
            }
for v in ("seat_e", "k1k2"):
    sp_v = dml_map(HEAD / f"dml-{v}-spark-ansi-true.jsonl")
    sp_o = dml_map(SPARK / "dml-spark-ansi-true.jsonl")
    report["faithful"][f"dml|{v}|true"] = {
        "cells": len(sp_o),
        "spark_form_differs": [k for k, a in sp_o.items() if not same(sp_v.get(k), a)],
    }
for mode in MODES:
    sp = dml_map(SPARK / f"dml-spark-ansi-{mode}.jsonl")
    for door in ("repark", "native"):
        mn = dml_map(MAIN / f"dml-{door}-ansi-{mode}.jsonl")
        ff = dml_map(FF34 / f"dml-{door}-ansi-{mode}.jsonl")
        for v in ("seat_e", "k1k2"):
            hd = dml_map(HEAD / f"dml-{door}-{v}-main-ansi-{mode}.jsonl")
            c = Counter()
            for k, s in sp.items():
                if k not in mn:
                    continue
                m, f, h = same(mn[k], s), same(ff.get(k), s), same(hd.get(k), s)
                c["cells"] += 1
                c["main wrong"] += not m
                c["head wrong"] += not h
                c["held vs main"] += (not h) and m
                c["held vs main or ff34"] += (not h) and (m or f)
                c["fixed vs main"] += h and not m
                if not h:
                    held.append({"set": f"dml-{door}", "variant": v, "ansi": mode, "key": k, "spark": s,
                                 "main": mn[k], "ff34": ff.get(k), "head": hd.get(k)})
            report["dml"][f"{mode}|{door}|{v}"] = dict(c)
(OUT / "head-summary.json").write_text(json.dumps(report, indent=1, sort_keys=True, default=str) + "\n")
with (OUT / "head-wrong.jsonl").open("w") as fh:
    for row in held:
        fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")
print(json.dumps(report["faithful"], default=str)[:2000])
print("held rows", Counter((r["set"], r["variant"], r["ansi"]) for r in held))
