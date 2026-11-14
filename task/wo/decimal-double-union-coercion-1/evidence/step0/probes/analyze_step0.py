import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "probes"))
from analyze import verdict

SPARK, MAIN, FF34, OUT = (Path(p) for p in sys.argv[1:5])
BUILDS = (("main", MAIN), ("ff34", FF34))
MODES = tuple(m for m in ("true", "false") if (SPARK / f"coerce-spark-ansi-{m}.jsonl").exists())
OUT.mkdir(parents=True, exist_ok=True)


def load(path: Path) -> dict:
    if not path.exists():
        return {}
    out = {}
    for line in path.read_text().splitlines():
        rec = json.loads(line)
        out[rec.get("key", (rec.get("shape"), rec.get("value")))] = rec
    return out


def shown(rec: dict | None) -> str:
    if rec is None:
        return "missing"
    if "error" in rec:
        return "raises " + str(rec.get("class") or rec.get("head") or rec["error"])[:100]
    if "ids" in rec:
        return str(rec["ids"])
    if "answer" in rec:
        return str(rec["answer"])
    return f"{rec.get('type')} " + ", ".join(str(v).split("|")[0] for v in rec.get("values", []))


def root(key: str) -> str:
    parts = key.split("/")
    if parts[0] in ("extra",) or parts[0].startswith("dfcol") or len(parts) < 3:
        return "dd"
    return "dd" if parts[1].startswith("dec") else "control"


def ok(v: str) -> bool:
    return v in ("equal", "EQUAL", "BOTH-RAISE", "both-raise")


def write_verdict(sw: dict, rw: dict, sr: dict, rr: dict) -> str:
    s_ok, r_ok = sw.get("write") == "ok", rw.get("write") == "ok"
    if not s_ok and not r_ok:
        return "BOTH-RAISE"
    if not r_ok:
        return "REPARK-RAISES"
    if not s_ok:
        return "REPARK-ACCEPTS"
    same = sr.get("rows") == rr.get("rows") and sr.get("spark_read", {}).get("type") == rr.get(
        "spark_read", {}
    ).get("type")
    return "EQUAL" if same else "WRONG-WRITE"


def write_shown(read: dict, write: dict) -> str:
    w = write.get("write")
    prefix = "" if w == "ok" else "write raises " + str(w.get("class") or w.get("head"))[:80] + "; "
    got = read.get("spark_read", {})
    if "error" in got:
        return prefix + "read raises"
    return prefix + f"{got.get('type')} " + "; ".join("=".join(r) for r in read.get("rows", []))


def ids_verdict(sp: dict, rp: dict) -> str:
    se, re_ = "error" in sp, "error" in rp
    if se and re_:
        return "BOTH-RAISE"
    if re_:
        return "REPARK-RAISES"
    if se:
        return "REPARK-ACCEPTS"
    key = "ids" if "ids" in sp else "answer"
    return "EQUAL" if sp.get(key) == rp.get(key) else "WRONG"


cells = []
summary = defaultdict(Counter)
moves = defaultdict(list)


def record(section: str, key: str, mode: str, sp_shown: str, verdicts: dict, shows: dict) -> None:
    row = {"section": section, "key": key, "ansi": mode, "spark": sp_shown}
    for b, _ in BUILDS:
        row[b] = shows[b]
        row[f"{b}_verdict"] = verdicts[b]
        summary[(section, mode, b)][verdicts[b]] += 1
    m, f = ok(verdicts["main"]), ok(verdicts["ff34"])
    trans = "both-ok" if m and f else "fixed-on-ff34" if f else "broken-on-ff34" if m else "both-wrong"
    if not (m and f) and shows["main"] != shows["ff34"] and not m and not f:
        trans = "both-wrong-differently"
    row["transition"] = trans
    summary[(section, mode, "transition")][trans] += 1
    if trans not in ("both-ok",):
        moves[section].append(row)
    cells.append(row)


for mode in MODES:
    for kind, section in (("select", "sql"), ("frames", "dataframe"), ("fexpr", "fexpr")):
        sp = load(SPARK / f"{kind}-spark-ansi-{mode}.jsonl")
        reps = {b: load(d / f"{kind}-repark-ansi-{mode}.jsonl") for b, d in BUILDS}
        for key, s in sp.items():
            if key in ("provenance", "elapsed"):
                continue
            vs = {b: verdict(s, reps[b][key]) if key in reps[b] else "missing" for b, _ in BUILDS}
            record(f"{section}-{root(key)}", key, mode, shown(s), vs, {b: shown(reps[b].get(key)) for b, _ in BUILDS})
    sp = load(SPARK / f"revision-spark-ansi-{mode}.jsonl")
    reps = {b: load(d / f"revision-repark-ansi-{mode}.jsonl") for b, d in BUILDS}
    for key, s in sp.items():
        if key.startswith("select/"):
            vs = {b: verdict(s, reps[b][key]) for b, _ in BUILDS}
            record("revision-select", key, mode, shown(s), vs, {b: shown(reps[b][key]) for b, _ in BUILDS})
    for prefix, section in (("write", "write"), ("revision", "revision-write")):
        sw = load(SPARK / f"{prefix}-spark-ansi-{mode}.jsonl")
        rb = "readback" if prefix == "write" else "revision-readback"
        sr = load(SPARK / f"{rb}-spark-ansi-{mode}.jsonl")
        rws = {b: load(d / f"{prefix}-repark-ansi-{mode}.jsonl") for b, d in BUILDS}
        rrs = {b: load(d / f"{rb}-repark-ansi-{mode}.jsonl") for b, d in BUILDS}
        for key, s in sw.items():
            if "table" not in s:
                continue
            vs = {b: write_verdict(s, rws[b][key], sr[key], rrs[b][key]) for b, _ in BUILDS}
            record(
                section,
                key,
                mode,
                write_shown(sr[key], s),
                vs,
                {b: write_shown(rrs[b][key], rws[b][key]) for b, _ in BUILDS},
            )
    sp = load(SPARK / f"dml-spark-ansi-{mode}.jsonl")
    for door in ("repark", "native"):
        reps = {b: load(d / f"dml-{door}-ansi-{mode}.jsonl") for b, d in BUILDS}
        for key, s in sp.items():
            if key == "provenance" or key not in reps["main"]:
                continue
            vs = {b: ids_verdict(s, reps[b][key]) for b, _ in BUILDS}
            section = ("dml-flip" if key.startswith("flip/") else "dml") + ("" if door == "repark" else "-native")
            record(section, key, mode, shown(s), vs, {b: shown(reps[b][key]) for b, _ in BUILDS})

native_main = load(MAIN / "select-native.jsonl")
native_ff34 = load(FF34 / "select-native.jsonl")
sp_true = load(SPARK / "select-spark-ansi-true.jsonl")
for key, s in sp_true.items():
    if key in ("provenance", "elapsed") or key not in native_main:
        continue
    vs = {"main": verdict(s, native_main[key]), "ff34": verdict(s, native_ff34[key])}
    record(f"native-{root(key)}", key, "native", shown(s), vs, {"main": shown(native_main[key]), "ff34": shown(native_ff34[key])})

family = {}
for mode in MODES:
    sp = json.loads((SPARK / f"family-spark-ansi-{mode}.json").read_text())
    rp = {b: json.loads((d / f"family-repark-ansi-{mode}.json").read_text()) for b, d in BUILDS}
    for pred in ("eq", "lt", "ns", "in"):
        s = set(sp[pred])
        wrong = {b: s ^ set(rp[b][pred]) for b, _ in BUILDS}
        family[(mode, pred)] = {
            "cells": sp["rows"],
            "main_wrong": len(wrong["main"]),
            "ff34_wrong": len(wrong["ff34"]),
            "broken_on_ff34": len(wrong["ff34"] - wrong["main"]),
            "fixed_on_ff34": len(wrong["main"] - wrong["ff34"]),
            "both_wrong": len(wrong["main"] & wrong["ff34"]),
        }

coerce = {}
coerce_moves = []
for mode in MODES:
    sp = load(SPARK / f"coerce-spark-ansi-{mode}.jsonl")
    rp = {b: load(d / f"coerce-repark-ansi-{mode}.jsonl") for b, d in BUILDS}
    by_shape = defaultdict(Counter)
    for key, s in sp.items():
        shape = key[0]
        fam = shape.split("_")[0]
        scale = "d38_18" if shape.endswith("d38_18") else "other"
        a = {b: (rp[b].get(key) or {}).get("answer", "MISSING") for b, _ in BUILDS}
        m, f = a["main"] == s["answer"], a["ff34"] == s["answer"]
        t = "both-ok" if m and f else "fixed-on-ff34" if f else "broken-on-ff34" if m else "both-wrong"
        by_shape[(fam, scale)][t] += 1
        by_shape[("all", "all")][t] += 1
        if t != "both-ok" and t != "both-wrong":
            coerce_moves.append(
                {"ansi": mode, "shape": shape, "value": key[1], "spark": s["answer"], **a, "transition": t}
            )
    coerce[mode] = {f"{k[0]}/{k[1]}": dict(v) for k, v in sorted(by_shape.items())}

replay = {}
for b, d in BUILDS:
    rec = load(d / "replay-888-repark.jsonl")
    replay[b] = {k: r for k, r in rec.items() if k != "provenance"}
replay_counts = defaultdict(Counter)
replay_moves = []
for key, r in replay["main"].items():
    f = replay["ff34"].get(key, {})
    replay_counts[(r.get("root"), "main")][r.get("verdict")] += 1
    replay_counts[(r.get("root"), "ff34")][f.get("verdict")] += 1
    if r.get("got") != f.get("got") or r.get("verdict") != f.get("verdict"):
        replay_moves.append({"key": key, "expect": r.get("expect"), "main": r.get("got"), "ff34": f.get("got"), "main_verdict": r.get("verdict"), "ff34_verdict": f.get("verdict")})

with (OUT / "cells.jsonl").open("w") as fh:
    for row in cells:
        fh.write(json.dumps(row, sort_keys=True) + "\n")
with (OUT / "coerce-moves.jsonl").open("w") as fh:
    for row in coerce_moves:
        fh.write(json.dumps(row, sort_keys=True) + "\n")
with (OUT / "replay-888-moves.jsonl").open("w") as fh:
    for row in replay_moves:
        fh.write(json.dumps(row, sort_keys=True) + "\n")
summary_out = {
    "cells": {"|".join(k): dict(v) for k, v in sorted(summary.items())},
    "family": {"|".join(k): v for k, v in sorted(family.items())},
    "coerce": coerce,
    "replay_888": {f"{k[0]}|{k[1]}": dict(v) for k, v in sorted(replay_counts.items(), key=str)},
}
(OUT / "summary.json").write_text(json.dumps(summary_out, indent=1, sort_keys=True) + "\n")
print("cells", len(cells), "coerce moves", len(coerce_moves), "replay moves", len(replay_moves))
