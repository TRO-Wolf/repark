import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

from f32 import nearest_f32
from model import show_float, spark_round

raw = Path(sys.argv[1])
values = json.loads(Path(sys.argv[2]).read_text())
out = Path(sys.argv[3])
MODES = ["true", "false"]
CLASS = {
    "DOUBLE": {str(r["id"]): r["class"] for r in values["double"]},
    "FLOAT": {str(r["id"]): r["class"] for r in values["float"]},
}


def load(name: str) -> dict:
    p = raw / name
    if not p.exists():
        return {}
    return {r["key"]: r for r in (json.loads(x) for x in p.read_text().splitlines())}


def family(cls: str) -> str:
    if cls.startswith("half/"):
        return "half " + cls.rsplit("/", 1)[1]
    return cls


def is_zero(v: object) -> bool:
    return isinstance(v, str) and "|" in v and float(v.split("|")[0]) == 0.0


def classify(sv: object, rv: object) -> str:
    if isinstance(sv, dict) and isinstance(rv, dict):
        return "EQUAL-ERR"
    if isinstance(rv, dict):
        return "ERR-FOR-VALUE"
    if isinstance(sv, dict):
        return "VALUE-FOR-ERR"
    if sv == rv:
        return "EQUAL"
    if is_zero(sv) and is_zero(rv):
        return "SIGN-ZERO"
    if sv is None or rv is None:
        return "NULL-MISMATCH"
    return "WRONG-VALUE"


def err(rec: dict) -> dict | None:
    if "error" in rec:
        return {"error": rec["error"], "class": rec.get("class"), "head": rec.get("head", "")[:120]}
    return None


def cells_of(rec: dict, keys: list) -> dict:
    e = err(rec)
    if e is not None:
        return dict.fromkeys(keys, e)
    rows = rec.get("rows", {})
    return {k: rows.get(k, "MISSING") for k in keys}


summary = Counter()
wrong_matrix = defaultdict(Counter)
type_diff = []
examples = defaultdict(list)
model_bad = []
kcheck_bad = []
errors_seen = Counter()
door_total = Counter()
for mode in MODES:
    sp = load(f"spark-ansi-{mode}.jsonl")
    rp = load(f"repark-ansi-{mode}.jsonl")
    if not sp or not rp:
        continue
    src_rec = {src: sp[f"source/{src}"] for src in ("DOUBLE", "FLOAT")}
    for src in ("DOUBLE", "FLOAT"):
        if src_rec[src]["rows"] != rp[f"source/{src}"].get("rows"):
            print("SOURCE MISMATCH spark-door", src, mode)
        if src_rec[src]["rows"] != rp[f"source/{src}"].get("native_rows"):
            print("SOURCE MISMATCH native", src, mode)
    for key, rrec in rp.items():
        ctx = rrec.get("ctx")
        if ctx not in ("sel", "lit", "store", "where", "group"):
            continue
        door, src, fn = rrec["door"], rrec["src"], rrec["fn"]
        skey = key.replace("/native/", "/sql/", 1) if door == "native" else key
        srec = sp.get(skey)
        if srec is None:
            print("no spark", skey)
            continue
        target = rrec.get("target", "")
        tag = (mode, ctx, door, src, fn, target)
        if e := err(rrec):
            errors_seen[(ctx, door, src, fn, e["head"][:90])] += 1
        if ctx in ("sel", "store"):
            if (
                ctx == "sel"
                and rrec.get("type")
                and srec.get("type")
                and rrec["type"] != srec["type"]
            ):
                type_diff.append((mode, key, srec["type"], rrec["type"]))
            keys = sorted(set(srec.get("rows", {})) | set(rrec.get("rows", {})), key=int)
            if not keys:
                keys = sorted(CLASS[src], key=int)
            sc_cells = cells_of(srec, keys)
            rc_cells = cells_of(rrec, keys)
            for k in keys:
                c = classify(sc_cells[k], rc_cells[k])
                summary[(*tag, c)] += 1
                door_total[(mode, door, ctx, c == "EQUAL")] += 1
                if c != "EQUAL":
                    wrong_matrix[(ctx, door, src, fn, target)][
                        (family(CLASS[src][k]), rrec["scale"])
                    ] += 1
                    if len(examples[(ctx, door, src, fn, target, mode)]) < 400:
                        examples[(ctx, door, src, fn, target, mode)].append(
                            (
                                rrec["scale"],
                                k,
                                CLASS[src][k],
                                src_rec[src]["text"][k]
                                if src == "DOUBLE"
                                else src_rec[src]["dtext"][k],
                                sc_cells[k],
                                rc_cells[k],
                                c,
                            )
                        )
        elif ctx == "lit":
            keys = sorted(set(srec.get("rows", {})) | set(rrec.get("rows", {})))
            if not keys:
                continue
            sc_cells = cells_of(srec, keys)
            rc_cells = cells_of(rrec, keys)
            if srec.get("types") != rrec.get("types"):
                type_diff.append((mode, key, srec.get("types"), rrec.get("types")))
            for k in keys:
                c = classify(sc_cells[k], rc_cells[k])
                summary[(*tag, c)] += 1
                door_total[(mode, door, ctx, c == "EQUAL")] += 1
                i, sc = k.split("@")
                if c != "EQUAL":
                    wrong_matrix[(ctx, door, src, fn, "")][(family(CLASS[src][i]), int(sc))] += 1
        elif ctx == "where" and rrec["op"] == "=":
            sc = rrec["scale"]
            base = key.rsplit("/", 1)[0]

            def klass(recs: dict, b: str) -> dict:
                out_ = {}
                for op, name in ((">", "gt"), ("=", "eq"), ("<", "lt")):
                    rr = recs.get(f"{b}/{op}")
                    if rr is None or "error" in rr:
                        return {"__error__": err(rr) if rr else "missing"}
                    for i in rr["ids"]:
                        out_[str(i)] = name
                return out_

            sb = base.replace("/native/", "/sql/", 1) if door == "native" else base
            sk = klass(sp, sb)
            rk = klass(rp, base)
            for i, t in src_rec[src]["text"].items():
                if t == "NaN":
                    continue
                if "__error__" in sk or "__error__" in rk:
                    c = (
                        "EQUAL-ERR"
                        if "__error__" in sk and "__error__" in rk
                        else ("ERR-FOR-VALUE" if "__error__" in rk else "VALUE-FOR-ERR")
                    )
                else:
                    c = "EQUAL" if sk.get(i, "none") == rk.get(i, "none") else "WRONG-VALUE"
                summary[(mode, ctx, door, src, fn, "", c)] += 1
                door_total[(mode, door, ctx, c == "EQUAL")] += 1
                if c != "EQUAL":
                    wrong_matrix[(ctx, door, src, fn, "")][(family(CLASS[src][i]), sc)] += 1
        elif ctx == "group":
            sg = {g[0]: (g[1], g[2]) for g in srec.get("groups", [])}
            rg = {g[0]: (g[1], g[2]) for g in rrec.get("groups", [])}
            if err(rrec) or err(srec):
                c = (
                    "EQUAL-ERR"
                    if err(rrec) and err(srec)
                    else ("ERR-FOR-VALUE" if err(rrec) else "VALUE-FOR-ERR")
                )
                summary[(*tag, c)] += max(len(sg), 1)
                door_total[(mode, door, ctx, False)] += max(len(sg), 1)
                continue
            for lo in sorted(set(sg) | set(rg), key=int):
                a, bb = sg.get(lo), rg.get(lo)
                if a == bb:
                    c = "EQUAL"
                elif a and bb and a[1] == bb[1] and is_zero(a[0]) and is_zero(bb[0]):
                    c = "SIGN-ZERO"
                else:
                    c = "WRONG-VALUE"
                summary[(*tag, c)] += 1
                door_total[(mode, door, ctx, c == "EQUAL")] += 1
                if c != "EQUAL":
                    wrong_matrix[(ctx, door, src, fn, "")][
                        (family(CLASS[src][lo]), rrec["scale"])
                    ] += 1
    for src in ("DOUBLE", "FLOAT"):
        texts = src_rec[src]["text"] if src == "DOUBLE" else src_rec[src]["dtext"]
        for fn in ("round", "bround"):
            for sc in values["scales"]:
                rows = sp[f"sel/sql/{src}/{fn}/{sc}"]["rows"]
                for i, t in texts.items():
                    want = spark_round(t, sc, fn, src)
                    if rows[i] != want:
                        model_bad.append((mode, src, fn, sc, i, t, rows[i], want))
    kc = load("kcheck.jsonl")
    for key, rec in kc.items():
        if not key.startswith("kcheck/"):
            continue
        src, sc = rec["src"], rec["scale"]
        rows = sp[f"sel/sql/{src}/round/{sc}"]["rows"]
        if "error" in rec:
            kcheck_bad.append((mode, key, "error", rec.get("head")))
            continue
        for i, d in rec["rows"].items():
            q = Decimal(d[4:])
            got = (
                show_float(0.0)
                if q == 0
                else (
                    show_float(nearest_f32(Fraction(q))) if src == "FLOAT" else show_float(float(q))
                )
            )
            if rows[i] != got:
                kcheck_bad.append((mode, src, sc, i, d, rows[i], got))
        summary[("kcheck", src, sc, "rows")] = len(rec["rows"])
res = {
    "summary": [
        [*list(k), v] for k, v in sorted(summary.items(), key=lambda x: tuple(map(str, x[0])))
    ],
    "door_total": [
        [*list(k), v] for k, v in sorted(door_total.items(), key=lambda x: tuple(map(str, x[0])))
    ],
    "wrong_matrix": {
        "|".join(map(str, k)): {f"{a}@{b}": n for (a, b), n in v.items()}
        for k, v in wrong_matrix.items()
    },
    "type_diff": type_diff,
    "examples": {"|".join(map(str, k)): v for k, v in examples.items()},
    "model_bad": model_bad,
    "kcheck_bad": kcheck_bad,
    "errors_seen": [[*list(k), v] for k, v in errors_seen.items()],
}
out.write_text(json.dumps(res, indent=0))
print("model_bad", len(model_bad), "kcheck_bad", len(kcheck_bad), "type_diff", len(type_diff))
