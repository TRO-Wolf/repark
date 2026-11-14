import json
import re
import sys
from collections import Counter
from pathlib import Path


def bits(v: object) -> object:
    if v is None:
        return "NULL"
    if isinstance(v, dict) or v == "ERR":
        return "ERR"
    if v == "NaN":
        return "NaN"
    return v.split("|")[1] if "|" in v else v


def norm_type(t: object) -> object:
    t = str(t)
    t = {"int32": "int", "int64": "bigint", "int16": "smallint", "int8": "tinyint"}.get(t, t)
    m = re.match(r"decimal128\((\d+), (\d+)\)", t)
    return f"decimal({m.group(1)},{m.group(2)})" if m else t


def misc_verdict(s: object, r: object) -> str:
    if s is None or r is None:
        return "MISSING"
    se, re_ = "error" in s, "error" in r
    if se and re_:
        return "EQUAL-ERR"
    if re_:
        return "ERR-FOR-VALUE"
    if se:
        return "VALUE-FOR-ERR"
    tv = [norm_type(x) for x in s["types"]] == [norm_type(x) for x in r["types"]]
    rows_s = [[bits(x) for x in row] for row in s["rows"]]
    rows_r = [[bits(x) for x in row] for row in r["rows"]]
    if rows_s == rows_r:
        return "EQUAL" if tv else "TYPE-ONLY"
    zs = [
        [("0" if x in ("0000000000000000", "8000000000000000") else x) for x in row]
        for row in rows_s
    ]
    zr = [
        [("0" if x in ("0000000000000000", "8000000000000000") else x) for x in row]
        for row in rows_r
    ]
    if zs == zr:
        return "SIGN-ZERO" if tv else "SIGN-ZERO+TYPE"
    return "WRONG-VALUE" if tv else "WRONG-VALUE+TYPE"


base_dir, head_dir, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])


def load(d: Path, name: str) -> dict:
    return {json.loads(x)["key"]: json.loads(x) for x in (d / name).read_text().splitlines()}


def val(rec: dict | None, k: str) -> object:
    if rec is None:
        return "MISSING"
    if "error" in rec:
        return "ERR"
    return rec.get("rows", {}).get(k, "MISSING")


def where_class(recs: dict, base: str) -> dict:
    out_ = {}
    for op, name in ((">", "gt"), ("=", "eq"), ("<", "lt")):
        r = recs.get(f"{base}/{op}")
        if r is None or "error" in r:
            return {"__err__": True}
        for i in r["ids"]:
            out_[str(i)] = name
    return out_


moves = Counter()
moved_examples = []
remaining = Counter()
for mode in sys.argv[5].split(",") if len(sys.argv) > 5 else ("true", "false"):
    sp = load(base_dir, f"spark-ansi-{mode}.jsonl")
    rb = load(base_dir, f"repark-ansi-{mode}.jsonl")
    rh = load(head_dir, f"repark-ansi-{mode}.jsonl")
    nan_ids = {
        s: {i for i, t in sp[f"source/{s}"]["text"].items() if t == "NaN"}
        for s in ("DOUBLE", "FLOAT")
    }
    for key, b in rb.items():
        ctx = b.get("ctx")
        if ctx not in ("sel", "lit", "store", "where", "group"):
            continue
        h = rh.get(key)
        door = b["door"]
        skey = key.replace("/native/", "/sql/", 1) if door == "native" else key
        if ctx == "where":
            if b["op"] != "=":
                continue
            base = key.rsplit("/", 1)[0]
            sb = base.replace("/native/", "/sql/", 1) if door == "native" else base
            s_c, b_c, h_c = where_class(sp, sb), where_class(rb, base), where_class(rh, base)
            cells = [
                (
                    i,
                    s_c.get(i, "none"),
                    b_c.get(i, "ERR" if "__err__" in b_c else "none"),
                    h_c.get(i, "ERR" if "__err__" in h_c else "none"),
                )
                for i in sp[f"source/{b['src']}"]["text"]
                if i not in nan_ids[b["src"]]
            ]
            if "__err__" in s_c:
                cells = [(i, "ERR", x, y) for i, _, x, y in cells]
        elif ctx == "group":
            s, hh = sp[skey], h or {}
            sg = {g[0]: tuple(g[1:]) for g in s.get("groups", [])}
            bg = {g[0]: tuple(g[1:]) for g in b.get("groups", [])} if "error" not in b else None
            hg = {g[0]: tuple(g[1:]) for g in hh.get("groups", [])} if "error" not in hh else None
            keys = set(sg) | set(bg or {}) | set(hg or {})
            cells = [
                (
                    lo,
                    sg.get(lo),
                    "ERR" if bg is None else bg.get(lo),
                    "ERR" if hg is None else hg.get(lo),
                )
                for lo in keys
            ]
        else:
            s = sp[skey]
            keys = set(s.get("rows", {})) | set(b.get("rows", {}))
            cells = [(k, val(s, k), val(b, k), val(h, k)) for k in keys]
        for k, sv, bv, hv in cells:
            was, now = bv == sv, hv == sv
            tag = (ctx, door, b["src"], b["fn"])
            moves[("EQUAL" if was else "DIFF") + "->" + ("EQUAL" if now else "DIFF")] += 1
            if was and not now and len(moved_examples) < 50:
                moved_examples.append((mode, key, k, sv, bv, hv))
            if not now:
                remaining[tag] += 1
    ms = load(base_dir, f"misc-spark-ansi-{mode}.jsonl")
    mb = load(base_dir, f"misc-repark-ansi-{mode}.jsonl")
    mh = load(head_dir, f"misc-repark-ansi-{mode}.jsonl")
    for key, s in ms.items():
        if key == "provenance":
            continue
        base, door = key.rsplit("/", 1)
        for d in [door] + (["native"] if door == "sql" else []):
            k = f"{base}/{d}"
            vb, vh = misc_verdict(s, mb.get(k)), misc_verdict(s, mh.get(k))
            was = vb in ("EQUAL", "EQUAL-ERR")
            now = vh in ("EQUAL", "EQUAL-ERR")
            moves["misc " + ("EQUAL" if was else "DIFF") + "->" + ("EQUAL" if now else "DIFF")] += 1
            if was and not now:
                moved_examples.append((mode, k, vb, vh))
            if not now:
                remaining[("misc", d, base.split("/")[0], vh)] += 1
res = {
    "moves": dict(moves),
    "moved_examples": moved_examples,
    "remaining": sorted([[*list(k), v] for k, v in remaining.items()]),
}
out.write_text(json.dumps(res, indent=1))
print(json.dumps(res["moves"]))
