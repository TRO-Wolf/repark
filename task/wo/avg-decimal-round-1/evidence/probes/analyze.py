import json
import sys
from collections import Counter, defaultdict
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, getcontext
from pathlib import Path

getcontext().prec = 120

src, dst = Path(sys.argv[1]), Path(sys.argv[2])


def load(name: str) -> dict:
    path = src / name
    if not path.exists():
        return {}
    return {r["key"]: r for r in map(json.loads, path.open())}


def num(x: str) -> object:
    try:
        return Decimal(x)
    except (InvalidOperation, ValueError):
        return x


def rows_equal_numeric(a: list, b: list) -> bool:
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b, strict=True):
        if len(ra) != len(rb):
            return False
        for x, y in zip(ra, rb, strict=True):
            if num(x) != num(y):
                return False
    return True


def is_err(v: object) -> bool:
    return isinstance(v, str) and v.startswith(("ERR", "SETUP"))


def renorm(res: object) -> object:
    if not isinstance(res, dict) or "types" not in res:
        return res
    scales = []
    for ty in res["types"]:
        if ty.startswith("decimal("):
            scales.append(int(ty[:-1].split(",")[1]))
        else:
            scales.append(None)
    rows = []
    for row in res["rows"]:
        fixed = []
        for x, sc in zip(row, scales, strict=False):
            v = num(x)
            if sc is not None and isinstance(v, Decimal) and v.as_tuple().exponent < -sc:
                q = v.quantize(Decimal(1).scaleb(-sc))
                fixed.append(format(q, "f") if q == v else x)
            else:
                fixed.append(x)
        rows.append(fixed)
    return dict(res, rows=rows)


def classify(sp: dict, rp: dict) -> str:
    a, b = sp.get("result"), rp.get("result")
    if sp.get("door") == "iceberg":
        sa, sb = sp.get("statement"), rp.get("statement")
        if is_err(sa) and is_err(sb):
            return "BOTH_RAISE"
        if is_err(sa) != is_err(sb):
            return "REPARK_RAISES" if is_err(sb) else "SPARK_RAISES"
        if is_err(a) or is_err(b):
            return "READ_ERR"
        if sp.get("schema") != rp.get("schema"):
            if rows_equal_numeric(a["rows"], b["rows"]):
                return "TYPE"
            return "VALUE"
        if a["rows"] != b["rows"]:
            return "VALUE" if not rows_equal_numeric(a["rows"], b["rows"]) else "TYPE"
        return "EQUAL"
    if is_err(a) and is_err(b):
        return "BOTH_RAISE"
    if is_err(a):
        return "SPARK_RAISES"
    if is_err(b):
        return "REPARK_RAISES"
    if not isinstance(a, dict) or not isinstance(b, dict):
        return "EQUAL" if a == b else "VALUE"
    if a["types"] == b["types"] and a["rows"] == b["rows"]:
        return "EQUAL"
    if rows_equal_numeric(a["rows"], b["rows"]):
        return "TYPE" if a["types"] != b["types"] else "FORMAT"
    return "VALUE"


def diff_rows(sp: dict, rp: dict) -> list:
    a, b = sp.get("result"), rp.get("result")
    if not isinstance(a, dict) or not isinstance(b, dict):
        return []
    out = []
    for ra, rb in zip(a["rows"], b["rows"], strict=False):
        if [num(x) for x in ra] != [num(x) for x in rb]:
            out.append([ra, rb])
    if len(a["rows"]) != len(b["rows"]):
        out.append(["rowcount", len(a["rows"]), len(b["rows"])])
    return out


def cause(x: str, y: str) -> str:
    a, b = num(x), num(y)
    if not isinstance(a, Decimal) or not isinstance(b, Decimal):
        return "null_or_text"
    ea, eb = a.as_tuple().exponent, b.as_tuple().exponent
    if ea == eb:
        ulp = Decimal(1).scaleb(ea)
        if abs(b) < abs(a) and abs(a - b) == ulp:
            return "truncated_one_unit"
        return "other_same_scale"
    if eb < ea:
        if b.quantize(Decimal(1).scaleb(ea), rounding=ROUND_HALF_UP) == a:
            return "longer_quotient"
        return "longer_other"
    return "fewer_digits"


def causes(sp: dict, rp: dict) -> dict:
    a, b = sp.get("result"), rp.get("result")
    out = Counter()
    if not isinstance(a, dict) or not isinstance(b, dict) or len(a["rows"]) != len(b["rows"]):
        return {}
    for ra, rb in zip(a["rows"], b["rows"], strict=True):
        for x, y in zip(ra, rb, strict=False):
            if num(x) != num(y):
                out[cause(x, y)] += 1
    return dict(out)


verdicts = []
summary = defaultdict(Counter)
runs = []
for ansi in ("true", "false"):
    for conf in ("base", "apl_false", "pmp_false"):
        runs.append((ansi, conf, f"spark-{ansi}-{conf}.jsonl", f"repark-{ansi}-{conf}.jsonl"))
runs.append(("true", "native", "spark-true-base.jsonl", "native.jsonl"))
runs.append(("false", "native", "spark-false-base.jsonl", "native.jsonl"))
for ansi, conf, sname, rname in runs:
    sp_all, rp_all = load(sname), load(rname)
    if not sp_all or not rp_all:
        continue
    for key, sp in sp_all.items():
        if key == "provenance" or key.startswith(("data/", "setup/", "conf/")):
            continue
        rp = rp_all.get(key)
        if rp is None:
            continue
        sp = dict(sp, result=renorm(sp.get("result")))
        rp = dict(rp, result=renorm(rp.get("result")))
        v = classify(sp, rp)
        if conf == "native":
            rp = dict(rp, door="native")
        rec = {
            "key": key,
            "ansi": ansi,
            "conf": conf,
            "family": sp.get("family"),
            "door": rp.get("door") if conf == "native" else sp.get("door"),
            "pos": sp.get("pos"),
            "col": sp.get("col"),
            "verdict": v,
            "spark_types": sp["result"].get("types")
            if isinstance(sp.get("result"), dict)
            else None,
            "repark_types": rp["result"].get("types")
            if isinstance(rp.get("result"), dict)
            else None,
        }
        if sp.get("door") == "iceberg":
            rec["spark_schema"] = sp.get("schema")
            rec["repark_schema"] = rp.get("schema")
            rec["spark_statement"] = sp.get("statement")
            rec["repark_statement"] = rp.get("statement")
        if v in ("VALUE", "TYPE", "FORMAT") or sp.get("door") == "iceberg":
            rec["diff"] = diff_rows(sp, rp)[:6]
            rec["wrong_rows"] = len(diff_rows(sp, rp))
            rec["causes"] = causes(sp, rp)
        if v in ("REPARK_RAISES", "SPARK_RAISES", "BOTH_RAISE"):
            rec["spark"] = sp.get("result") if is_err(sp.get("result")) else "answers"
            rec["repark"] = rp.get("result") if is_err(rp.get("result")) else "answers"
        verdicts.append(rec)
        summary[(conf, ansi, sp.get("family"), rec["door"], sp.get("pos"))][v] += 1
with (dst / "verdicts.jsonl").open("w") as fh:
    for r in verdicts:
        fh.write(json.dumps(r, sort_keys=True) + "\n")
order = [
    "EQUAL",
    "TYPE",
    "FORMAT",
    "VALUE",
    "REPARK_RAISES",
    "SPARK_RAISES",
    "BOTH_RAISE",
    "READ_ERR",
]
lines = ["| conf | ANSI | family | door | position | " + " | ".join(order) + " |"]
lines.append("|" + "---|" * (5 + len(order)))
for (conf, ansi, fam, door, pos), c in sorted(
    summary.items(), key=lambda kv: tuple(str(x) for x in kv[0])
):
    lines.append(
        f"| {conf} | {ansi} | {fam} | {door} | {pos} | "
        + " | ".join(str(c.get(o, 0)) for o in order)
        + " |"
    )
tot = Counter()
for c in summary.values():
    tot.update(c)
lines.append("")
lines.append(
    "Totals: " + ", ".join(f"{o} {tot.get(o, 0)}" for o in order) + f", cells {sum(tot.values())}"
)
(dst / "summary.md").write_text("\n".join(lines) + "\n")
print(sum(tot.values()), dict(tot))
