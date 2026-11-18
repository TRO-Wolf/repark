import json
import sys
from collections import Counter, defaultdict

rows = [json.loads(l) for l in open(sys.argv[1])]
AVG_FAMILIES = {"avg", "mean", "mixed", "sum", "avgctl"}


def protected(r):
    k = r["key"]
    if "native" in r["conf"]:
        return "native door"
    if k.startswith(("shape/avg_div/", "shape/mean_div/")):
        return None
    if r["family"] in AVG_FAMILIES or k.startswith(("avg", "mean", "sum/")):
        return "avg/mean/sum"
    if k.startswith("try") or "try_" in k:
        return "try_*"
    if k.startswith("div/typed/") or "fold" in k:
        return "typed operand or folded divisor"
    return None


def named(r):
    k, conf = r["key"], r["conf"]
    if "native" in conf:
        return "native door (NATIVE-DECIMAL-AVG-DIVIDE-1, S2)"
    if r["head"] == "BOTH_RAISE":
        return "both raise (the 38-digit row 10, the same condition)"
    if conf == "apl_false":
        return "allowPrecisionLoss=false (DECIMAL-PRECISION-LOSS-CONF-1, S2)"
    if k.startswith("mixed/avg_distinct_count/") or k.startswith("mixed/mean_distinct_count/"):
        return "AVG-DISTINCT-MIXED-1 (S3)"
    if k.startswith(("meandf/select_F.expr_", "avgdf/select_F.expr")):
        return "FEXPR-COUNT-STAR-AGG-1 (S3)"
    if k.startswith("avgdf/summary_default/") or k.startswith(("avgdf/describe/", "avgdf/summary_five/")):
        return "describe/summary (not this family)"
    if "d38_10" in k and ("mul" in k):
        return "d38_10 * 3 overflow (its S2 card)"
    if k.startswith(("round/", "moddf/round_col_div/")):
        return "round() type (DECIMAL-ROUND-RESULT-TYPE-1, S3)"
    if k.startswith("try") or "try_divide" in k:
        return "try_* coercion (TRY-ARITH-DECIMAL-INT-1, S1)"
    if r["family"] in AVG_FAMILIES:
        return "avg family (AVG-DECIMAL-ROUND-1 classes)"
    return "UNNAMED"


trans = Counter((r["main"], r["head"]) for r in rows)
print("cells", len(rows))
for (a, b), n in sorted(trans.items()):
    print(f"  main {a:>14} -> head {b:<14} {n}")
reg = [r for r in rows if r["main"] == "EQUAL" and r["head"] != "EQUAL"]
print("EQUAL -> non-EQUAL:", len(reg))
for r in reg:
    print("  REG", r["ansi"], r["conf"], r["key"], r["head"], str(r.get("head_result"))[:150], "| spark", str(r.get("spark_result"))[:150])
moved_protected = [(protected(r), r) for r in rows if not r["same_as_main"] and protected(r)]
print("protected cells that moved:", len(moved_protected))
for p, r in moved_protected[:40]:
    print("  MOVED", p, r["ansi"], r["conf"], r["key"], r["main"], "->", r["head"])
became = Counter(r["key"].split("/")[0] for r in rows if r["main"] != "EQUAL" and r["head"] == "EQUAL")
print("became EQUAL by family prefix:", dict(became))
by = defaultdict(list)
for r in rows:
    if r["head"] != "EQUAL":
        by[named(r)].append(r)
print("head non-EQUAL by class:")
for c, rs in sorted(by.items()):
    print(f"  {c}: {len(rs)} {dict(Counter(r['head'] for r in rs))} moved={sum(not r['same_as_main'] for r in rs)}")
for r in by.get("UNNAMED", [])[:200]:
    print("  UNNAMED", r["ansi"], r["conf"], r["key"], r["main"], "->", r["head"], str(r.get("head_result"))[:110], "| spark", str(r.get("spark_result"))[:110])
