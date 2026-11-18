import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

with Path(sys.argv[1]).open() as fh:
    rows = [json.loads(line) for line in fh]
AVG_FAMILIES = {"avg", "mean", "mixed", "sum", "avgctl"}
DESCRIBE = ("avgdf/summary_default/", "avgdf/describe/", "avgdf/summary_five/")
MUL38 = ("divctl/mul/d38_", "divdf/col_mul_int/d38_", "shape/avg_mul/d38_")
ROUND_COMPARE = ("div/where_round/", "div/case_round/", "divdf/where_round/")
CLASSES = [
    (lambda k, r: "native" in r["conf"], "native door (NATIVE-DECIMAL-AVG-DIVIDE-1, S2)"),
    (
        lambda k, r: r["head"] == "BOTH_RAISE",
        "both raise (the 38-digit row 10, the same condition)",
    ),
    (
        lambda k, r: r["conf"] == "apl_false",
        "allowPrecisionLoss=false (DECIMAL-PRECISION-LOSS-CONF-1, S2)",
    ),
    (
        lambda k, r: k.startswith(("mixed/avg_distinct_count/", "mixed/mean_distinct_count/")),
        "AVG-DISTINCT-MIXED-1 (S3)",
    ),
    (
        lambda k, r: k.startswith(("meandf/select_F.expr_", "avgdf/select_F.expr")),
        "FEXPR-COUNT-STAR-AGG-1 (S3)",
    ),
    (lambda k, r: k.startswith(DESCRIBE), "describe/summary (not this family)"),
    (lambda k, r: "d38_10" in k and "mul" in k, "d38_10 * 3 overflow (its S2 card)"),
    (lambda k, r: k.startswith(MUL38), "d38_10 * 3 overflow (its S2 card)"),
    (
        lambda k, r: k.startswith(ROUND_COMPARE) and k.endswith("/d38_10"),
        "WHERE / CASE on d38_10 / 3 = round(d38_10 / 3, 6) (the round() seat)",
    ),
    (lambda k, r: k == "hdiv/sessview", "hdiv/sessview (SESSION-VIEW-DECIMAL-DIV-1, proposed)"),
    (
        lambda k, r: k == "ice/where_div/d38_2",
        "ice/where_div/d38_2: both raise, Spark's class with .WITH_SUGGESTION",
    ),
    (
        lambda k, r: k.startswith(("round/", "moddf/round_col_div/")),
        "round() type (DECIMAL-ROUND-RESULT-TYPE-1, S3)",
    ),
    (
        lambda k, r: k.startswith("try") or "try_divide" in k,
        "try_* coercion (TRY-ARITH-DECIMAL-INT-1, S1)",
    ),
    (lambda k, r: r["family"] in AVG_FAMILIES, "avg family (AVG-DECIMAL-ROUND-1 classes)"),
]


def protected(r: dict) -> str | None:
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


def named(r: dict) -> str:
    for test, name in CLASSES:
        if test(r["key"], r):
            return name
    return "UNNAMED"


def cut(value: object, width: int) -> str:
    return str(value)[:width]


trans = Counter((r["main"], r["head"]) for r in rows)
print("cells", len(rows))
for (a, b), n in sorted(trans.items()):
    print(f"  main {a:>14} -> head {b:<14} {n}")
reg = [r for r in rows if r["main"] == "EQUAL" and r["head"] != "EQUAL"]
print("EQUAL -> non-EQUAL:", len(reg))
for r in reg:
    head, spark = cut(r.get("head_result"), 150), cut(r.get("spark_result"), 150)
    print("  REG", r["ansi"], r["conf"], r["key"], r["head"], head, "| spark", spark)
moved_protected = [(protected(r), r) for r in rows if not r["same_as_main"] and protected(r)]
print("protected cells that moved:", len(moved_protected))
for p, r in moved_protected[:40]:
    print("  MOVED", p, r["ansi"], r["conf"], r["key"], r["main"], "->", r["head"])
became = Counter(
    r["key"].split("/")[0] for r in rows if r["main"] != "EQUAL" and r["head"] == "EQUAL"
)
print("became EQUAL by family prefix:", dict(became))
by = defaultdict(list)
for r in rows:
    if r["head"] != "EQUAL":
        by[named(r)].append(r)
print("head non-EQUAL by class:")
for c, rs in sorted(by.items()):
    verdicts = dict(Counter(r["head"] for r in rs))
    moved = sum(not r["same_as_main"] for r in rs)
    print(f"  {c}: {len(rs)} {verdicts} moved={moved}")
for r in by.get("UNNAMED", [])[:200]:
    head, spark = cut(r.get("head_result"), 110), cut(r.get("spark_result"), 110)
    print(
        "  UNNAMED",
        r["ansi"],
        r["conf"],
        r["key"],
        r["main"],
        "->",
        r["head"],
        head,
        "| spark",
        spark,
    )
