import sys
from collections import Counter

from analyze import load, verdict

raw = sys.argv[1]
ABBR = {
    "equal": "=",
    "type-only": "T",
    "wrong-value": "W",
    "repark-raises-overflow": "X",
    "repark-raises-other": "R",
    "both-raise": "B",
    "spark-raises-repark-answers": "A",
}
LS = ["dec_0.1", "dec_1.5", "dec_18_9", "dec_0.001", "dec_38_18", "dec_col"]
CS = ["int", "bigint", "null", "str_digit"]
RS = ["dbl_1e-20", "dbl_1e30", "dbl_neg0", "dbl_nan", "dbl_inf", "dbl_col", "flt_1.5", "flt_1e-20"]


def matrix(sp: dict, rp: dict, lefts: list, title: str) -> None:
    keys = [k for k in sp if "/" in k and not k.startswith("extra")]
    cons = list(dict.fromkeys(k.split("/")[0] for k in keys))
    print(f"\n{title}\n")
    print("| construct | " + " | ".join(RS) + " |")
    print("|---|" + "---|" * len(RS))
    for c in cons:
        cellstr = []
        for r in RS:
            s = ""
            for left in lefts:
                k = f"{c}/{left}/{r}"
                s += ABBR[verdict(sp[k], rp[k])] if k in sp and k in rp else "."
            cellstr.append(f"`{s}`")
        print(f"| `{c}` | " + " | ".join(cellstr) + " |")


for mode in ("true", "false"):
    sp = load(f"select-spark-ansi-{mode}.jsonl")
    rp = load(f"select-repark-ansi-{mode}.jsonl")
    matrix(sp, rp, LS, f"ANSI {mode}, decimal operands in the order {', '.join(LS)}")
    matrix(sp, rp, CS, f"ANSI {mode}, control operands in the order {', '.join(CS)}")
sp = load("select-spark-ansi-true.jsonl")
nt = load("select-native.jsonl")
matrix(sp, nt, LS, "native door against Spark ANSI true, decimal operands")
counts = Counter()
for mode in ("true", "false"):
    for door, pref in (("sql", "select"), ("df", "frames")):
        sp = load(f"{pref}-spark-ansi-{mode}.jsonl")
        rp = load(f"{pref}-repark-ansi-{mode}.jsonl")
        for k in sp:
            if k in ("provenance", "elapsed") or k not in rp:
                continue
            counts[(mode, door, verdict(sp[k], rp[k]))] += 1
print()
for k in sorted(counts):
    print(k, counts[k])
