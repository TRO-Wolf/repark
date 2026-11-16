import collections
import json
import statistics
import sys
from pathlib import Path

C = Path(sys.argv[1])
per = collections.defaultdict(lambda: collections.defaultdict(list))
answers = collections.defaultdict(dict)
extra = collections.defaultdict(dict)
for name in ("raw-held.jsonl", "raw-cu.jsonl"):
    p = C / name
    if not p.exists():
        continue
    for line in p.open():
        r = json.loads(line)
        per[r["case"]][r["label"]].append(r["seconds"] / r.get("reps", 1))
        answers[r["case"]][r["label"]] = r.get("answer")
        for k in ("tasks", "pruning", "per_row"):
            if k in r:
                extra[r["case"]][r["label"]] = r[k]
for line in (C / "samples.jsonl").open():
    r = json.loads(line)
    if r["group"] == "c012" and r["rc"] == 0:
        per["C-012 " + r["case"]][r["build"]].append(r["wall"])


def med(xs: list) -> float:
    return statistics.median(xs) if xs else float("nan")


def band(xs: list, ys: list) -> str:
    rs = [x / y for x, y in zip(xs, ys, strict=False) if y]
    return f"{min(rs):.3f} to {max(rs):.3f}" if rs else ""


rows = []
for case in sorted(per):
    d = per[case]
    m, h, a = d.get("main", []), d.get("head", []), d.get("main-aa", [])
    rows.append(
        (
            case,
            med(m),
            med(h),
            med(h) / med(m) if m and h else float("nan"),
            band(a, m),
            min(len(m), len(h)),
            len({str(v) for v in answers[case].values()}) <= 1,
            extra[case].get("head", ""),
        )
    )
print(
    "| row | main (s) | head (s) | head/main | control main-aa/main | runs "
    "| answers equal | head scan |"
)
print("|---|---|---|---|---|---|---|---|")
for r in rows:
    cells = " | ".join(str(x) for x in (r[4], r[5], r[6], r[7]))
    print(f"| {r[0]} | {r[1]:.4f} | {r[2]:.4f} | {r[3]:.3f} | {cells} |")
print()
print(
    "| shielded row vs hand-written range (same build) | build | cast (s) | hand (s) "
    "| cast/hand | runs |"
)
print("|---|---|---|---|---|---|")
for cast, hand in (
    ("c2_eq", "c2_eq_hand"),
    ("c2_lt", "c2_lt_hand"),
    ("c3_cow_eq", "c3_cow_eq_hand"),
    ("c3_cow_lt", "c3_cow_lt_hand"),
    ("c3_mor_eq", "c3_mor_eq_hand"),
    ("c3_mor_lt", "c3_mor_lt_hand"),
    ("del_cow_eq", "del_cow_eq_hand"),
    ("del_mor_eq", "del_mor_eq_hand"),
):
    for build in ("head", "main", "main-aa"):
        x, y = per[cast].get(build, []), per[hand].get(build, [])
        if x and y:
            pairs = [a / b for a, b in zip(x, y, strict=False)]
            print(
                f"| {cast} | {build} | {med(x):.4f} | {med(y):.4f} | {med(x) / med(y):.3f} "
                f"(per run {min(pairs):.3f} to {max(pairs):.3f}) | {len(x)} |"
            )
