import collections
import json
import statistics
import sys
from pathlib import Path

C = Path(sys.argv[1])
per = collections.defaultdict(lambda: collections.defaultdict(list))
answers = collections.defaultdict(dict)
for line in (C / "raw-sum.jsonl").open():
    r = json.loads(line)
    per[r["case"]][r["label"]].append(r["seconds"] / r["reps"])
    answers[r["case"]][r["label"]] = r["answer"]
for line in (C / "samples.jsonl").open():
    r = json.loads(line)
    if r["group"] == "c012" and r["rc"] == 0:
        per["c012_" + r["case"]][r["build"]].append(r["wall"])
REPORTED = ("k5_",)


def med(xs: list) -> float:
    return statistics.median(xs) if xs else float("nan")


print(
    "| row | main (s) | head (s) | head / main | control main-aa / main | runs | answers equal |",
    end="",
)
print(" bar |")
print("|---|---|---|---|---|---|---|---|")
for case in sorted(per):
    d = per[case]
    m, h, a = d.get("main", []), d.get("head", []), d.get("main-aa", [])
    ratio = med(h) / med(m) if m and h else float("nan")
    band = [x / y for x, y in zip(a, m, strict=False) if y]
    text = f"{min(band):.3f} to {max(band):.3f}" if band else ""
    same = len(set(answers[case].values())) <= 1 if case in answers else ""
    if case.startswith(REPORTED):
        bar = "reported"
    elif ratio <= 1.05:
        bar = "held"
    elif band and ratio <= max(band):
        bar = "inside the band"
    else:
        bar = "ABOVE"
    cells = (
        case,
        f"{med(m):.4f}",
        f"{med(h):.4f}",
        f"{ratio:.3f}",
        text,
        min(len(m), len(h)),
        same,
        bar,
    )
    print("| " + " | ".join(str(cell) for cell in cells) + " |")
for col in ("d10", "d38"):
    wide, narrow = med(per[f"k2_rows1000_{col}"]["head"]), med(per[f"k2_rows3_{col}"]["head"])
    print(
        f"\nK2 scaling, head, {col}: ROWS 1000 PRECEDING / ROWS 3 PRECEDING = {wide / narrow:.3f}"
    )
