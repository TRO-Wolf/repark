import json
import sys
from collections import Counter
from pathlib import Path

P = Path(sys.argv[1])
labels = sys.argv[2:]
data = {label: json.loads((P / f"{label}-cells.json").read_text()) for label in ["spark", *labels]}


def same(a: dict, b: dict) -> bool:
    if "error" in a or "error" in b:
        return "error" in a and "error" in b and (not a["error"] or a["error"] == b["error"])
    return a.get("rows") == b.get("rows")


def show(a: dict) -> str:
    if "error" in a:
        return "RAISE " + (a["error"] or a.get("text", "")[:50])
    return "|".join(a.get("rows", []))[:60]


tally = Counter()
for mode in ("true", "false", "unset"):
    for key in sorted(data["spark"][mode]):
        want = data["spark"][mode][key]
        verdicts = [same(want, data[label][mode][key]) for label in labels]
        tally[(mode, tuple(verdicts))] += 1
        if not verdicts[-1]:
            row = [show(want)] + [show(data[label][mode][key]) for label in labels]
            print(mode, key, "|", " || ".join(row))
for k, n in sorted(tally.items()):
    print("TALLY", k, n)
