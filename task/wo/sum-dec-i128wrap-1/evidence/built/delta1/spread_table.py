import json
import sys
from collections import Counter
from pathlib import Path

CLASS_B = ("mp/sum/grouped_s4/g11/", "mp/try/s4/g11/", "sum/distinct/g10/", "sum/distinct/g12/")


def label(answer: str) -> str:
    if answer.startswith("ERR:"):
        return "raise " + answer[4:]
    rows = json.loads(answer)["rows"]
    values = [row[-1] for row in rows]
    text = ",".join(values)
    for size in (38, 28):
        text = text.replace("9" * size, "M")
    return text.replace("0" * 30, "0…")


def load(path: Path) -> tuple[dict, dict]:
    records = [json.loads(line) for line in path.open()]
    head = next(r for r in records if r["key"] == "provenance")
    return head, {r["key"]: r["answers"] for r in records if "answers" in r}


def name(head: dict, path: Path) -> str:
    stem = path.stem.removesuffix("-" + head["subset"]).replace("---", "-")
    return stem.replace(f"-{head['ansi']}-", "-") + f" ({head['reps']} runs)"


summary = []
detail: dict = {}
for path in sorted(Path(arg) for arg in sys.argv[1:]):
    head, answers = load(path)
    varied = [k for k, a in answers.items() if len(a) > 1]
    run = f"{name(head, path)} ANSI {head['ansi']}"
    summary.append((run, len(answers), len(varied), head["reps"]))
    for key, seen in answers.items():
        if key.startswith(CLASS_B) or len(seen) > 1:
            detail.setdefault(key, {})[run] = "; ".join(
                f"{label(a)} x{n}" for a, n in Counter(seen).most_common()
            )
print("| run | queries | reps | queries with more than one answer |")
print("|---|---|---|---|")
for run, n, v, reps in summary:
    print(f"| {run} | {n} | {reps} | {v} |")
print()
for key in sorted(detail):
    print(f"- `{key}`")
    for run, text in sorted(detail[key].items()):
        print(f"  - {run}: {text}")
